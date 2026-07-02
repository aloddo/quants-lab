#!/usr/bin/env python3
"""v25 wallet-selection harness -- shared frozen constants + infrastructure.

BINDING spec: /tmp/v25_prereg_v3.md (pre-registration v3.2, frozen). Every constant here
is copied verbatim from that doc. The freeze record binds the doc content hash, the
harness git commit, and the input sha256 manifest together (v25_freeze.py); the fold
runner validates FREEZE.json against current code + inputs at startup.

Contents:
- FOLDS: 5 expanding folds with EXACT half-open UTC bounds frozen in the doc:
  train_k = [2025-12-01T00:00Z, test_start_k); tests F1 [2026-03-01, 2026-03-22),
  F2 [2026-03-22, 2026-04-12), F3 [2026-04-12, 2026-05-03), F4 [2026-05-03, 2026-05-24),
  F5 [2026-05-24, 2026-06-11). asof_k = test_start_k. (gate-b blocker #2)
- ExecScenario: the TWO frozen fee/slippage scenarios (BASE, WORST) as explicit
  constructor overrides on top of the canonical research/v15/execution_model.py tables
- MarksIndex: in-memory asof/next 1m-close mark index over app/data/v15/marks_cache
  (NO Mongo, NO live API -- fail-closed on missing coins); next_mark supports a hard
  cap_ms so NO price beyond a fold/holdout boundary is ever read (gate-b blocker #5)
- iter_wallet_frames: memory-safe wallet-contiguous streaming over m02_actions.parquet
- build_journeys: journey reconstruction via opening_journey_id / closing_journey_id
  (REVERSE rows carry BOTH ids; grouping by back-compat journey_id is FORBIDDEN --
  gate-b blocker #4), with NET realized PnL via the canonical m02 fee model
- event-hash Bernoulli dropout (deterministic per event, independent of iteration order)
"""
from __future__ import annotations

import hashlib
import sys
import urllib.parse as _ulib
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent.parent
V15_DIR = REPO / "research" / "v15"
if str(V15_DIR) not in sys.path:
    sys.path.insert(0, str(V15_DIR))

from _streaming_io import ShardedParquetWriter, install_memory_guard  # noqa: F401,E402

ACTIONS_PARQUET = REPO / "app" / "data" / "v15" / "m02_actions.parquet"
MARKS_CACHE_DIR = REPO / "app" / "data" / "v15" / "marks_cache"
L2_CALIB_PATH = REPO / "app" / "data" / "v15" / "l2_calib_10coin.json"
OUT_DIR = REPO / "app" / "data" / "research" / "v25"
PREREG_DOC = Path("/tmp/v25_prereg_v3.md")

MS_MIN = 60_000
MS_DAY = 86_400_000

# ---- frozen experiment constants (spec: /tmp/v25_prereg_v3.md) ------------------------------ #
TRAIN_START = pd.Timestamp("2025-12-01")
# EXACT frozen half-open test windows (gate-b blocker #2). asof_k = test_start_k.
FOLD_TEST_STARTS = ["2026-03-01", "2026-03-22", "2026-04-12", "2026-05-03", "2026-05-24"]
FOLD_TEST_ENDS = ["2026-03-22", "2026-04-12", "2026-05-03", "2026-05-24", "2026-06-11"]
HOLDOUT_START = pd.Timestamp("2026-06-11")
HOLDOUT_EARLIEST_EVAL = pd.Timestamp("2026-07-16")
DELTA_BPS = 10.0                                # frozen hurdle: 10 bps/trip net
ORDER_USD = 150.0                               # $150/order, frozen
INITIAL_EQUITY = 500.0                          # frozen
MAX_GROSS_X = 2.5                               # "netx 2.5x" cap (gross notional / equity)
MAX_COIN_SIDE_X = 2.0                           # per coin-side notional / equity
MAX_MARGIN_UTIL = 0.7
RESERVE_LEV = 10.0          # margin reserve leverage (live margin_reserve_max_lev, v17 engine)
REPRICE_MS = 2000                               # 2s repricing
REPRICE_WINDOW_MS = 60_000                      # mark must exist within 60s else drop+count
TOP_N_ENTITIES = 25
R2_MIN_TRIPS = 50
R2_LCB_LEVEL = 0.90                             # one-sided 90% LCB
R2_BLOCK_DAYS = 5
R2_RESAMPLES = 2000
R2_SEED = 42
PORT_BLOCK_DAYS = 7                             # primary daily-PnL bootstrap block
PORT_BLOCK_DAYS_ROBUST = 14
PORT_RESAMPLES = 10_000
PORT_SEED = 42
HOLDOUT_LCB_LEVEL = 0.95                        # holdout: one-sided 95%, single rule
HOLDOUT_MIN_TRIPS = 100
DROPOUT_P = 0.4
DROPOUT_SEEDS = [17, 42, 137]
CLUSTER_OVERLAP_FRAC = 0.30                     # edge if > 30%
CLUSTER_WINDOW_MS = 60_000
OPEN_BAG_FRAC = -0.10                           # open MTM at asof >= -10% x trailing 30d |PnL|
OPEN_BAG_TRAIL_DAYS = 30

# Causal activity gate (frozen; codex vetoed resolution 4): last fill <= 7 DAYS before
# asof AND >= 20 distinct active days in train.
ACTIVITY_LAST_FILL_DAYS = 7
MIN_ACTIVE_DAYS = 20
# Coverage gate (frozen): wallet EXCLUDED if > 5% of its CLOSED train JOURNEYS
# (exit_ts <= asof) are unmarkable (journey-level 95%). A journey is unmarkable iff ANY
# of its constituent actions lacks a valid sizing mark (mark NaN or <= 0).
COVERAGE_MAX_UNMARKABLE_FRAC = 0.05

# FIRST_CLOSE live-parity semantics (frozen; gate-b blocker #5): copy exit triggers when
# CUMULATIVE leader reverse flow >= 85% of accumulated copied notional (live
# exit_min_trim_pct = full_exit_trim_pct = 0.85 in config v24_LIVE); leader addons are
# NOT copied but grow the trim denominator; dust threshold $1 (a leader position whose
# residual notional falls below $1 counts as fully closed). NO partial exits exist.
EXIT_TRIGGER_FRAC = 0.85
DUST_USD = 1.0

# Canonical m02 leader fee model (research/v15/v15_m02_journey_trace.py:196):
# SOURCE_ASSUMED_TAKER_FEE_BPS = 4.32 per side of fill notional. m02_actions.parquet
# carries NO fee or funding columns, so R1 NET realized PnL is reconstructed as
# realized - fees with this model applied to every leader fill (REVERSE fees split
# proportionally between the closing and opening leg, exactly as m02 does). Funding
# is NOT reconstructible from the actions file and is omitted -- DOCUMENTED limitation
# per the spec's fallback clause ("if m02 lacks fee columns, reconstruct net via the
# canonical fee model applied to the leader fills, documented").
LEADER_FEE_RATE = 4.32 / 10000.0


def folds() -> list[dict]:
    out = []
    for i, (s, e) in enumerate(zip(FOLD_TEST_STARTS, FOLD_TEST_ENDS), start=1):
        asof = pd.Timestamp(s)
        end = pd.Timestamp(e)
        out.append({
            "fold": i,
            "train_start_ms": int(TRAIN_START.value // 10**6),
            "asof_ms": int(asof.value // 10**6),
            "test_end_ms": int(end.value // 10**6),
            "test_days": int((end - asof).days),
        })
    return out


def coin_is_spot(coin: str) -> bool:
    """V15 m01 convention: @/# prefixed and slash symbols are spot / outcome markets, not perps."""
    return coin.startswith(("@", "#")) or "/" in coin or coin == "USDC"


def coin_is_hip3(coin: str) -> bool:
    return ":" in coin


# --------------------------------------------------------------------------------------------- #
# Execution scenarios (frozen). Composes the CANONICAL research/v15/execution_model.py per-coin
# L2 slippage table; fees are EXPLICIT constructor overrides per the spec (blocker #7).
# --------------------------------------------------------------------------------------------- #
@dataclass
class ExecScenario:
    name: str
    taker_oneway_main_bps: float        # one-way taker fee, main dex
    taker_oneway_hip3_bps: float        # one-way taker fee, HIP-3 (prefixed) markets
    slip_default_bps: float             # one-way slippage default for uncalibrated coins
    use_l2_calib: bool                  # BASE: per-coin measured L2; WORST: flat default everywhere
    _slip_table: dict = field(default_factory=dict, repr=False)

    def __post_init__(self):
        if self.use_l2_calib:
            import execution_model as em
            em._load_slip()
            # snapshot: scenario-local copy, no module-global mutation across scenarios
            self._slip_table = dict(em._SLIP_ONEWAY)

    def slip_oneway(self, coin: str) -> float:
        v = self._slip_table.get(coin)
        if v is None:
            return self.slip_default_bps / 1e4
        return float(v)

    def fee_oneway(self, coin: str) -> float:
        b = self.taker_oneway_hip3_bps if coin_is_hip3(coin) else self.taker_oneway_main_bps
        return b / 1e4

    def rt_cost(self, coin: str) -> float:
        """Round-trip cost fraction: entry slip + exit slip + entry fee + exit fee."""
        return 2.0 * self.slip_oneway(coin) + 2.0 * self.fee_oneway(coin)

    def entry_px(self, coin: str, mark: float, is_long: bool) -> float:
        s = self.slip_oneway(coin)
        return mark * (1 + s) if is_long else mark * (1 - s)

    def exit_px(self, coin: str, mark: float, is_long: bool) -> float:
        s = self.slip_oneway(coin)
        return mark * (1 - s) if is_long else mark * (1 + s)


def scenario_base() -> ExecScenario:
    """BASE (frozen): measured live schedule, main 4.32bps/side, HIP-3 8.64/side
    [v17_exchange_fills reconciliation 2026-07-02]; per-coin L2 slip, default 4.7."""
    return ExecScenario("BASE", 4.32, 8.64, 4.7, True)


def scenario_worst() -> ExecScenario:
    """WORST (frozen): file schedule 4.5/side (9.0 RT); HIP-3 = file hip3_mult 2.0 => 9.0/side;
    slip_default 7.0 one-way EVERYWHERE (no per-coin calib)."""
    return ExecScenario("WORST", 4.5, 9.0, 7.0, False)


SCENARIOS = {"BASE": scenario_base, "WORST": scenario_worst}


# --------------------------------------------------------------------------------------------- #
# Marks index: 1m closes from the m02 marks cache (.npy per coin). Asof-only, fail-closed.
# The bar with open-minute m closes at m + 60s; its close is AVAILABLE from m + 60s onward.
# --------------------------------------------------------------------------------------------- #
class MarksIndex:
    """In-memory asof/next mark index over the precomputed 1m close cache.

    Repricing rule (spec, frozen): our fill price = close of the FIRST bar whose close time
    (bar_open + 60s) >= signal_ts + 2000ms, and that close time must be within 60s of the
    target, else the trade is dropped and counted. NEVER the prior bar (always-late).
    cap_ms (window-boundary isolation, gate-b #5): a returned mark's close time must be
    strictly BELOW cap_ms -- the window is half-open [start, end), so a mark exactly AT
    the end is UNREADABLE; no price at/beyond the fold/holdout end is ever read.
    Fail-closed: missing coin series => no mark => drop/count."""

    def __init__(self, cache_dir: Path = MARKS_CACHE_DIR, max_coins: int = 256):
        self.cache_dir = Path(cache_dir)
        self.max_coins = max_coins
        self._lru: OrderedDict[str, tuple] = OrderedDict()
        self.n_missing_series = 0

    def _series(self, coin: str):
        s = self._lru.get(coin)
        if s is not None:
            self._lru.move_to_end(coin)
            return s
        p = self.cache_dir / f"{_ulib.quote(coin, safe='')}.npy"
        if not p.exists():
            self.n_missing_series += 1
            s = (np.empty(0, "int64"), np.empty(0, "float64"))
        else:
            arr = np.load(p, mmap_mode="r")
            # keep the mmap views (page-cached, flat RSS); do NOT materialize copies
            s = (arr[0], arr[1])
        self._lru[coin] = s
        if len(self._lru) > self.max_coins:
            self._lru.popitem(last=False)
        return s

    def next_mark(self, coin: str, signal_ts_ms: int,
                  reprice_ms: int = REPRICE_MS,
                  window_ms: int | None = REPRICE_WINDOW_MS,
                  cap_ms: int | None = None):
        """(fill_ts_ms, close) of the first bar CLOSING at >= signal + reprice_ms.
        window_ms bounds how late the mark may be (None = unbounded). cap_ms: half-open
        upper bound on the mark close time -- close_ts >= cap_ms is rejected
        (window-boundary isolation; None = no cap). Returns (None, None) if unavailable."""
        mins, closes = self._series(coin)
        if mins.size == 0:
            return None, None
        target = signal_ts_ms + reprice_ms
        # bar close time = open minute + 60s ; want first close_ts >= target
        i = int(np.searchsorted(mins, target - MS_MIN, side="left"))
        if i >= mins.size:
            return None, None
        close_ts = int(mins[i]) + MS_MIN
        if window_ms is not None and close_ts - target > window_ms:
            return None, None
        if cap_ms is not None and close_ts >= cap_ms:   # half-open: ts == cap unreadable
            return None, None
        v = float(closes[i])
        if v != v or v <= 0:
            return None, None
        return close_ts, v

    def asof_mark(self, coin: str, ts_ms: int):
        """Causal last available close at ts (bar close time <= ts). For MTM/equity sampling,
        NOT for pricing our fills (fills use next_mark). None if uncovered."""
        mins, closes = self._series(coin)
        if mins.size == 0:
            return None
        i = int(np.searchsorted(mins, ts_ms - MS_MIN, side="right")) - 1
        if i < 0:
            return None
        v = float(closes[i])
        return None if (v != v or v <= 0) else v


# --------------------------------------------------------------------------------------------- #
# Streaming wallet iterator (memory-safe; file is wallet-sorted, asserted)
# --------------------------------------------------------------------------------------------- #
ACTION_COLS = ["wallet", "coin", "ts", "action_type", "signed_size", "price", "position_after",
               "mark", "mark_ts", "is_liquidation", "journey_id", "opening_journey_id",
               "closing_journey_id", "source_equity_post"]


def iter_wallet_frames(path: Path = ACTIONS_PARQUET, columns: list[str] | None = None,
                       wallet_filter=None, batch_rows: int = 262_144, max_wallets: int | None = None):
    """Yield (wallet, DataFrame-of-its-actions) streaming over the wallet-sorted parquet.
    Bounded memory: holds at most one wallet's rows + one batch. Asserts wallet order is
    non-decreasing (fail loud if the sort assumption breaks). wallet_filter: optional
    callable(wallet)->bool applied before accumulating (cheap skip)."""
    columns = columns or ACTION_COLS
    pf = pq.ParquetFile(path)
    cur_w = None
    parts: list[pd.DataFrame] = []
    last_w_seen = ""
    n_yielded = 0
    for batch in pf.iter_batches(batch_size=batch_rows, columns=columns):
        df = batch.to_pandas()
        if df.empty:
            continue
        w_arr = df["wallet"].to_numpy()
        if w_arr[0] < last_w_seen:
            raise RuntimeError("m02_actions.parquet is not wallet-sorted; streaming invalid")
        last_w_seen = w_arr[-1]
        # split batch into consecutive wallet runs
        change = np.nonzero(w_arr[1:] != w_arr[:-1])[0] + 1
        starts = np.concatenate(([0], change))
        ends = np.concatenate((change, [len(df)]))
        for s, e in zip(starts, ends):
            w = w_arr[s]
            if w != cur_w:
                if cur_w is not None and parts:
                    yield cur_w, pd.concat(parts, ignore_index=True)
                    n_yielded += 1
                    if max_wallets is not None and n_yielded >= max_wallets:
                        return
                cur_w = w
                parts = []
            if wallet_filter is None or wallet_filter(w):
                parts.append(df.iloc[s:e])
        del df
    if cur_w is not None and parts:
        yield cur_w, pd.concat(parts, ignore_index=True)


# --------------------------------------------------------------------------------------------- #
# Journey reconstruction via opening_journey_id / closing_journey_id (gate-b blocker #4)
# --------------------------------------------------------------------------------------------- #
JOURNEY_COLS = ["coin", "journey_id", "entry_ts", "exit_ts", "side", "realized_pnl",
                "fees_paid", "net_realized_pnl", "max_notional", "duration_h", "liq_closed",
                "n_actions", "n_unmarked_actions", "unmarkable", "open_size", "open_basis"]


def build_journeys(wdf: pd.DataFrame) -> pd.DataFrame:
    """Per-coin journey records from one wallet's actions, replaying the m02 state machine
    with opening_journey_id / closing_journey_id semantics (v15_m02_journey_trace.py:436+).

    A REVERSE row carries TWO journey ids: closing_journey_id (the leg it closes) and
    opening_journey_id (the new leg it opens); back-compat journey_id equals the OPENING
    side and grouping by it corrupts both legs -- FORBIDDEN here (gate-b blocker #4).

    Accounting: average-cost on the leader's own fill prices. NET realized PnL =
    realized - fees, fees from the canonical m02 model (LEADER_FEE_RATE per fill side;
    REVERSE fee split proportionally between closing/opening leg). Funding is not
    reconstructible from m02_actions and is omitted (documented, see LEADER_FEE_RATE).

    Unattributable legs (ADDON/TRIM/EXIT/REVERSE with no observed opening ENTRY, e.g.
    carried-in positions) are skipped fail-closed: no journey is fabricated for them.

    Perp coins only (spot excluded, V15 m01 convention). exit_ts is NaN for journeys still
    open at the end of the slice. exit_ts <= asof filtering is the CALLER's job (pass a
    train-sliced wdf)."""
    rows: list[dict] = []
    p = wdf[~wdf["coin"].map(coin_is_spot)]
    has_ids = "opening_journey_id" in p.columns and "closing_journey_id" in p.columns

    def _emit(st: dict, exit_ts, liq_now: bool = False):
        closed = exit_ts is not None
        rows.append({
            "coin": st["coin"], "journey_id": int(st["jid"]),
            "entry_ts": int(st["entry_ts"]),
            "exit_ts": float(exit_ts) if closed else np.nan,
            "side": int(st["side"]),
            "realized_pnl": float(st["realized"]),
            "fees_paid": float(st["fees"]),
            "net_realized_pnl": float(st["realized"] - st["fees"]),
            "max_notional": float(st["max_notional"]),
            "duration_h": ((exit_ts - st["entry_ts"]) / 3.6e6) if closed else np.nan,
            "liq_closed": bool(st["liq"] or liq_now),
            "n_actions": int(st["n_actions"]),
            "n_unmarked_actions": int(st["n_unmarked"]),
            "unmarkable": bool(st["n_unmarked"] > 0),
            "open_size": 0.0 if closed else float(st["pos"]),
            "open_basis": 0.0 if closed else float(st["basis"]),
        })

    for coin, g in p.groupby("coin", sort=False):
        g = g.sort_values("ts", kind="mergesort")
        st: dict | None = None
        marks = g["mark"].to_numpy() if "mark" in g.columns else np.full(len(g), np.nan)
        ojids = g["opening_journey_id"].to_numpy() if has_ids else g["journey_id"].to_numpy()
        for k, r in enumerate(g.itertuples(index=False)):
            ts = int(r.ts)
            at = r.action_type
            ssz = float(r.signed_size)
            px = float(r.price) if (r.price == r.price and r.price > 0) else 0.0
            pafter = float(r.position_after)
            isliq = bool(r.is_liquidation)
            mv = marks[k]
            mark_bad = not (mv == mv and mv is not None and mv > 0)

            if at == "ENTRY":
                if st is not None:
                    # state desync (should not happen in valid m02 output): close fail-safe
                    _emit(st, None)
                st = {"coin": coin, "jid": int(ojids[k]), "entry_ts": ts,
                      "side": 1 if pafter > 0 else -1, "pos": pafter,
                      "basis": px if px > 0 else 0.0, "realized": 0.0,
                      "fees": abs(ssz) * px * LEADER_FEE_RATE,
                      "max_notional": abs(pafter) * px, "liq": isliq,
                      "n_actions": 1, "n_unmarked": int(mark_bad)}
                continue
            if st is None:
                continue        # unattributable leg (carried-in): skip fail-closed
            st["n_actions"] += 1
            st["n_unmarked"] += int(mark_bad)
            if isliq:
                st["liq"] = True
            pre_notional = abs(st["pos"]) * px
            st["max_notional"] = max(st["max_notional"], pre_notional)
            eff_px = px if px > 0 else st["basis"]

            if at == "ADDON":
                new_pos = st["pos"] + ssz
                if abs(new_pos) > 0:
                    st["basis"] = ((abs(st["pos"]) * st["basis"] + abs(ssz) * eff_px)
                                   / abs(new_pos))
                st["pos"] = pafter
                st["fees"] += abs(ssz) * px * LEADER_FEE_RATE
                st["max_notional"] = max(st["max_notional"], abs(pafter) * px)
            elif at == "TRIM":
                reduce_sz = min(abs(ssz), abs(st["pos"]))
                st["realized"] += reduce_sz * (eff_px - st["basis"]) * np.sign(st["pos"])
                st["fees"] += abs(ssz) * px * LEADER_FEE_RATE
                st["pos"] = pafter
            elif at == "EXIT":
                reduce_sz = abs(st["pos"])
                st["realized"] += reduce_sz * (eff_px - st["basis"]) * np.sign(st["pos"])
                st["fees"] += abs(ssz) * px * LEADER_FEE_RATE
                _emit(st, ts, liq_now=isliq)
                st = None
            elif at == "REVERSE":
                # one action row, two journeys: close the old leg, open the new
                closed_sz = abs(st["pos"])
                opened_sz = abs(pafter)
                st["realized"] += closed_sz * (eff_px - st["basis"]) * np.sign(st["pos"])
                split = closed_sz + opened_sz
                fee_total = abs(ssz) * px * LEADER_FEE_RATE
                closing_fee = fee_total * (closed_sz / split) if split > 0 else 0.0
                opening_fee = fee_total - closing_fee
                st["fees"] += closing_fee
                _emit(st, ts, liq_now=isliq)
                st = {"coin": coin, "jid": int(ojids[k]), "entry_ts": ts,
                      "side": 1 if pafter > 0 else -1, "pos": pafter,
                      "basis": px if px > 0 else 0.0, "realized": 0.0,
                      "fees": opening_fee, "max_notional": abs(pafter) * px,
                      "liq": False, "n_actions": 1, "n_unmarked": int(mark_bad)}
        if st is not None:
            _emit(st, None)     # still open at end of slice
    return pd.DataFrame(rows, columns=JOURNEY_COLS)


# --------------------------------------------------------------------------------------------- #
# Deterministic event-hash Bernoulli dropout (participation stress)
# --------------------------------------------------------------------------------------------- #
def event_dropout(seed: int, wallet: str, coin: str, journey_id: int, p: float = DROPOUT_P) -> bool:
    """True = this ENTRY event is dropped. Deterministic per (seed, event identity),
    independent of iteration order (spec: event-hash seeding, seeds 17/42/137)."""
    h = hashlib.sha256(f"{seed}|{wallet}|{coin}|{journey_id}".encode()).digest()
    u = int.from_bytes(h[:8], "big") / 2**64
    return u < p


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def git_commit() -> str:
    import subprocess
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip()
    except Exception:
        return "UNKNOWN"

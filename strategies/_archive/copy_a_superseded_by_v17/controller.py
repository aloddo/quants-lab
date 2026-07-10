"""Copy A controller decision core -- PURE logic (no network, no SDK).

Maps a leader fill event + current follower state -> OrderIntent (which then passes RiskBroker.submit,
the enforcement layer). Kept pure so the SAME decision code drives the live WS loop AND the shadow
harness -> shadow is a valid proxy of live behaviour (Fable/Codex plan requirement).

Mirror mapping (leader `dir` field: 'Open Long'|'Open Short'|'Close Long'|'Close Short'):
  Open Long  -> follower BUY  (open/hold long, fixed size, one entry per pair; no pyramiding)
  Open Short -> follower SELL (open/hold short)
  Close *    -> follower reduce-only EXIT of the matching position
Follower risk exits (independent of the leader, Hard Rule 7): stop-loss, trailing take-profit,
max-hold -> reduce-only flatten.
"""
from __future__ import annotations
from dataclasses import dataclass

from copy_a.risk_broker import OrderIntent


@dataclass
class FollowerPos:
    wallet: str
    coin: str
    signed_sz: float          # + long, - short (follower base units); 0 = flat
    entry_px: float
    peak_gain_frac: float     # best unrealized-gain fraction seen (for trailing TP)
    opened_ts: float


@dataclass
class ControllerConfig:
    allowed_pairs: frozenset          # {(wallet_lc, coin)}
    order_size_usd: float
    cooldown_s: float = 30.0
    stop_frac: float = 0.05           # hard stop at -5% on the position notional
    trail_frac: float = 0.03          # give back 3% from peak gain -> take profit
    trail_arm_frac: float = 0.02      # only arm trailing TP after +2% gain
    max_hold_s: float = 6 * 3600      # flatten after 6h


_EPS = 1e-9


def _sign(x: float) -> int:
    return 1 if x > _EPS else (-1 if x < -_EPS else 0)


def decide_net_mirror(leader_pos_before: float, leader_pos_after: float,
                      wallet: str, coin: str, mark_px: float,
                      follower: FollowerPos | None, cfg: ControllerConfig,
                      now: float, last_entry_ts: float) -> OrderIntent | None:
    """Mirror the leader's NET position, not each fill (the fix for the fee-churn bug found in shadow).

    Follower trades ONLY when the leader's net position crosses zero or flips sign:
      leader flat -> long/short   => follower ENTER fixed size in that direction (if not already held)
      leader long/short -> flat   => follower EXIT (reduce-only)
      leader flips sign           => follower EXIT (a subsequent call, once flat, opens the new side)
      leader scales in/out same direction (no zero-cross) => follower does NOTHING (holds)
    Enforcement (caps/kill/leverage) is RiskBroker's job; this only decides intent.
    """
    if (wallet.lower(), coin) not in cfg.allowed_pairs or mark_px <= 0:
        return None
    sb, sa = _sign(leader_pos_before), _sign(leader_pos_after)
    holding = follower is not None and follower.signed_sz != 0
    holding_long = holding and follower.signed_sz > 0

    # 1. leader still holds a non-flat position and did NOT flip -> no follower action (hold through scale)
    if sa != 0 and sa == sb:
        return None

    # 2. leader reached flat, OR flipped: if we hold, exit first (reduce-only)
    if holding and (sa == 0 or (sa != 0 and sb != 0 and sa != sb)):
        return OrderIntent(wallet=wallet, coin=coin, is_buy=(not holding_long),
                           sz=abs(follower.signed_sz), limit_px=mark_px, reduce_only=True)

    # 3. leader opened from flat (or is now directional and we are flat) -> enter if not already aligned
    if sa != 0 and not holding:
        if now - last_entry_ts < cfg.cooldown_s:
            return None
        want_long = sa > 0
        return OrderIntent(wallet=wallet, coin=coin, is_buy=want_long,
                           sz=cfg.order_size_usd / mark_px, limit_px=mark_px, reduce_only=False)

    return None


def risk_exit(follower: FollowerPos, mark_px: float, now: float,
              cfg: ControllerConfig) -> tuple[OrderIntent | None, FollowerPos]:
    """Independent follower risk exits: stop, trailing-TP, max-hold. Returns (intent_or_None,
    updated_follower). The follower's peak_gain_frac is updated in place for trailing logic."""
    if follower.signed_sz == 0 or mark_px <= 0 or follower.entry_px <= 0:
        return None, follower
    is_long = follower.signed_sz > 0
    gain = (mark_px - follower.entry_px) / follower.entry_px
    if not is_long:
        gain = -gain
    # update peak
    if gain > follower.peak_gain_frac:
        follower.peak_gain_frac = gain

    def _flatten(reason):
        return OrderIntent(wallet="__flatten__", coin=follower.coin, is_buy=(not is_long),
                           sz=abs(follower.signed_sz), limit_px=mark_px, reduce_only=True)

    # hard stop
    if gain <= -cfg.stop_frac:
        return _flatten("stop"), follower
    # trailing TP (only after armed)
    if follower.peak_gain_frac >= cfg.trail_arm_frac and (follower.peak_gain_frac - gain) >= cfg.trail_frac:
        return _flatten("trail"), follower
    # max hold
    if now - follower.opened_ts >= cfg.max_hold_s:
        return _flatten("maxhold"), follower
    return None, follower

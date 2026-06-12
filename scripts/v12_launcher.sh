#!/bin/bash
# v12_launcher.sh — wrapper for the live HL copy trader under launchd.
#
# 2026-06-02 (Alberto go-live): REPURPOSED from hl_copy_trader_v11.py (fixed-size) to the V15 proportional
# copy engine hl_prop_copy.py (codex-SHIP'd, dry-run-validated). V11 was flattened first (clean slate).
# Same supervised slot (com.quantslab.v12-copy-trader.plist, KeepAlive). NOTE for CoS: the plist is
# unchanged; only this repo launcher script was re-pointed. To revert to V11, restore SCRIPT/CONFIG below.
# Used by /Library/LaunchDaemons/com.quantslab.v12-copy-trader.plist.
# Note: no flock; launchd KeepAlive guarantees one supervised instance.
# Pause: touch /tmp/v12_pause to make the launcher exit cleanly; remove to resume.

set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
# 2026-06-08 (Alberto "Build"+"Don't wait"): repointed to the codex-SHIPPED (7 rounds) EXIT-COPY bot --
# commitment entry + follow-leader-exit + overlay (disaster/trailing-TP/funding/24h), liquid builder perps
# (xyz:CL/xyz:SILVER), venue-native basket, DUST $10-50. Live-small validation of entry concession vs the
# leader commitment fill. Same supervised slot. To revert: SCRIPT="strategies/live/hl_copy_measure.py";
# ARGS="--wallets config/copy_live_realpnl_5.json config/copy_live_backtest_5.json --live".
# 2026-06-09 (Alberto go-live): live-tiny PROBE copying 0x3ea004bb (passed shadow gate +3-5% net, 2% DD).
# Proportional copy via the codex-SHIP'd hl_prop_copy engine. Tight gates: -8% account stop, $700 gross cap,
# 5x max lev, per-position 12% trail, per-leader -35% breaker, per-coin 35%. Kill: touch /tmp/v12_pause.
# 2026-06-11 (Alberto V16 go-live, Fable build): repointed to V16 -- top-decile faithful copy on liquid
# majors (codex SHIP-validated strategy; 100-wallet cohort, $50 fixed/trade, 10x cap, book-level risk).
# Engine = hl_copy_trader_v16.py (V15 engine subclass + liquid whitelist hard guard + fail-fast asserts).
# GATED: do NOT remove halt flags until the V16 codex gate passes AND Alberto gives the launch word.
# To revert: SCRIPT="strategies/live/hl_prop_copy.py"; ARGS="--config config/copy_live_basket.json".
# 2026-06-11 ~12:40 (Alberto V17 go-live msg 9254, Fable sprint): repointed to V17 -- GATED HERD
# COPY. V16 + knet>=0 net-consensus entry gate + $50 + stop -25% latched + util 0.40 + backstop 5x
# + netx 2.5x + coin-side 2.0x caps + seed audit (>=98/100 configured, top-30 hard block) +
# stale-tracker kill. Codex chain: strategy SHIP-WITH-CHANGES (rounds 1-4) + CODE review GO
# (rounds 1-2; 4 P1s fixed + verified). Replay (real marks) +$260/+$60 vs V16 +$165/+$32; forward
# gate spread +140bps. Rollback gates: 18d forward fail -> revert V16; live DD 18% pre-forward ->
# revert V16. To revert to V16: SCRIPT="strategies/live/hl_copy_trader_v16.py";
# ARGS="--config config/copy_trader_wallets_v16.json".
# 2026-06-12 (quant): UNIVERSE EXPANSION deploy. Same V17 engine + cohort; config adds the
# global.expansion block (29 net-positive new coins: 7 crypto-main + 22 xyz builder-dex) + the
# codex-required guards (per-coin kill -$25/n20-fee-net-mean<0, expansion-wide kill -$50, builder
# seed retry, restart kill-state reload, NEW/OLD reject tagging). Alberto GO (msg 9351 "only
# positive full set") + codex GO (session 019e2d30, re-review). Guards inert if the expansion block
# is absent. To revert to the baseline 10-coin V17: ARGS="--config config/copy_trader_wallets_v17.json".
SCRIPT="strategies/live/hl_copy_trader_v17.py"
ARGS="--config config/copy_trader_wallets_v17_expansion.json"

cd "$WORKDIR"

# 2026-05-22: pause flag for ops maintenance (drift heal, schema changes, etc).
# Touch /tmp/v12_pause to make launcher exit 0 cleanly; remove to resume.
# 2026-06-10 (quant): /tmp is volatile (cleared by macOS periodic / wipe). After the reboot-OOM
# incident where launchd KeepAlive auto-re-armed an unapproved basket on machine-wake, added a
# PERSISTENT halt file in the repo. Either flag pauses the launcher. Remove BOTH to resume.
# Resume requires: rm -f /tmp/v12_pause "$WORKDIR/.HALT_COPY"
if [ -f /tmp/v12_pause ] || [ -f "$WORKDIR/.HALT_COPY" ]; then
  echo "[$(date '+%F %T %Z')] v12_launcher: pause flag present (/tmp/v12_pause or .HALT_COPY), exiting (paused)"
  sleep 5
  exit 0
fi

# Source .env safely (auto-export, then disable)
if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

echo "[$(date '+%F %T %Z')] v12_launcher: starting $SCRIPT $ARGS"
exec "$PYTHON" "$SCRIPT" $ARGS

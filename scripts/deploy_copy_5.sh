#!/bin/bash
# deploy_copy_5.sh -- clean, verified deploy of the chosen 5 copy wallets into V15.
# Usage: bash scripts/deploy_copy_5.sh 0xADDR1 0xADDR2 0xADDR3 0xADDR4 0xADDR5
# Prep'd 2026-06-03 to fix this session's messy multi-restart + WS-stall failure mode.
# Does NOT run unless invoked with 5 addresses. Copy is 1:1 (sizing_leverage 1.0), -15% account stop.
set -euo pipefail
cd /Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
CFG=config/copy_trader_wallets_v15_prop.json

if [ "$#" -ne 5 ]; then echo "ERROR: need exactly 5 wallet addresses"; exit 1; fi

# 1) write the 5 wallets + force copy-1:1 (no leverage knob) + keep -15% stop
$PY - "$@" <<'PYEOF'
import json, sys
addrs=[a.lower() for a in sys.argv[1:6]]
cfg=json.load(open("config/copy_trader_wallets_v15_prop.json"))
json.dump(cfg.get("wallets",{}), open("/tmp/wallets_backup_predeploy.json","w"))
cfg["wallets"]={a:{"group":"v15_chosen5"} for a in addrs}
cfg["global"]["sizing_leverage"]=1.0   # copy 1:1, NO leverage knob
cfg["global"]["global_stop_pct"]=0.15  # -15% account trailing stop
json.dump(cfg, open("config/copy_trader_wallets_v15_prop.json","w"), indent=2)
print("config written: 5 wallets, copy 1:1, -15% stop ->", addrs)
PYEOF

# 2) un-pause + restart (KeepAlive respawns the launcher with the new config)
rm -f /tmp/v12_pause
pkill -f hl_prop_copy.py 2>/dev/null || true
echo "pause cleared + old process killed; waiting for KeepAlive respawn..."

# 3) VERIFY: poll the log for parity-OK / live-trading-enabled (the gate that stalled last time)
LOG=/tmp/ql-v12-copy-trader-launchd.log
ok=0
# 36 x 5s = 180s. Worst case = up to 30s (launchd ThrottleInterval) for respawn
# after the pause flag is cleared + WS parity warmup. 90s was too tight -> false ALERT risk.
for i in $(seq 1 36); do   # ~180s
  sleep 5
  if tail -40 "$LOG" 2>/dev/null | grep -q "live trading enabled"; then
    echo "OK: WS parity confirmed, live trading enabled (after ~$((i*5))s)"; ok=1; break
  fi
  if ! pgrep -f hl_prop_copy.py >/dev/null; then echo "WARN: engine not running yet (respawn pending)"; fi
done
if [ "$ok" -ne 1 ]; then
  echo "ALERT: parity NOT confirmed in 180s -- engine may be WS-stalled (the 16-leader failure mode). Investigate; do NOT assume deployed."
  tail -8 "$LOG"
  exit 2
fi

# 4) report resulting book
$PY - <<'PYEOF'
import requests
p="0x11ca20aeb7cd014cf8406560ae405b12601994b4"
st=requests.post("https://api.hyperliquid.xyz/info",json={"type":"clearinghouseState","user":p},timeout=10).json()
ms=st["marginSummary"]; ntl=float(ms["totalNtlPos"]); mu=float(ms["totalMarginUsed"])
print(f"DEPLOYED: gross ${ntl:.0f}, margin used ${mu:.0f}, positions {len(st['assetPositions'])}")
PYEOF
echo "deploy_copy_5 complete."

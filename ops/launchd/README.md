# ops/launchd — launchd service templates (infra-as-code)

Canonical, version-controlled copies of the launchd services for the LIVE HL copy-trading stack. macOS
loads plists from `/Library/LaunchDaemons` (system) or `~/Library/LaunchAgents` (user); those installed
copies should match the templates here. **CoS owns installing/loading/unloading** (needs the right session
+ sometimes sudo). This dir is the source of truth for what SHOULD be loaded.

## The KEEP set (live stack — must stay loaded)
| Service | Kind | Supervises |
|---|---|---|
| `com.quantslab.v12-copy-trader` | system daemon | the V17 engine via `scripts/v12_launcher.sh` (KeepAlive) |
| `com.quantslab.hl-s3-fills-daily` | user agent | 06:20 S3 data refresh (`scripts/hl_s3_fills_daily_refresh.sh`) |
| `com.quantslab.v12-watchdog` | user agent | engine stall watchdog (`scripts/v12_heartbeat_watchdog.sh`, STATS-heartbeat content) — the ONE sanctioned killer (promoted 2026-07-10) |
| `com.quantslab.pnl-tracker` | system daemon | live PnL → Telegram (`tools/pnl_tracker.py`) |
| `com.quantslab.hl-mark-collector` | user agent | **NEW (this dir)** — supervises the live 1m mark collector |

## Phase-1 activation runbook (CoS to execute; codex-reviewed 2026-07-10)
Live V17 engine must stay untouched. Do these three; verify after each.

### 1. Supervise the mark collector (fixes the unsupervised SPOF)
```bash
# 1a. Stop the old unsupervised nohup collector GRACEFULLY and WAIT for it to exit (codex Q4: a lingering
#     old process + the new one share the same {day}.parquet.tmp path -> race / lost final rows).
pkill -TERM -f 'data_pipeline/hl_live_mark_collector.py' || true
for i in $(seq 1 20); do pgrep -f 'data_pipeline/hl_live_mark_collector.py' >/dev/null || break; sleep 1; done
pgrep -f 'data_pipeline/hl_live_mark_collector.py' && { echo "old collector still up — SIGKILL"; pkill -9 -f 'data_pipeline/hl_live_mark_collector.py'; sleep 2; } || echo "old collector exited"
# 1a-final: PROVE it is gone before loading the new agent (codex R3) — do NOT load if any old collector remains.
pgrep -f 'data_pipeline/hl_live_mark_collector.py' && { echo "ABORT: old collector still present, do not load"; exit 1; } || echo "confirmed no old collector"
# 1b. Only now install + load the KeepAlive agent (RunAtLoad starts a fresh one).
cp /Users/hermes/quants-lab/ops/launchd/com.quantslab.hl-mark-collector.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.quantslab.hl-mark-collector.plist
# 1c. verify: exactly one fresh collector, supervised, writing marks.
launchctl list | grep hl-mark-collector
pgrep -fl 'hl_live_mark_collector'   # expect exactly ONE
tail -f /tmp/ql-hl-mark-collector.launchd.log
```
Handover: the collector writes `app/data/hl_mark_1m_hot/{day}.parquet` atomically and resumes the current UTC
day on start. Marks are safe ONLY if the old process has fully exited (step 1a) before the new one loads — the
brief gap (seconds) is at most one 60s poll, and SIGTERM flushes the buffer on the way out.

### 2. Resolve the two-killers problem (retire the log-mtime watchdog, keep the STATS one)
Codex (2026-07-10): log-mtime staleness is NOT loop health — a reconnect/error loop can keep the log fresh
while the trading loop never runs its protected checks, bypassing the mtime watchdog. The heartbeat watchdog
(`scripts/v12_heartbeat_watchdog.sh`, com.quantslab.v12-watchdog, ALREADY loaded) keys on the STATS heartbeat
CONTENT and is strictly more targeted. Keep it; retire the mtime one.
```bash
# retire the log-mtime watchdog
launchctl unload ~/Library/LaunchAgents/com.quantslab.v17-stall-watchdog.plist
launchctl list | grep -c v17-stall-watchdog   # expect 0
# confirm the sanctioned heartbeat killer stays up
launchctl list | grep v12-watchdog            # expect it present (PID)
```
(The `~/.claude/scripts/v17-copy-engine-stall-watchdog.sh` script + its plist are archived after unload.)

### 3. kill_switch.sh (no daemon action — repo script)
`scripts/kill_switch.sh` was rewritten to actually stop V17 (halt flags + kill engine PID + flatten via
`tools/flatten_all_offline.py`). No launchd change; just verify it runs (`bash scripts/kill_switch.sh --pause`
is the safe no-op-ish smoke: sets flags only; then `rm -f /tmp/v12_pause .HALT_COPY` to resume).

## INVARIANT (must hold before + during activation) — log-inode stability
The sanctioned heartbeat watchdog reads the engine's STATS line from `/tmp/ql-v12-copy-trader-launchd.log`.
If that file is externally ROTATED/REPLACED (new inode) while the engine keeps writing its old fd, the
watchdog sees no new post-offset `STATS:` and can FALSE-KILL a healthy engine (codex R4). Enforce:
- Do NOT `mv`/rotate/truncate `/tmp/ql-v12-copy-trader-launchd.log` while V17 is live.
- If the log must be rotated, do it as: kill_switch --pause → confirm engine exited → rotate → remove halt
  flags (engine + watchdog both re-baseline on the fresh log). Preflight check before activation:
  `ls -li /tmp/ql-v12-copy-trader-launchd.log` and confirm no logrotate/tmpwatch touches it.
Note: after a watchdog (re)start it rebaselines at current log size, so a bot already frozen BEFORE the
watchdog starts is killed after grace + stale checks (~7 min), not instantly — acceptable, documented.

## Secrets note (Phase-2)
The retired `/Library/LaunchDaemons/com.quantslab.{api,pipeline}.plist` contain plaintext Bybit/Telegram
secrets. The KEEP-set plists above are clean (only `MONGO_URI`/`HOME`/`PATH` env; the stall watchdog reads
the TG token from `.env`). Removing the retired plists in Phase 2 removes the plaintext secrets.

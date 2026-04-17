# Infrastructure Hardening Plan
**Status:** DRAFT (Apr 17, 2026)
**Goal:** Scale from 2 bot containers to dozens of concurrent strategies

## Current State (broken)

### Measured Fragility Points
| Issue | Severity | Measured Impact |
|-------|----------|-----------------|
| HB API crash loop | CRITICAL | 266 restarts/24h (every 5.4min) |
| WS connection overload | HIGH | 38 close-code-1006 events/23h on Bot A, bursts of 12 correlated across bots |
| REST rate limit storms | HIGH | Bot A hitting 49/49 GET limit every 2min for hours |
| No container restart policies | HIGH | restart=no on all containers |
| Memory ceiling | MEDIUM | 1.3GB/3.8GB used (34%), each new bot = 330MB |
| LaunchDaemon restart loop | HIGH | docker-stack plist runs every 5min, recreating crashed containers |
| Order book WS waste | HIGH | X5 (directional, 1h) opens orderbook WS per pair — never used |

### Root Causes
1. **HB API crash loop:** docker-stack LaunchDaemon runs `docker compose up -d` every 300s. API crashes from WS reconnection storm → asyncio overload → unhandled exception → LaunchDaemon recreates → repeat. NOT Docker restart policy (restart=no).
2. **WS overload:** Each bot: 12 pairs x (candle WS + orderbook WS) + private WS = 25+ connections. Two bots = 50+ from one IP. Bybit demo culls excess (server-side, correlated timestamps).
3. **REST storms:** After WS drops, HB rebuilds order books via REST. 12 pairs x concurrent requests = saturated 49/s GET limit within seconds. Blocks real operations for hours.
4. **Key insight:** X5 does NOT need orderbook WS or candle WS. Uses 1h candles (already in parquet) + Coinalyze REST. All WS connections are wasted overhead.

## Target Architecture

Two-tier execution model:

```
Mac Mini (Colima VM 8-10GB)
├── QL Pipeline (LaunchDaemon)
│   ├── TaskOrchestrator (data collection, features, signals)
│   ├── RiskStateTask (portfolio risk, every 60s)
│   └── ExecutionService (for Tier 2 strategies)
│       - Signals from controller logic
│       - Orders via BybitExchangeClient REST
│       - 1 private WS per API key for fills
│       - Cost: ~200MB shared, NOT per-strategy
│
├── Docker (Colima)
│   ├── hummingbot-api (pre-patched image, restart:unless-stopped, healthcheck)
│   ├── hummingbot-broker (EMQX MQTT)
│   ├── hummingbot-postgres
│   ├── redis (rate limit coordination, ~50MB)
│   │
│   ├── [Tier 1: Fast strategies needing real-time data]
│   │   └── HB bot containers (6 pairs max each, restart:on-failure)
│   │
│   └── [Tier 2: Slow strategies — NO containers needed]
│       └── X5, X8, all 1h strategies handled by ExecutionService
│
├── MongoDB (native macOS)
└── Bybit (4-6 API keys for rate limit partitioning)
```

### Resource Budget at Scale (20 strategies, ~200 pairs)

| Component | Memory | WS Connections | REST calls/s |
|-----------|--------|---------------|-------------|
| HB API | 350MB | 0 | Minimal |
| EMQX | 210MB | 0 (internal) | 0 |
| Postgres | 100MB | 0 | 0 |
| Redis | 50MB | 0 | 0 |
| Tier 1 bots (4 x 6 pairs) | 1.3GB | 32 | ~10/s per key |
| Execution Service (shared) | 200MB | 6 (1 private WS/key) | ~5/s per key |
| QL Pipeline | 500MB | 0 | ~2/s |
| **Total** | **~2.7GB** | **~38** | **~17/s** |

## Prioritized Fixes

### P0 — Do Now (stop the bleeding, < 1 day)

1. **Increase Colima VM memory: 3.8GB to 8GB**
   ```bash
   colima stop && colima start --memory 8 --cpu 4
   ```

2. **Add restart:unless-stopped to hummingbot-api in docker-compose.yml**
   - File: `/Users/hermes/hummingbot/hummingbot-api/docker-compose.yml`
   - Add `restart: unless-stopped` under hummingbot-api service

3. **Kill the 5-min restart loop**
   - File: `scripts/launchd/com.quantslab.docker-stack.plist`
   - Remove `StartInterval` key
   - Keep `RunAtLoad: true` (boot-time start only)
   - Reload: `sudo launchctl unload + load`

4. **Verify:** HB API stays up >1h without external restarts

### P1 — This Week (structural fixes)

5. **Add restart policies to bot containers**
   - File: HB API `services/docker_service.py` line 278
   - Add: `restart_policy={"Name": "on-failure", "MaximumRetryCount": 5}`

6. **Bake Bybit demo patches into Docker image**
   - Build `quants-lab/hummingbot-api:demo` with patches pre-applied
   - Eliminate fragile sed patching after every restart

7. **Add health checks to docker-compose**
   ```yaml
   healthcheck:
     test: ["CMD", "curl", "-sf", "-u", "admin:admin", "http://localhost:8000/"]
     interval: 30s
     timeout: 10s
     retries: 3
   ```

8. **Reduce pairs per bot: 12 to 6**
   - Deploy 4 bots instead of 2 for same coverage
   - Halves WS connections per container

9. **Add circuit breaker to BybitExchangeClient**
   - States: CLOSED/OPEN/HALF_OPEN
   - Failure threshold, recovery timeout
   - When OPEN: all calls fail fast

10. **Rate limit coordination via MongoDB**
    - Shared token bucket accessible to all bots
    - Each bot checks remaining budget before REST calls

### P2 — This Month (architectural evolution)

11. **Build Execution Service in QL pipeline**
    - For all 1h+ strategies (X5, X8, future)
    - Signal computation from controller logic
    - Order execution via BybitExchangeClient REST
    - Fill monitoring via 1 private WS per API key
    - Position lifecycle in MongoDB
    - Cost: ~0MB marginal per strategy

12. **WS Multiplexer via MQTT**
    - Single WS connection per pair to Bybit
    - Fan out to bots via EMQX (already running)
    - For fast strategies only

13. **Multi-API-key rotation (4-6 keys)**
    - REST limits scale linearly with keys
    - Assign keys to bot groups
    - Add `api_key_group` to StrategyMetadata

14. **Migrate X5, X8 to Execution Service tier**

15. **Deploy at scale (20+ strategies)**

## Files to Modify

### P0 Changes
- `docker-compose.yml` — restart policy, healthcheck, mem_limit
- `scripts/launchd/com.quantslab.docker-stack.plist` — remove StartInterval

### P1 Changes
- HB API `services/docker_service.py` line 278 — restart policy for bot containers
- `docker-build/Dockerfile` — baked patches for API image
- `app/services/bybit_exchange_client.py` — circuit breaker, retry, rate limit awareness
- `app/tasks/watchdog_task.py` — container health, WS count, rate limit monitoring

### P2 Changes (new files)
- `app/services/execution_service.py` — lightweight execution layer
- `app/services/ws_fill_monitor.py` — private WS for fill tracking
- `app/engines/strategy_registry.py` — add `execution_tier` field

## What Requires HB Changes vs Not

**No HB changes needed:**
- All P0 fixes (Docker config, LaunchDaemon)
- Restart policies (docker_service.py is HB API service layer, not core)
- All QL pipeline changes
- Execution Service (entirely new code)
- Reducing pairs per bot (deployment config)

**Requires HB API service layer change (trivial):**
- `docker_service.py`: 1 line for restart_policy

**Would require HB core changes (NOT recommended):**
- Disabling orderbook WS per-connector
- Modifying WS reconnection behavior
- Candle feed from local source

**Workaround:** Execution Service bypasses HB entirely for slow strategies, avoiding all HB core limitations.

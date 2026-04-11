"""
Dashboard router — mounts at /dashboard on the QL API (:8001).

Endpoints:
    GET /dashboard              HTML page (fetches data via JS)
    GET /dashboard/api/health   System health JSON
    GET /dashboard/api/positions Active + recent closed positions
    GET /dashboard/api/trades   Last 50 resolved trades
    GET /dashboard/api/performance  30-day rolling metrics per engine

Injected into core FastAPI app from cli.py serve_api().
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse
from pymongo import MongoClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

# ── HTML ──────────────────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QuantsLab Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root { --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a; --text: #e2e8f0;
          --muted: #8892a4; --green: #22c55e; --red: #ef4444; --blue: #3b82f6;
          --yellow: #eab308; }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'SF Mono', 'Fira Code', monospace;
         font-size: 13px; }
  .header { background: var(--card); border-bottom: 1px solid var(--border); padding: 14px 24px;
            display: flex; align-items: center; justify-content: space-between; }
  .header h1 { font-size: 15px; font-weight: 600; letter-spacing: 0.5px; }
  .header .ts { color: var(--muted); font-size: 11px; }
  .main { padding: 20px 24px; display: grid; gap: 16px; }
  .row { display: grid; gap: 16px; }
  .row-2 { grid-template-columns: 1fr 1fr; }
  .row-3 { grid-template-columns: 1fr 1fr 1fr; }
  .row-4 { grid-template-columns: repeat(4, 1fr); }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
  .card h2 { font-size: 11px; font-weight: 500; color: var(--muted); letter-spacing: 1px;
             text-transform: uppercase; margin-bottom: 12px; }
  .stat { font-size: 28px; font-weight: 700; }
  .stat-label { font-size: 11px; color: var(--muted); margin-top: 2px; }
  .green { color: var(--green); }
  .red { color: var(--red); }
  .yellow { color: var(--yellow); }
  .blue { color: var(--blue); }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
  .dot.ok { background: var(--green); }
  .dot.warn { background: var(--yellow); }
  .dot.err { background: var(--red); }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; font-size: 10px; color: var(--muted); letter-spacing: 1px;
       text-transform: uppercase; padding: 4px 8px; border-bottom: 1px solid var(--border); }
  td { padding: 6px 8px; border-bottom: 1px solid var(--border); font-size: 12px; }
  tr:last-child td { border-bottom: none; }
  .badge { display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 10px; }
  .badge.tp { background: rgba(34,197,94,0.15); color: var(--green); }
  .badge.sl { background: rgba(239,68,68,0.15); color: var(--red); }
  .badge.tl { background: rgba(234,179,8,0.15); color: var(--yellow); }
  .badge.active { background: rgba(59,130,246,0.15); color: var(--blue); }
  .health-item { display: flex; align-items: center; padding: 5px 0;
                 border-bottom: 1px solid var(--border); }
  .health-item:last-child { border-bottom: none; }
  .health-item .name { flex: 1; color: var(--text); }
  .health-item .age { color: var(--muted); font-size: 11px; }
  canvas { max-height: 220px; }
  .loading { color: var(--muted); font-size: 12px; }
  .err-msg { color: var(--red); font-size: 11px; padding: 8px 0; }
</style>
</head>
<body>
<div class="header">
  <h1>QuantsLab</h1>
  <span class="ts" id="ts">Loading...</span>
</div>
<div class="main">

  <!-- Row 1: KPI cards -->
  <div class="row row-4" id="kpi-row">
    <div class="card"><h2>Active Positions</h2><div class="stat blue" id="kpi-active">—</div></div>
    <div class="card"><h2>All-Time Trades</h2><div class="stat" id="kpi-trades">—</div></div>
    <div class="card"><h2>Win Rate (30d)</h2><div class="stat" id="kpi-wr">—</div></div>
    <div class="card"><h2>PnL (30d)</h2><div class="stat" id="kpi-pnl">—</div></div>
  </div>

  <!-- Row 2: Health + Active Positions -->
  <div class="row row-2">
    <div class="card">
      <h2>System Health</h2>
      <div id="health-list" class="loading">Loading...</div>
    </div>
    <div class="card">
      <h2>Active Positions</h2>
      <div id="positions-table" class="loading">Loading...</div>
    </div>
  </div>

  <!-- Row 3: PnL chart -->
  <div class="row">
    <div class="card">
      <h2>Cumulative PnL (30d)</h2>
      <canvas id="pnl-chart"></canvas>
    </div>
  </div>

  <!-- Row 4: Recent trades -->
  <div class="row">
    <div class="card">
      <h2>Recent Trades</h2>
      <div id="trades-table" class="loading">Loading...</div>
    </div>
  </div>

</div>
<script>
const BASE = window.location.origin;
const API = BASE + '/dashboard/api';

function fmt(v, decimals=2) {
  if (v == null) return '—';
  return parseFloat(v).toFixed(decimals);
}

function pnlClass(v) {
  if (v == null) return '';
  return parseFloat(v) >= 0 ? 'green' : 'red';
}

function closeBadge(ct) {
  if (!ct) return '';
  const t = ct.toUpperCase();
  if (t === 'TP') return '<span class="badge tp">TP</span>';
  if (t === 'SL') return '<span class="badge sl">SL</span>';
  if (t.includes('TIME') || t === 'EXPIRED') return '<span class="badge tl">TIME</span>';
  return `<span class="badge tl">${ct}</span>`;
}

function tsAge(ms) {
  if (!ms) return '—';
  const diff = Date.now() - ms;
  const m = Math.floor(diff / 60000);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h/24)}d ago`;
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status}`);
  return r.json();
}

let pnlChart = null;

async function loadHealth() {
  try {
    const data = await fetchJSON(API + '/health');
    const el = document.getElementById('health-list');
    const items = data.tasks || [];
    if (!items.length) { el.innerHTML = '<div class="err-msg">No task data</div>'; return; }
    el.innerHTML = items.map(t => {
      const cls = t.age_minutes < 90 ? 'ok' : t.age_minutes < 180 ? 'warn' : 'err';
      const age = t.age_minutes != null ? `${Math.round(t.age_minutes)}m ago` : 'never';
      return `<div class="health-item"><span class="dot ${cls}"></span><span class="name">${t.name}</span><span class="age">${age}</span></div>`;
    }).join('');

    const sys = data.system || {};
    if (sys.hb_api != null || sys.ql_api != null) {
      const hbCls = sys.hb_api ? 'ok' : 'err';
      const qlCls = sys.ql_api ? 'ok' : 'err';
      el.innerHTML += `<div class="health-item"><span class="dot ${hbCls}"></span><span class="name">HB API</span><span class="age">${sys.hb_api ? 'up' : 'down'}</span></div>`;
      el.innerHTML += `<div class="health-item"><span class="dot ${qlCls}"></span><span class="name">QL API</span><span class="age">${sys.ql_api ? 'up' : 'down'}</span></div>`;
    }
  } catch(e) {
    document.getElementById('health-list').innerHTML = `<div class="err-msg">Error: ${e}</div>`;
  }
}

async function loadPositions() {
  try {
    const data = await fetchJSON(API + '/positions');
    const el = document.getElementById('positions-table');
    document.getElementById('kpi-active').textContent = data.active_count || 0;
    const rows = data.active || [];
    if (!rows.length) { el.innerHTML = '<div style="color:var(--muted);padding:8px 0">No active positions</div>'; return; }
    el.innerHTML = `<table>
      <tr><th>Engine</th><th>Pair</th><th>Dir</th><th>Entry</th><th>uPnL%</th><th>Age</th></tr>
      ${rows.map(r => `<tr>
        <td>${r.engine}</td><td>${r.pair}</td>
        <td class="${r.direction === 'LONG' ? 'green' : 'red'}">${r.direction}</td>
        <td>${fmt(r.entry, 4)}</td>
        <td class="${pnlClass(r.upnl_pct)}">${r.upnl_pct != null ? fmt(r.upnl_pct, 2) + '%' : '—'}</td>
        <td>${tsAge(r.placed_at)}</td>
      </tr>`).join('')}
    </table>`;
  } catch(e) {
    document.getElementById('positions-table').innerHTML = `<div class="err-msg">Error: ${e}</div>`;
  }
}

async function loadTrades() {
  try {
    const data = await fetchJSON(API + '/trades');
    const el = document.getElementById('trades-table');
    document.getElementById('kpi-trades').textContent = data.total_count || 0;
    const rows = data.trades || [];
    if (!rows.length) { el.innerHTML = '<div style="color:var(--muted);padding:8px 0">No resolved trades yet</div>'; return; }
    el.innerHTML = `<table>
      <tr><th>Engine</th><th>Pair</th><th>Dir</th><th>Close</th><th>PnL $</th><th>Slip bps</th><th>When</th></tr>
      ${rows.map(r => `<tr>
        <td>${r.engine}</td><td>${r.pair}</td>
        <td class="${r.direction === 'LONG' ? 'green' : 'red'}">${r.direction}</td>
        <td>${closeBadge(r.close_type)}</td>
        <td class="${pnlClass(r.pnl)}">${r.pnl != null ? '$' + fmt(r.pnl, 2) : '—'}</td>
        <td class="${r.slip_bucket === 'danger' ? 'red' : r.slip_bucket === 'borderline' ? 'yellow' : ''}">${r.slippage_bps != null ? fmt(r.slippage_bps, 1) : '—'}</td>
        <td>${tsAge(r.resolved_at)}</td>
      </tr>`).join('')}
    </table>`;
  } catch(e) {
    document.getElementById('trades-table').innerHTML = `<div class="err-msg">Error: ${e}</div>`;
  }
}

async function loadPerformance() {
  try {
    const data = await fetchJSON(API + '/performance');
    const engines = Object.keys(data.engines || {});
    if (!engines.length) return;

    // KPI: aggregate
    let totalPnl = 0, totalWins = 0, totalTrades = 0;
    for (const e of engines) {
      const m = data.engines[e];
      totalPnl += m.total_pnl || 0;
      totalWins += m.wins || 0;
      totalTrades += m.trades || 0;
    }
    document.getElementById('kpi-pnl').textContent = '$' + totalPnl.toFixed(0);
    document.getElementById('kpi-pnl').className = 'stat ' + (totalPnl >= 0 ? 'green' : 'red');
    document.getElementById('kpi-wr').textContent = totalTrades > 0
      ? (totalWins / totalTrades * 100).toFixed(0) + '%' : '—';

    // Chart: cumulative PnL from daily snapshots
    const snapshots = data.snapshots || [];
    if (snapshots.length > 0) {
      const labels = [...new Set(snapshots.map(s => s.date))].sort();
      const colors = { E1: '#3b82f6', E2: '#22c55e', E3: '#f97316', E4: '#a855f7' };
      const datasets = engines.map(eng => {
        let cum = 0;
        const pts = labels.map(d => {
          const snap = snapshots.find(s => s.date === d && s.engine === eng);
          if (snap) cum += (snap.total_pnl || 0);
          return cum;
        });
        return { label: eng, data: pts, borderColor: colors[eng] || '#8892a4',
                 backgroundColor: 'transparent', tension: 0.3, pointRadius: 2 };
      });

      if (pnlChart) pnlChart.destroy();
      const ctx = document.getElementById('pnl-chart').getContext('2d');
      pnlChart = new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
          plugins: { legend: { labels: { color: '#8892a4', font: { size: 11 } } } },
          scales: {
            x: { ticks: { color: '#8892a4', font: { size: 10 }, maxTicksLimit: 10 },
                 grid: { color: '#2a2d3a' } },
            y: { ticks: { color: '#8892a4', font: { size: 10 },
                          callback: v => '$' + v.toFixed(0) },
                 grid: { color: '#2a2d3a' } },
          },
        },
      });
    }
  } catch(e) {
    console.warn('Performance load error:', e);
  }
}

async function refresh() {
  document.getElementById('ts').textContent = new Date().toUTCString();
  await Promise.all([loadHealth(), loadPositions(), loadTrades(), loadPerformance()]);
}

refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>"""


@router.get("", response_class=HTMLResponse)
async def dashboard():
    return _HTML


# ── JSON API endpoints ────────────────────────────────────────────────────────

def _get_db():
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
    if not mongo_uri:
        return None, None
    client = MongoClient(mongo_uri)
    return client, client[mongo_db]


@router.get("/api/health")
async def api_health():
    """System health: last task execution times + API reachability."""
    _, db = _get_db()

    tasks_to_check = [
        "candles_downloader_bybit",
        "bybit_derivatives",
        "feature_computation",
        "signal_scan",
        "testnet_resolver",
        "watchdog",
    ]

    task_status: List[Dict[str, Any]] = []
    if db is not None:
        now = datetime.now(timezone.utc)
        for name in tasks_to_check:
            last = db.task_executions.find_one(
                {"task_name": name, "status": "completed"},
                sort=[("started_at", -1)],
            )
            age_min = None
            if last and last.get("started_at"):
                ts = last["started_at"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_min = (now - ts).total_seconds() / 60
            task_status.append({"name": name, "age_minutes": age_min})

    # Check API reachability + HB bot status
    hb_up = False
    ql_up = False
    hb_bots = []
    hb_executors = {"total_active": 0}
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        auth = aiohttp.BasicAuth("admin", "admin")
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get("http://localhost:8000/", auth=auth) as r:
                    hb_up = r.status < 500
            except Exception:
                pass
            try:
                async with session.get("http://localhost:8001/health") as r:
                    ql_up = r.status < 500
            except Exception:
                pass
            # HB bot status
            if hb_up:
                try:
                    async with session.get("http://localhost:8000/bot-orchestration/status",
                                           auth=auth) as r:
                        if r.status == 200:
                            bot_data = await r.json()
                            bots_list = bot_data.get("data", bot_data) if isinstance(bot_data, dict) else bot_data
                            if isinstance(bots_list, dict):
                                for name, info in bots_list.items():
                                    hb_bots.append({"name": name, **info} if isinstance(info, dict) else {"name": name, "status": str(info)})
                            elif isinstance(bots_list, list):
                                hb_bots = bots_list
                except Exception:
                    pass
                try:
                    async with session.get("http://localhost:8000/executors/summary",
                                           auth=auth) as r:
                        if r.status == 200:
                            hb_executors = await r.json()
                except Exception:
                    pass
    except Exception:
        pass

    return JSONResponse({
        "tasks": task_status,
        "system": {"hb_api": hb_up, "ql_api": ql_up},
        "hb_bots": hb_bots,
        "hb_executors": hb_executors,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })


@router.get("/api/positions")
async def api_positions():
    """Active positions with basic info."""
    _, db = _get_db()
    active = []
    active_count = 0

    if db is not None:
        docs = list(db.candidates.find({"disposition": "TESTNET_ACTIVE"}).limit(20))
        active_count = len(docs)
        for doc in docs:
            active.append({
                "engine": doc.get("engine"),
                "pair": doc.get("pair"),
                "direction": doc.get("direction"),
                "entry": doc.get("decision_price") or doc.get("testnet_fill_price"),
                "placed_at": doc.get("testnet_placed_at"),
                "upnl_pct": None,  # would need live price — browser can enrich later
            })

    return JSONResponse({"active": active, "active_count": active_count})


@router.get("/api/hb-positions")
async def api_hb_positions():
    """Active executors from HB API (native bot positions)."""
    positions = []
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        auth = aiohttp.BasicAuth("admin", "admin")
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("http://localhost:8000/executors/positions/summary",
                                   auth=auth) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list):
                        positions = data
                    elif isinstance(data, dict):
                        positions = data.get("data", [])
    except Exception:
        pass
    return JSONResponse({"positions": positions})


@router.get("/api/trades")
async def api_trades():
    """Last 50 resolved trades + all-time count."""
    _, db = _get_db()
    trades = []
    total_count = 0

    if db is not None:
        total_count = db.candidates.count_documents({"disposition": "RESOLVED_TESTNET"})
        docs = list(
            db.candidates.find({"disposition": "RESOLVED_TESTNET"})
            .sort("testnet_resolved_at", -1)
            .limit(50)
        )
        for doc in docs:
            pnl = doc.get("testnet_pnl")
            trades.append({
                "engine": doc.get("engine"),
                "pair": doc.get("pair"),
                "direction": doc.get("direction"),
                "close_type": doc.get("testnet_close_type"),
                "pnl": float(pnl) if pnl is not None else None,
                "slippage_bps": doc.get("testnet_slippage_bps"),
                "slip_bucket": doc.get("testnet_slippage_bucket"),
                "resolved_at": doc.get("testnet_resolved_at"),
            })

    return JSONResponse({"trades": trades, "total_count": total_count})


@router.get("/api/performance")
async def api_performance():
    """Per-engine 30-day rolling metrics + daily snapshots for chart."""
    _, db = _get_db()
    engines_data: Dict[str, Any] = {}
    snapshots: List[Dict] = []

    if db is not None:
        # Latest daily snapshot per engine
        for doc in db.performance_daily.find().sort("date", -1).limit(200):
            engine = doc.get("engine", "")
            date = doc.get("date", "")
            snap = {
                "date": date,
                "engine": engine,
                "total_pnl": doc.get("total_pnl", 0),
                "trades": doc.get("trades", 0),
            }
            snapshots.append(snap)
            if engine not in engines_data:
                engines_data[engine] = {
                    "trades": doc.get("trades", 0),
                    "wins": doc.get("wins", 0),
                    "win_rate": doc.get("win_rate", 0),
                    "total_pnl": doc.get("total_pnl", 0),
                    "sharpe": doc.get("sharpe"),
                }

    return JSONResponse({
        "engines": engines_data,
        "snapshots": snapshots,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    })

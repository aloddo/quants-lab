#!/usr/bin/env python3
"""V12 Copy Trader -- Real-time Performance Dashboard.

All data sourced from exchange fills (MongoDB) and live HL API.
No internal PnL counters. Exchange is truth.

Usage: streamlit run scripts/v12_dashboard.py --server.port 8501
"""
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient

# ── Config ───────────────────────────────────────────────────────────────────

V12_EPOCH = datetime(2026, 5, 17, 11, 42, 7, tzinfo=timezone.utc)
HL_PARENT = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
REFRESH_INTERVAL = 86400  # seconds -- on-demand only (use Refresh Now button)
POSITION_REFRESH = 86400  # seconds -- on-demand only

st.set_page_config(page_title="V12 Dashboard", layout="wide", page_icon="📊")


@st.cache_resource
def get_db():
    return MongoClient("mongodb://localhost:27017").quants_lab


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_exchange_fills():
    """Load V12-era exchange fills (post-epoch) for PnL metrics."""
    db = get_db()
    epoch_ms = int(V12_EPOCH.timestamp() * 1000)
    fills = list(db["v11_exchange_fills"].find({"time": {"$gte": epoch_ms}}).sort("time", 1))
    return fills


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_all_exchange_fills():
    """Load ALL exchange fills for historical context."""
    db = get_db()
    return list(db["v11_exchange_fills"].find().sort("time", 1))


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_v11_trades():
    """Load V12-era trade records + V11 legacy trades still open."""
    db = get_db()
    # V12 trades
    v12 = list(db["unified_copy_trades"].find({"timestamp": {"$gte": V12_EPOCH.replace(tzinfo=None)}}).sort("timestamp", 1))
    # V11 legacy trades (for attribution of carried positions)
    v11 = list(db["unified_copy_trades"].find({"timestamp": {"$lt": V12_EPOCH.replace(tzinfo=None)}}).sort("timestamp", 1))
    return v12, v11


@st.cache_data(ttl=POSITION_REFRESH)
def load_open_positions():
    db = get_db()
    return list(db["v11_open_positions"].find())


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_v11_oids():
    db = get_db()
    return set(doc["oid"] for doc in db["v11_order_ids"].find())


@st.cache_data(ttl=POSITION_REFRESH)
def get_hl_state():
    try:
        import requests as req
        from hyperliquid.info import Info
        info = Info(skip_ws=True)
        state = info.user_state(HL_PARENT)
        margin = state.get("marginSummary", {})

        # Collect positions from main perps + builder dexes (xyz, flx)
        all_asset_positions = list(state.get("assetPositions", []))
        for dex_name in ["xyz", "flx"]:
            try:
                r = req.post("https://api.hyperliquid.xyz/info", json={
                    "type": "clearinghouseState", "user": HL_PARENT, "dex": dex_name,
                }, timeout=5)
                all_asset_positions.extend(r.json().get("assetPositions", []))
            except Exception:
                pass

        positions = [
            {
                "coin": p["position"]["coin"],
                "size": float(p["position"]["szi"]),
                "entry_px": float(p["position"]["entryPx"]),
                "upnl": float(p["position"]["unrealizedPnl"]),
                "notional": abs(float(p["position"]["szi"])) * float(p["position"]["entryPx"]),
                "side": "LONG" if float(p["position"]["szi"]) > 0 else "SHORT",
            }
            for p in all_asset_positions
            if abs(float(p["position"]["szi"])) > 1e-10
        ]
        spot = info.spot_user_state(HL_PARENT)
        spot_usdc = 0
        for b in spot.get("balances", []):
            if b["coin"] == "USDC":
                spot_usdc = float(b["total"])
        # HL unified wallet: spot USDC IS the perp margin. Don't add accountValue
        # (which is just the perp-side equity slice). Spot USDC = total equity.
        return {
            "equity": spot_usdc,
            "margin_used": float(margin.get("totalMarginUsed", 0)),
            "positions": positions,
            "total_upnl": sum(p["upnl"] for p in positions),
        }
    except Exception as e:
        return {"equity": 0, "margin_used": 0, "positions": [], "total_upnl": 0, "error": str(e)}


def compute_metrics(fills, v11_oids):
    """Compute all metrics from exchange fills."""
    if not fills:
        return {}

    closing = [f for f in fills if abs(f.get("closedPnl", 0)) > 0.0001]
    all_fees = sum(f.get("fee", 0) for f in fills)
    gross_pnl = sum(f["closedPnl"] for f in closing)
    net_pnl = gross_pnl - all_fees

    # V12-attributed
    v12_fills = [f for f in fills if f.get("oid") in v11_oids]
    v12_closing = [f for f in v12_fills if abs(f.get("closedPnl", 0)) > 0.0001]
    v12_gross = sum(f["closedPnl"] for f in v12_closing)
    v12_fees = sum(f.get("fee", 0) for f in v12_fills)
    v12_net = v12_gross - v12_fees

    pnls = [f["closedPnl"] for f in closing]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]  # BUG 4 fix: exclude zero from losses

    # Daily PnL for risk metrics
    daily_pnl = defaultdict(float)
    for f in closing:
        day = datetime.fromtimestamp(f["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        daily_pnl[day] += f["closedPnl"]
    # Subtract fees per day (spread across all fills, not just closing)
    daily_fees = defaultdict(float)
    for f in fills:
        day = datetime.fromtimestamp(f["time"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        daily_fees[day] += f.get("fee", 0)
    for day in daily_fees:
        daily_pnl[day] -= daily_fees[day]

    daily_pnl_values = list(daily_pnl.values()) if daily_pnl else [0]

    # Equity curve (only accumulate on closing fills for correct shape)
    equity_curve = []
    running = 0
    fee_running = 0
    peak = 0
    max_dd = 0
    for f in fills:
        fee_running += f.get("fee", 0)
        if abs(f.get("closedPnl", 0)) > 0.0001:
            running += f["closedPnl"]
        # Equity = gross PnL - cumulative fees
        eq = running - fee_running
        equity_curve.append({
            "time": datetime.fromtimestamp(f["time"] / 1000, tz=timezone.utc),
            "equity": eq,
        })
        peak = max(peak, eq)
        dd = peak - eq
        max_dd = max(max_dd, dd)

    wr = len(wins) / len(pnls) * 100 if pnls else 0
    pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float("inf")
    avg_win = np.mean(wins) if wins else 0
    avg_loss = np.mean(losses) if losses else 0

    # Risk metrics (Sharpe/Sortino on daily RETURNS, not dollar PnL)
    ACCOUNT_EQUITY = 530.0  # approximate starting equity for return computation
    dr_dollars = np.array(daily_pnl_values)
    dr = dr_dollars / ACCOUNT_EQUITY  # convert to returns
    sharpe = np.mean(dr) / np.std(dr) * np.sqrt(365) if np.std(dr) > 0 else 0
    downside = dr[dr < 0]
    sortino = np.mean(dr) / np.std(downside) * np.sqrt(365) if len(downside) > 0 and np.std(downside) > 0 else 0
    var_95 = np.percentile(dr_dollars, 5) if len(dr_dollars) > 1 else 0  # VaR in dollars
    es_95 = np.mean(dr_dollars[dr_dollars <= var_95]) if len(dr_dollars[dr_dollars <= var_95]) > 0 else 0

    total_volume = sum(float(f.get("sz", 0)) * float(f.get("px", 0)) for f in fills)

    return {
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "fees": all_fees,
        "fee_drag_bps": all_fees / total_volume * 10000 if total_volume > 0 else 0,
        "v12_net": v12_net,
        "v12_gross": v12_gross,
        "v12_fees": v12_fees,
        "v12_closes": len(v12_closing),
        "total_closes": len(closing),
        "total_fills": len(fills),
        "win_rate": wr,
        "profit_factor": pf,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_dd": max_dd,
        "var_95": var_95,
        "es_95": es_95,
        "skewness": float(pd.Series(dr).skew()) if len(dr) > 2 else 0,
        "kurtosis": float(pd.Series(dr).kurtosis()) if len(dr) > 3 else 0,
        "equity_curve": equity_curve,
        "daily_pnl": dict(daily_pnl),
        "total_volume": total_volume,
    }


def attribution_table(trades, group_key):
    """Build attribution breakdown by a key field."""
    groups = defaultdict(list)
    for t in trades:
        key = t.get(group_key, "unknown")
        if group_key == "target_wallet":
            key = key[:14]
        groups[key].append(t)

    rows = []
    for key, ts in sorted(groups.items(), key=lambda x: sum(t.get("pnl_usd", 0) for t in x[1]), reverse=True):
        pnls = [t.get("pnl_usd", 0) for t in ts]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        holds = [t.get("hold_s", 0) / 60 for t in ts if t.get("hold_s", 0) > 0]
        pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float("inf")
        rows.append({
            group_key: key,
            "trades": len(pnls),
            "pnl": round(sum(pnls), 4),
            "win_rate": f"{len(wins)/len(pnls)*100:.0f}%" if pnls else "0%",
            "pf": round(min(pf, 99.9), 2),
            "avg_hold_min": round(np.mean(holds), 0) if holds else 0,
            "avg_pnl": round(np.mean(pnls), 4) if pnls else 0,
        })
    return pd.DataFrame(rows)


# ── Main Dashboard ───────────────────────────────────────────────────────────

st.title("V12 Copy Trader Dashboard")
col_title, col_refresh = st.columns([5, 1])
with col_title:
    st.caption(f"V12 epoch: {V12_EPOCH.strftime('%Y-%m-%d %H:%M UTC')} | Refresh: on-demand only (click Refresh Now)")
with col_refresh:
    if st.button("Refresh Now"):
        st.cache_data.clear()
        st.rerun()

# Load data
fills = load_exchange_fills()
all_fills = load_all_exchange_fills()
v12_trades, v11_trades = load_v11_trades()
trades = v12_trades  # V12 trades for attribution
open_pos = load_open_positions()
v11_oids = load_v11_oids()
hl = get_hl_state()

if hl.get("error"):
    st.error(f"HL API ERROR: {hl['error']}")

# Show dashboard even with no V12 fills (legacy positions have uPnL)
m = compute_metrics(fills, v11_oids) if fills else {}

# ═══ S1: Executive Summary ═══
st.header("1. Executive Summary")
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Net PnL", f"${m.get('net_pnl', 0):+.2f}")
c2.metric("Gross PnL", f"${m.get('gross_pnl', 0):+.2f}")
c3.metric("Fees", f"${m.get('fees', 0):.2f}")
c4.metric("Win Rate", f"{m.get('win_rate', 0):.1f}%")
pf_display = min(m.get('profit_factor', 0), 99.9)
c5.metric("Profit Factor", f"{pf_display:.2f}")

c6, c7, c8, c9, c10 = st.columns(5)
c6.metric("V12 Net (attributed)", f"${m.get('v12_net', 0):+.2f}")
c7.metric("V12 Closes", m.get("v12_closes", 0))
c8.metric("Account Equity", f"${hl.get('equity', 0):,.0f}")
c9.metric("Unrealized", f"${hl.get('total_upnl', 0):+.2f}")
c10.metric("Margin Used", f"{hl.get('margin_used', 0) / max(hl.get('equity', 1), 1) * 100:.0f}%")

# Equity curve
if m.get("equity_curve"):
    eq_df = pd.DataFrame(m["equity_curve"])
    fig = px.line(eq_df, x="time", y="equity", title="Equity Curve (Net PnL)")
    fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

# ═══ S2: Risk Metrics ═══
st.header("2. Risk Metrics")
rc1, rc2, rc3, rc4 = st.columns(4)
rc1.metric("Sharpe (ann.)", f"{m.get('sharpe', 0):.2f}")
rc2.metric("Sortino (ann.)", f"{m.get('sortino', 0):.2f}")
rc3.metric("Max Drawdown", f"${m.get('max_dd', 0):.2f}")
rc4.metric("VaR 95%", f"${m.get('var_95', 0):.2f}")

rc5, rc6, rc7, rc8 = st.columns(4)
rc5.metric("Expected Shortfall", f"${m.get('es_95', 0):.2f}")
rc6.metric("Skewness", f"{m.get('skewness', 0):.2f}")
rc7.metric("Kurtosis", f"{m.get('kurtosis', 0):.2f}")
rc8.metric("Fee Drag (bps)", f"{m.get('fee_drag_bps', 0):.1f}")

# ═══ S3: Attribution ═══
st.header("3. Attribution")

# V12 trades only (clean start)
attr_trades = v12_trades

if attr_trades:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Wallet Group", "Individual Wallet", "Coin", "Side", "Exit Type", "Market Type"
    ])

    with tab1:
        df = attribution_table(attr_trades, "wallet_group")
        st.dataframe(df, use_container_width=True, hide_index=True)
        if len(df) > 1:
            fig = px.bar(df, x="wallet_group", y="pnl", color="pnl",
                        color_continuous_scale=["red", "gray", "green"], title="PnL by Wallet Group")
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        df = attribution_table(attr_trades, "target_wallet")
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab3:
        df = attribution_table(attr_trades, "coin")
        st.dataframe(df, use_container_width=True, hide_index=True)
        if len(df) > 1:
            fig = px.bar(df.head(20), x="coin", y="pnl", color="pnl",
                        color_continuous_scale=["red", "gray", "green"], title="PnL by Coin (top 20)")
            st.plotly_chart(fig, use_container_width=True)

    with tab4:
        df = attribution_table(attr_trades, "side")
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab5:
        df = attribution_table(attr_trades, "exit_type")
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tab6:
        df = attribution_table(attr_trades, "market_type")
        st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No closed trades yet.")

# ═══ S4: Daily Breakdown ═══
st.header("4. Daily PnL")
if m.get("daily_pnl"):
    daily_df = pd.DataFrame([
        {"date": k, "pnl": v} for k, v in sorted(m["daily_pnl"].items())
    ])
    fig = px.bar(daily_df, x="date", y="pnl", color="pnl",
                color_continuous_scale=["red", "gray", "green"], title="Daily Net PnL")
    fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
    st.plotly_chart(fig, use_container_width=True)

# ═══ S5: Position Management ═══
st.header("5. Open Positions")
if hl.get("positions"):
    # Merge exchange positions with DB wallet attribution
    db_pos = {p["coin"]: p for p in open_pos}
    for p in hl["positions"]:
        db_match = db_pos.get(p["coin"], {})
        p["wallet"] = db_match.get("wallet", "unknown")[:10]
        p["origin"] = "V11 legacy" if p["coin"] in db_pos else "V12"

    pos_df = pd.DataFrame(hl["positions"])
    col_order = ["coin", "side", "size", "entry_px", "upnl", "notional", "wallet", "origin"]
    display_cols = [c for c in col_order if c in pos_df.columns]
    st.dataframe(pos_df[display_cols], use_container_width=True, hide_index=True)

    # Summary metrics
    pc1, pc2, pc3 = st.columns(3)
    pc1.metric("Total uPnL", f"${hl['total_upnl']:+.4f}")
    pc2.metric("Total Notional", f"${pos_df['notional'].sum():.0f}")
    # Concentration
    if len(pos_df) > 1:
        total_notional = pos_df["notional"].sum()
        pos_df["weight"] = pos_df["notional"] / total_notional
        hhi = (pos_df["weight"] ** 2).sum()
        pc3.metric("HHI", f"{hhi:.3f}", help="< 0.15 = diversified, > 0.25 = concentrated")
else:
    st.info("No open positions.")

# Hold time distribution
if trades:
    hold_times = [t.get("hold_s", 0) / 60 for t in trades if t.get("hold_s", 0) > 0]
    if hold_times:
        fig = px.histogram(hold_times, nbins=30, title="Hold Time Distribution (minutes)",
                          labels={"value": "Hold Time (min)", "count": "Trades"})
        fig.update_layout(height=250, margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig, use_container_width=True)

# ═══ S6: Exposure ═══
st.header("6. Exposure")
if hl.get("positions"):
    long_notional = sum(p["notional"] for p in hl["positions"] if p["size"] > 0)
    short_notional = sum(p["notional"] for p in hl["positions"] if p["size"] < 0)
    ec1, ec2, ec3 = st.columns(3)
    ec1.metric("Long Exposure", f"${long_notional:.0f}")
    ec2.metric("Short Exposure", f"${short_notional:.0f}")
    ec3.metric("Net Exposure", f"${long_notional - short_notional:+.0f}")
else:
    st.info("No open positions for exposure analysis.")

# ═══ S7: Fees ═══
st.header("7. Fee Analysis")
fc1, fc2, fc3 = st.columns(3)
fees = m.get("fees", 0)
fc1.metric("Total Fees", f"${fees:.4f}")
fc2.metric("Fee/Volume", f"{m.get('fee_drag_bps', 0):.2f} bps")
if fees > 0 and m.get("gross_pnl", 0) > 0:
    fc3.metric("Fee % of Gross", f"{fees / m['gross_pnl'] * 100:.0f}%")
elif fees > 0 and m.get("gross_pnl", 0) < 0:
    fc3.metric("Impact", "Gross loss + fees compound")
else:
    fc3.metric("Fee Impact", "No fees yet")

# ═══ S8: System Health ═══
st.header("8. System Health")
db = get_db()
latest_fill = db["v11_exchange_fills"].find_one(sort=[("time", -1)])
sync_age = (time.time() * 1000 - latest_fill["time"]) / 1000 if latest_fill else 999

db_positions = db["v11_open_positions"].count_documents({})
exch_positions = len(hl.get("positions", []))
drift = abs(db_positions - exch_positions)

hc1, hc2, hc3, hc4 = st.columns(4)
hc1.metric("Last Fill Age", f"{sync_age:.0f}s")
hc2.metric("DB Positions", db_positions)
hc3.metric("Exchange Positions", exch_positions)
hc4.metric("Position Drift", drift, delta_color="inverse" if drift > 0 else "off")

oid_count = db["v11_order_ids"].count_documents({})
fill_count = db["v11_exchange_fills"].count_documents({})
st.caption(f"V12 oids tracked: {oid_count} | Total exchange fills stored: {fill_count}")

# ═══ S9: Top/Bottom Trades ═══
if trades:
    st.header("9. Notable Trades")
    sorted_trades = sorted(trades, key=lambda t: t.get("pnl_usd", 0))
    worst = sorted_trades[:5]
    best = sorted_trades[-5:][::-1]

    col_best, col_worst = st.columns(2)
    with col_best:
        st.subheader("Best 5")
        for t in best:
            ts = t.get("timestamp", "")
            ts_str = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else ""
            hold = t.get("hold_s", 0)
            hold_str = f"{hold/60:.0f}m" if hold > 0 else ""
            st.write(f"**{t['coin']}** {t['side']} ${t.get('pnl_usd', 0):+.4f} ({t.get('wallet_group', '?')}) {ts_str} {hold_str}")
    with col_worst:
        st.subheader("Worst 5")
        for t in worst:
            ts = t.get("timestamp", "")
            ts_str = ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else ""
            hold = t.get("hold_s", 0)
            hold_str = f"{hold/60:.0f}m" if hold > 0 else ""
            st.write(f"**{t['coin']}** {t['side']} ${t.get('pnl_usd', 0):+.4f} ({t.get('wallet_group', '?')}) {ts_str} {hold_str}")

# No auto-refresh. Use "Refresh Now" button to update.
st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')} -- Click 'Refresh Now' to update")

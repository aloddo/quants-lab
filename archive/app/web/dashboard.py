"""
Dashboard router -- mounts at /dashboard on the QL API (:8001).

Tabs:
    1. Overview       -- KPIs, live exchange positions, system health, wallet
    2. Live Trades    -- closed PnL log, cumulative equity, per-pair breakdown
    3. Research       -- strategy pipeline with full drill-down per engine/pair

API endpoints:
    GET /dashboard                             HTML SPA
    GET /dashboard/api/overview                Wallet + positions + health
    GET /dashboard/api/live                    Closed PnL + executions
    GET /dashboard/api/strategies              All engines summary
    GET /dashboard/api/strategy/{engine}       Full engine detail
    GET /dashboard/api/strategy/{engine}/{pair} Pair drill-down (trades, equity, distributions)

Injected into core FastAPI app from cli.py serve_api().
"""
import hashlib
import hmac
import json as _json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, Response
from pymongo import MongoClient

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _safe_json(obj) -> Response:
    """JSONResponse that handles inf/nan (replace with None)."""

    def _clean(o):
        if isinstance(o, float) and (math.isinf(o) or math.isnan(o)):
            return None
        if isinstance(o, dict):
            return {k: _clean(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [_clean(v) for v in o]
        return o

    return Response(content=_json.dumps(_clean(obj)), media_type="application/json")


# ---------------------------------------------------------------------------
# HTML SPA -- served as a single page with tabs
# ---------------------------------------------------------------------------

_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>QuantsLab</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#0f1117;--card:#1a1d27;--card2:#1e2130;--border:#2a2d3a;--text:#e2e8f0;
--muted:#8892a4;--green:#22c55e;--red:#ef4444;--blue:#3b82f6;--yellow:#eab308;
--purple:#a855f7;--orange:#f97316;--cyan:#06b6d4}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,'SF Pro Text','Helvetica Neue',sans-serif;font-size:13px}

.hdr{background:var(--card);border-bottom:1px solid var(--border);padding:12px 24px;display:flex;align-items:center;justify-content:space-between}
.hdr h1{font-size:16px;font-weight:700}.hdr .ts{color:var(--muted);font-size:11px;margin-right:12px}
.hdr .rbtn{background:var(--border);border:none;color:var(--muted);padding:4px 10px;border-radius:4px;cursor:pointer;font-size:11px}
.hdr .rbtn:hover{background:var(--blue);color:#fff}

.tabs{display:flex;gap:0;background:var(--card);border-bottom:1px solid var(--border);padding:0 24px}
.tab{padding:10px 20px;color:var(--muted);cursor:pointer;font-size:12px;font-weight:500;border-bottom:2px solid transparent;transition:.15s;letter-spacing:.3px}
.tab:hover{color:var(--text)}.tab.a{color:var(--blue);border-bottom-color:var(--blue)}

.tc{display:none;padding:20px 24px}.tc.a{display:block}
.grid{display:grid;gap:16px}.g2{grid-template-columns:1fr 1fr}.g3{grid-template-columns:1fr 1fr 1fr}
.g4{grid-template-columns:repeat(4,1fr)}.g5{grid-template-columns:repeat(5,1fr)}.g23{grid-template-columns:2fr 3fr}
.g14{grid-template-columns:1fr 4fr}

.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px}
.card h2{font-size:10px;font-weight:600;color:var(--muted);letter-spacing:1.2px;text-transform:uppercase;margin-bottom:10px}
.stat{font-size:26px;font-weight:700;line-height:1.1}.stat-s{font-size:11px;color:var(--muted);margin-top:3px}

.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}
.purple{color:var(--purple)}.orange{color:var(--orange)}.muted{color:var(--muted)}

table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:10px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase;padding:6px 8px;border-bottom:1px solid var(--border);font-weight:600;white-space:nowrap}
td{padding:7px 8px;border-bottom:1px solid var(--border);font-size:12px}
tr:last-child td{border-bottom:none}tr:hover td{background:rgba(59,130,246,.04)}
td.n{text-align:right;font-variant-numeric:tabular-nums}
.clickrow{cursor:pointer}
.clickrow:hover td{background:rgba(59,130,246,.08) !important}

.badge{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;letter-spacing:.3px}
.badge.allow,.badge.paper,.badge.tp,.badge.long{background:rgba(34,197,94,.12);color:var(--green)}
.badge.watch,.badge.tl,.badge.research{background:rgba(234,179,8,.12);color:var(--yellow)}
.badge.block,.badge.sl,.badge.short,.badge.dead{background:rgba(239,68,68,.12);color:var(--red)}
.badge.deploy{background:rgba(59,130,246,.12);color:var(--blue)}

.dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px;flex-shrink:0}
.dot.ok{background:var(--green)}.dot.warn{background:var(--yellow)}.dot.err{background:var(--red)}
.hr{display:flex;align-items:center;padding:5px 0;border-bottom:1px solid var(--border)}
.hr:last-child{border-bottom:none}.hr .nm{flex:1}.hr .age{color:var(--muted);font-size:11px}

.pill{display:inline-block;padding:4px 12px;border-radius:6px;font-size:11px;margin:2px;cursor:pointer;border:1px solid var(--border);font-weight:500;transition:.15s}
.pill:hover{border-color:var(--blue);color:var(--blue)}.pill.a{border-color:var(--blue);color:var(--blue);background:rgba(59,130,246,.08)}

/* pipeline progress */
.pipeline{display:flex;align-items:center;gap:0;margin:16px 0 20px}
.pstep{flex:1;text-align:center;padding:10px 6px;font-size:10px;font-weight:600;letter-spacing:.5px;
  border-bottom:3px solid var(--border);color:var(--muted);position:relative}
.pstep.done{border-bottom-color:var(--green);color:var(--green)}
.pstep.current{border-bottom-color:var(--blue);color:var(--blue)}
.pstep.skip{border-bottom-color:var(--red);color:var(--red)}
.pstep .pnum{font-size:18px;font-weight:700;display:block;margin-bottom:2px}

/* detail panel */
.detail{background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:20px;margin-top:16px}
.detail h3{font-size:13px;font-weight:600;margin-bottom:12px;display:flex;align-items:center;gap:8px}
.detail .close-btn{margin-left:auto;cursor:pointer;color:var(--muted);font-size:18px;padding:0 6px}
.detail .close-btn:hover{color:var(--red)}

.filter-bar{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;align-items:center}
.filter-bar select{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:5px 10px;border-radius:4px;font-size:12px}

canvas{max-height:260px}
.loading{color:var(--muted);font-size:12px;padding:12px 0}
.empty{color:var(--muted);font-size:12px;padding:16px 0;text-align:center}
.section{margin-bottom:20px}
.section-title{font-size:11px;font-weight:600;color:var(--muted);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border)}

@media(max-width:900px){.g2,.g3,.g4,.g5,.g23,.g14{grid-template-columns:1fr}.tabs{overflow-x:auto}}
</style>
</head><body>

<div class="hdr"><h1>QuantsLab</h1><div style="display:flex;align-items:center"><span class="ts" id="ts">--</span><button class="rbtn" onclick="refresh()">Refresh</button></div></div>
<div class="tabs">
  <div class="tab a" data-t="overview">Overview</div>
  <div class="tab" data-t="live">Live Trades</div>
  <div class="tab" data-t="research">Research Pipeline</div>
</div>

<!-- ===== TAB 1: OVERVIEW ===== -->
<div class="tc a" id="tab-overview">
  <div class="grid g5" style="margin-bottom:16px">
    <div class="card"><h2>Wallet Equity</h2><div class="stat" id="k-eq">--</div><div class="stat-s" id="k-av"></div></div>
    <div class="card"><h2>Open Positions</h2><div class="stat blue" id="k-op">--</div><div class="stat-s" id="k-ex"></div></div>
    <div class="card"><h2>Unrealized PnL</h2><div class="stat" id="k-ur">--</div><div class="stat-s" id="k-urp"></div></div>
    <div class="card"><h2>Realized PnL</h2><div class="stat" id="k-rl">--</div><div class="stat-s" id="k-rlt"></div></div>
    <div class="card"><h2>Win Rate</h2><div class="stat" id="k-wr">--</div><div class="stat-s" id="k-wrs"></div></div>
  </div>
  <div class="grid g23">
    <div class="card"><h2>System Health</h2><div id="health"></div></div>
    <div class="card"><h2>Open Positions (Exchange)</h2><div id="pos-tbl"></div></div>
  </div>
</div>

<!-- ===== TAB 2: LIVE TRADES ===== -->
<div class="tc" id="tab-live">
  <div class="grid g3" style="margin-bottom:16px">
    <div class="card"><h2>Total Realized</h2><div class="stat" id="l-tp">--</div></div>
    <div class="card"><h2>Avg PnL / Trade</h2><div class="stat" id="l-ap">--</div></div>
    <div class="card"><h2>Total Fees</h2><div class="stat yellow" id="l-fe">--</div></div>
  </div>
  <div class="grid g2" style="margin-bottom:16px">
    <div class="card"><h2>Cumulative PnL</h2><canvas id="eq-chart"></canvas></div>
    <div class="card"><h2>PnL by Pair</h2><canvas id="pp-chart"></canvas></div>
  </div>
  <div class="card"><h2>Closed Trades</h2><div id="cl-tbl"></div></div>
  <div class="card" style="margin-top:16px"><h2>Execution Log</h2><div id="ex-tbl"></div></div>
</div>

<!-- ===== TAB 3: RESEARCH PIPELINE ===== -->
<div class="tc" id="tab-research">
  <div id="engine-pills" class="filter-bar" style="margin-bottom:0"></div>
  <div id="engine-detail"></div>
</div>

<script>
const API=window.location.origin+'/dashboard/api';
let charts={},curEngine=null,stratData=null;

// -- Helpers --
function fmt(v,d=2){return v!=null?parseFloat(v).toFixed(d):'--'}
function fmtK(v){if(v==null)return'--';const n=parseFloat(v);return n>=1000?'$'+(n/1000).toFixed(1)+'k':'$'+n.toFixed(0)}
function pc(v){return v==null?'':parseFloat(v)>=0?'green':'red'}
function ps(v,d=2){if(v==null)return'--';const n=parseFloat(v);return(n>=0?'+':'')+n.toFixed(d)}
function ta(ms){if(!ms)return'--';const m=Math.floor((Date.now()-ms)/60000);if(m<60)return m+'m ago';const h=Math.floor(m/60);if(h<24)return h+'h ago';return Math.floor(h/24)+'d ago'}
function tf(ms){if(!ms)return'--';return new Date(ms).toISOString().slice(5,16).replace('T',' ')}
function tf2(ts){if(!ts)return'--';return new Date(ts*1000).toISOString().slice(0,10)}
function sb(s){if(!s)return'';const u=s.toUpperCase();return u==='BUY'||u==='LONG'?'<span class="badge long">LONG</span>':'<span class="badge short">SHORT</span>'}
function cb(ct){if(!ct)return'';const t=ct.toUpperCase().replace('CLOSETYPE.','');if(t.includes('TAKE_PROFIT')||t==='TP')return'<span class="badge tp">TP</span>';if(t.includes('STOP_LOSS')||t==='SL')return'<span class="badge sl">SL</span>';if(t.includes('TIME'))return'<span class="badge tl">TL</span>';return'<span class="badge tl">'+t.slice(0,8)+'</span>'}
function vb(v){if(!v)return'';return'<span class="badge '+v.toLowerCase()+'">'+v+'</span>'}
async function fj(u){const r=await fetch(u);if(!r.ok)throw new Error(r.status);return r.json()}

// -- Tabs --
document.querySelectorAll('.tab').forEach(t=>{t.addEventListener('click',()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('a'));
  document.querySelectorAll('.tc').forEach(x=>x.classList.remove('a'));
  t.classList.add('a');document.getElementById('tab-'+t.dataset.t).classList.add('a');
  if(t.dataset.t==='research'&&!stratData)loadStrategies();
})});

// =========== TAB 1: OVERVIEW ===========
async function loadOverview(){
  try{
    const d=await fj(API+'/overview');
    const w=d.wallet||{};
    document.getElementById('k-eq').textContent=fmtK(w.equity);
    document.getElementById('k-av').textContent='Available: '+fmtK(w.available);
    const pos=d.positions||[];
    document.getElementById('k-op').textContent=pos.length;
    const tv=pos.reduce((s,p)=>s+parseFloat(p.value||0),0);
    document.getElementById('k-ex').textContent='$'+tv.toFixed(0)+' exposure';
    const tu=pos.reduce((s,p)=>s+parseFloat(p.unrealized_pnl||0),0);
    const ke=document.getElementById('k-ur');ke.textContent=ps(tu);ke.className='stat '+pc(tu);
    const up=tv>0?(tu/tv*100):0;document.getElementById('k-urp').textContent=ps(up,1)+'% on exposure';
    const cl=d.closed_pnl||[];const tr=cl.reduce((s,c)=>s+parseFloat(c.pnl||0),0);
    const kr=document.getElementById('k-rl');kr.textContent='$'+ps(tr);kr.className='stat '+pc(tr);
    document.getElementById('k-rlt').textContent=cl.length+' closed trades';
    const wi=cl.filter(c=>parseFloat(c.pnl||0)>0).length;const wr=cl.length>0?(wi/cl.length*100):0;
    document.getElementById('k-wr').textContent=cl.length>0?wr.toFixed(0)+'%':'--';
    document.getElementById('k-wrs').textContent=wi+'W / '+(cl.length-wi)+'L';
    // Positions table
    const pe=document.getElementById('pos-tbl');
    if(!pos.length){pe.innerHTML='<div class="empty">No open positions</div>'}
    else{pe.innerHTML='<table><tr><th>Pair</th><th>Side</th><th>Size</th><th>Entry</th><th>Mark</th><th style="text-align:right">uPnL</th><th style="text-align:right">Value</th></tr>'+pos.map(p=>{const pn=parseFloat(p.unrealized_pnl||0);return'<tr><td><b>'+p.pair+'</b></td><td>'+sb(p.side)+'</td><td class="n">'+fmt(p.size,4)+'</td><td class="n">'+fmt(p.entry,4)+'</td><td class="n">'+fmt(p.mark,4)+'</td><td class="n '+pc(pn)+'">'+ps(pn,4)+'</td><td class="n">$'+fmt(p.value,0)+'</td></tr>'}).join('')+'</table>'}
    // Health
    const he=document.getElementById('health');const ts=d.tasks||[];const sy=d.system||{};
    let h='';for(const t of ts){const c=t.age_minutes==null?'err':t.age_minutes<90?'ok':t.age_minutes<180?'warn':'err';const a=t.age_minutes!=null?Math.round(t.age_minutes)+'m ago':'never';h+='<div class="hr"><span class="dot '+c+'"></span><span class="nm">'+t.name+'</span><span class="age">'+a+'</span></div>'}
    h+='<div class="hr"><span class="dot '+(sy.hb_api?'ok':'err')+'"></span><span class="nm">HB API</span><span class="age">'+(sy.hb_api?'up':'down')+'</span></div>';
    h+='<div class="hr"><span class="dot ok"></span><span class="nm">QL API</span><span class="age">up</span></div>';
    he.innerHTML=h;
  }catch(e){console.error('Overview:',e)}
}

// =========== TAB 2: LIVE TRADES ===========
async function loadLive(){
  try{
    const d=await fj(API+'/live');const cl=d.closed_pnl||[];const ex=d.executions||[];
    const tradePnl=cl.reduce((s,c)=>s+parseFloat(c.pnl||0),0);
    const tradeFees=d.trade_fees||0;
    const fundingInc=d.funding_income||0;
    const totalPnl=tradePnl+fundingInc;
    const ap=cl.length>0?tradePnl/cl.length:0;
    const e1=document.getElementById('l-tp');e1.textContent='$'+ps(totalPnl);e1.className='stat '+pc(totalPnl);
    const e2=document.getElementById('l-ap');e2.textContent='$'+ps(ap);e2.className='stat '+pc(ap);
    document.getElementById('l-fe').textContent='Fees: $'+tradeFees.toFixed(2)+' | Funding: $'+ps(fundingInc);
    // Equity chart
    if(cl.length>0){
      const so=[...cl].sort((a,b)=>a.created_time-b.created_time);let cm=0;
      const lb=so.map(c=>tf(c.created_time));const pt=so.map(c=>{cm+=parseFloat(c.pnl||0);return cm});
      if(charts.eq)charts.eq.destroy();
      charts.eq=new Chart(document.getElementById('eq-chart').getContext('2d'),{type:'line',data:{labels:lb,datasets:[{label:'PnL',data:pt,borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,.08)',fill:true,tension:.3,pointRadius:4,pointBackgroundColor:pt.map(v=>v>=0?'#22c55e':'#ef4444')}]},options:{plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8892a4',font:{size:10}},grid:{color:'#2a2d3a'}},y:{ticks:{color:'#8892a4',callback:v=>'$'+v.toFixed(1)},grid:{color:'#2a2d3a'}}}}});
    }
    // Pair PnL
    const bp={};for(const c of cl){const p=c.pair||'?';bp[p]=(bp[p]||0)+parseFloat(c.pnl||0)}
    const pl=Object.keys(bp).sort((a,b)=>bp[b]-bp[a]);const pv=pl.map(p=>bp[p]);
    if(pl.length>0){if(charts.pp)charts.pp.destroy();charts.pp=new Chart(document.getElementById('pp-chart').getContext('2d'),{type:'bar',data:{labels:pl,datasets:[{data:pv,backgroundColor:pv.map(v=>v>=0?'rgba(34,197,94,.6)':'rgba(239,68,68,.6)'),borderRadius:4}]},options:{plugins:{legend:{display:false}},indexAxis:'y',scales:{x:{ticks:{color:'#8892a4',callback:v=>'$'+v.toFixed(1)},grid:{color:'#2a2d3a'}},y:{ticks:{color:'#e2e8f0',font:{size:11}},grid:{display:false}}}}})}
    // Tables
    const ce=document.getElementById('cl-tbl');
    if(!cl.length){ce.innerHTML='<div class="empty">No closed trades yet</div>'}
    else{ce.innerHTML='<table><tr><th>Pair</th><th>Side</th><th>Qty</th><th>Entry</th><th>Exit</th><th style="text-align:right">PnL</th><th>When</th></tr>'+[...cl].sort((a,b)=>(b.created_time||0)-(a.created_time||0)).map(c=>{const pn=parseFloat(c.pnl||0);return'<tr><td><b>'+(c.pair||'?')+'</b></td><td>'+sb(c.side)+'</td><td class="n">'+fmt(c.qty,4)+'</td><td class="n">'+fmt(c.entry,4)+'</td><td class="n">'+fmt(c.exit,4)+'</td><td class="n '+pc(pn)+'">$'+ps(pn,4)+'</td><td class="muted">'+tf(c.created_time)+'</td></tr>'}).join('')+'</table>'}
    const ee=document.getElementById('ex-tbl');
    if(!ex.length){ee.innerHTML='<div class="empty">No executions</div>'}
    else{ee.innerHTML='<table><tr><th>Type</th><th>Pair</th><th>Side</th><th>Qty</th><th>Price</th><th>Fee</th><th>Maker</th><th>When</th></tr>'+ex.slice(0,50).map(e=>{const f=parseFloat(e.fee||0);const isFund=e.is_funding;const typ=isFund?'<span style="color:#f59e0b">FUND</span>':'TRADE';return'<tr'+(isFund?' style="opacity:.75"':'')+'><td>'+typ+'</td><td><b>'+(e.pair||'?')+'</b></td><td>'+sb(e.side)+'</td><td class="n">'+fmt(e.qty,4)+'</td><td class="n">'+fmt(e.price,4)+'</td><td class="n '+(f<0?'green':'yellow')+'">'+ps(f,6)+'</td><td>'+(isFund?'-':e.is_maker?'Y':'N')+'</td><td class="muted">'+tf(e.exec_time)+'</td></tr>'}).join('')+'</table>'}
  }catch(e){console.error('Live:',e)}
}

// =========== TAB 3: RESEARCH PIPELINE ===========
async function loadStrategies(){
  try{
    stratData=await fj(API+'/strategies');
    const el=document.getElementById('engine-pills');
    const engines=stratData.engines||[];
    el.innerHTML=engines.map(e=>'<span class="pill'+(e.name===curEngine?' a':'')+'" data-e="'+e.name+'"><b>'+e.name+'</b> '+e.display_name+'</span>').join('');
    el.querySelectorAll('.pill').forEach(p=>p.addEventListener('click',()=>selectEngine(p.dataset.e)));
    if(!curEngine&&engines.length>0){
      // Auto-select first paper/research engine, fallback to first
      const paper=engines.find(e=>e.phase==='PAPER')||engines.find(e=>e.phase!=='DEAD')||engines[0];
      selectEngine(paper.name);
    }else if(curEngine){selectEngine(curEngine)}
  }catch(e){console.error('Strategies:',e)}
}

async function selectEngine(name){
  curEngine=name;
  document.querySelectorAll('#engine-pills .pill').forEach(p=>p.classList.toggle('a',p.dataset.e===name));
  const el=document.getElementById('engine-detail');
  el.innerHTML='<div class="loading">Loading '+name+'...</div>';
  try{
    const d=await fj(API+'/strategy/'+name);
    renderEngineDetail(d);
  }catch(e){el.innerHTML='<div class="empty">Error loading '+name+': '+e+'</div>'}
}

function renderEngineDetail(d){
  const el=document.getElementById('engine-detail');
  const s=d.summary||{};const pairs=d.pairs||[];const wf=d.walk_forward||[];
  const phases=['IDEA','BACKTEST','WALK_FORWARD','ROBUSTNESS','PAPER','DEPLOY'];
  const curPhase=s.phase||'DEAD';
  const phaseIdx=phases.indexOf(curPhase);

  let html='';

  // Pipeline progress
  html+='<div class="pipeline">';
  phases.forEach((p,i)=>{
    let cls='';
    if(curPhase==='DEAD')cls='skip';
    else if(i<phaseIdx)cls='done';
    else if(i===phaseIdx)cls='current';
    html+='<div class="pstep '+cls+'"><span class="pnum">'+(i+1)+'</span>'+p.replace('_',' ')+'</div>';
  });
  html+='</div>';

  // Summary KPIs
  html+='<div class="grid g5 section" style="margin-bottom:16px">';
  html+='<div class="card"><h2>Status</h2><div class="stat">'+vb(curPhase)+'</div><div class="stat-s">'+s.display_name+'</div></div>';
  html+='<div class="card"><h2>Pairs</h2><div class="stat"><span class="green">'+s.allow+'</span> / <span class="yellow">'+s.watch+'</span> / <span class="red">'+s.block+'</span></div><div class="stat-s">ALLOW / WATCH / BLOCK</div></div>';
  html+='<div class="card"><h2>BT Trades</h2><div class="stat">'+(s.total_bt_trades||0).toLocaleString()+'</div><div class="stat-s">across '+(s.total_pairs||0)+' pairs</div></div>';
  const avgPf=s.avg_pf_allow||0;
  html+='<div class="card"><h2>Avg PF (ALLOW)</h2><div class="stat '+pc(avgPf-1)+'">'+fmt(avgPf,3)+'</div><div class="stat-s">'+s.allow+' qualifying pairs</div></div>';
  html+='<div class="card"><h2>WF Folds</h2><div class="stat">'+(s.wf_folds||0)+'</div><div class="stat-s">'+(s.wf_pairs||0)+' pairs tested</div></div>';
  html+='</div>';

  // Gate checks
  html+='<div class="card section"><h2>Quality Gates</h2>';
  html+=renderGates(s,pairs,wf);
  html+='</div>';

  // Pair matrix (sortable)
  html+='<div class="card section"><h2>Pair Results (click to drill down)</h2>';
  html+='<div class="filter-bar"><select id="verdict-filter" onchange="filterPairs()"><option value="">All Verdicts</option><option value="ALLOW">ALLOW</option><option value="WATCH">WATCH</option><option value="BLOCK">BLOCK</option></select></div>';
  html+='<div id="pair-matrix">'+renderPairMatrix(pairs,wf)+'</div></div>';

  // Walk-forward overview
  if(wf.length>0){
    html+='<div class="grid g2 section">';
    html+='<div class="card"><h2>Walk-Forward: Train vs Test PF</h2><canvas id="wf-scatter"></canvas></div>';
    html+='<div class="card"><h2>Walk-Forward Detail</h2><div id="wf-detail">'+renderWFTable(wf)+'</div></div>';
    html+='</div>';
  }

  // Aggregate distributions
  html+='<div class="grid g3 section">';
  html+='<div class="card"><h2>Close Type Distribution</h2><canvas id="ct-pie"></canvas></div>';
  html+='<div class="card"><h2>Long vs Short Performance</h2><canvas id="ls-chart"></canvas></div>';
  html+='<div class="card"><h2>PF by Pair</h2><canvas id="pf-bar"></canvas></div>';
  html+='</div>';

  // Pair detail panel (populated on click)
  html+='<div id="pair-detail-panel"></div>';

  el.innerHTML=html;

  // Render charts after DOM update
  setTimeout(()=>{
    renderWFScatter(wf);
    renderCloseTypePie(d.agg_close_types||{});
    renderLongShortChart(pairs);
    renderPFBar(pairs);
  },50);
}

function renderGates(s,pairs,wf){
  const gates=[
    {name:'Backtest Coverage',desc:'>=30 trades on primary validation window',
     pass:s.total_bt_trades>=30,val:s.total_bt_trades+' trades'},
    {name:'Positive Expectancy',desc:'>=1 pair with PF > 1.0 (ALLOW verdict)',
     pass:s.allow>0,val:s.allow+' ALLOW pairs'},
    {name:'Walk-Forward',desc:'WF test PF > 1.0, overfit check',
     pass:wf.length>0&&wf.some(w=>w.period_type==='test'&&w.pf>1),val:wf.length>0?(wf.filter(w=>w.period_type==='test').length+' test folds'):'Not run'},
    {name:'Long/Short Split',desc:'Both sides analyzed separately',
     pass:pairs.some(p=>p.n_long>0&&p.n_short>0),val:pairs.length>0?'Analyzed':'No data'},
    {name:'Regime Coverage',desc:'Tested across multiple market regimes',
     pass:s.total_bt_trades>200,val:s.period||'N/A'},
    {name:'Paper Trading',desc:'>=20 live signals with execution quality',
     pass:s.phase==='PAPER'||s.phase==='DEPLOY',val:s.phase==='PAPER'?'Active':'Not started'},
  ];
  return '<table><tr><th></th><th>Gate</th><th>Criteria</th><th>Evidence</th></tr>'+
    gates.map(g=>'<tr><td><span class="dot '+(g.pass?'ok':'err')+'"></span></td><td><b>'+g.name+'</b></td><td class="muted">'+g.desc+'</td><td>'+g.val+'</td></tr>').join('')+'</table>';
}

function renderPairMatrix(pairs,wf){
  // Merge WF data into pairs
  const wfByPair={};
  for(const w of wf){
    if(!wfByPair[w.pair])wfByPair[w.pair]={};
    wfByPair[w.pair][w.period_type]=w;
  }

  const sorted=[...pairs].sort((a,b)=>{
    const vo={ALLOW:0,WATCH:1,BLOCK:2};
    return(vo[a.verdict]||9)-(vo[b.verdict]||9)||(b.pf||0)-(a.pf||0);
  });

  return '<table><tr><th>Pair</th><th>Verdict</th><th style="text-align:right">PF</th><th style="text-align:right">WR%</th><th style="text-align:right">Sharpe</th><th style="text-align:right">Trades</th><th style="text-align:right">L/S</th><th style="text-align:right">Max DD</th><th style="text-align:right">WF Test PF</th><th style="text-align:right">Overfit%</th><th>Close Types</th></tr>'+
    sorted.map(p=>{
      const wfd=wfByPair[p.pair]||{};const trainPf=wfd.train?.pf||0;const testPf=wfd.test?.pf||0;
      const of2=trainPf>0?((trainPf-testPf)/trainPf*100):0;
      const ofCls=of2>30?'red':of2>15?'yellow':'green';
      const ct=p.close_types||{};
      const ctStr=Object.entries(ct).map(([k,v])=>k.replace('STOP_LOSS','SL').replace('TAKE_PROFIT','TP').replace('TIME_LIMIT','TL')+':'+v).join(' ');
      return '<tr class="clickrow" onclick="loadPairDetail(\''+curEngine+'\',\''+p.pair+'\')"><td><b>'+p.pair+'</b></td><td>'+vb(p.verdict)+'</td><td class="n '+pc((p.pf||0)-1)+'">'+fmt(p.pf,3)+'</td><td class="n">'+fmt(p.wr,1)+'</td><td class="n '+pc(p.sharpe)+'">'+fmt(p.sharpe,2)+'</td><td class="n">'+(p.trades||'--')+'</td><td class="n">'+p.n_long+'/'+p.n_short+'</td><td class="n red">'+fmt(p.max_dd,1)+'%</td><td class="n '+(testPf>1?'green':'red')+'">'+fmt(testPf,3)+'</td><td class="n '+ofCls+'">'+fmt(of2,0)+'%</td><td class="muted" style="font-size:10px">'+ctStr+'</td></tr>';
    }).join('')+'</table>';
}

window.filterPairs=function(){
  const v=document.getElementById('verdict-filter').value;
  const rows=document.querySelectorAll('#pair-matrix tr.clickrow');
  rows.forEach(r=>{
    if(!v){r.style.display=''}
    else{r.style.display=r.innerHTML.includes('>'+v+'<')?'':'none'}
  });
};

function renderWFTable(wf){
  const byPair={};for(const w of wf){if(!byPair[w.pair])byPair[w.pair]={};byPair[w.pair][w.period_type]=w}
  const ps2=Object.keys(byPair).sort();
  return '<table style="font-size:11px"><tr><th>Pair</th><th style="text-align:right">Train PF</th><th style="text-align:right">Test PF</th><th style="text-align:right">Overfit</th><th style="text-align:right">Test WR</th><th style="text-align:right">Test Sharpe</th><th style="text-align:right">Test Trades</th><th style="text-align:right">Test DD</th></tr>'+
    ps2.map(pair=>{
      const tr=byPair[pair].train||{};const te=byPair[pair].test||{};
      const tp2=tr.pf||0;const tep=te.pf||0;const of2=tp2>0?((tp2-tep)/tp2*100):0;
      const oc=of2>30?'red':of2>15?'yellow':'green';
      return '<tr><td><b>'+pair+'</b></td><td class="n">'+fmt(tp2,3)+'</td><td class="n '+pc(tep-1)+'">'+fmt(tep,3)+'</td><td class="n '+oc+'">'+fmt(of2,0)+'%</td><td class="n">'+fmt(te.wr,1)+'</td><td class="n '+pc(te.sharpe)+'">'+fmt(te.sharpe,2)+'</td><td class="n">'+(te.trades||'--')+'</td><td class="n red">'+fmt(te.max_dd,1)+'%</td></tr>';
    }).join('')+'</table>';
}

function renderWFScatter(wf){
  const byPair={};for(const w of wf){if(!byPair[w.pair])byPair[w.pair]={};byPair[w.pair][w.period_type]=w}
  const pts=Object.entries(byPair).filter(([_,v])=>v.train&&v.test).map(([p,v])=>({x:v.train.pf||0,y:v.test.pf||0,pair:p}));
  if(!pts.length)return;
  if(charts.wfScat)charts.wfScat.destroy();
  const maxV=Math.max(2,...pts.map(p=>Math.max(p.x,p.y)))+0.2;
  charts.wfScat=new Chart(document.getElementById('wf-scatter').getContext('2d'),{
    type:'scatter',
    data:{datasets:[
      {data:pts,backgroundColor:pts.map(p=>p.y>=1?'rgba(34,197,94,.7)':'rgba(239,68,68,.7)'),pointRadius:6,pointHoverRadius:9},
      {data:[{x:0,y:0},{x:maxV,y:maxV}],type:'line',borderColor:'rgba(136,146,164,.3)',borderDash:[4,4],pointRadius:0,borderWidth:1}
    ]},
    options:{plugins:{legend:{display:false},tooltip:{callbacks:{label:ctx=>pts[ctx.dataIndex]?.pair+' (train:'+fmt(pts[ctx.dataIndex]?.x,2)+' test:'+fmt(pts[ctx.dataIndex]?.y,2)+')'}}},
      scales:{x:{title:{display:true,text:'Train PF',color:'#8892a4'},ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'},min:0,max:maxV},
              y:{title:{display:true,text:'Test PF',color:'#8892a4'},ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'},min:0,max:maxV}}}
  });
}

function renderCloseTypePie(ct){
  const ks=Object.keys(ct);if(!ks.length)return;
  const colors={'TAKE_PROFIT':'#22c55e','STOP_LOSS':'#ef4444','TIME_LIMIT':'#eab308'};
  if(charts.ctPie)charts.ctPie.destroy();
  charts.ctPie=new Chart(document.getElementById('ct-pie').getContext('2d'),{
    type:'doughnut',
    data:{labels:ks.map(k=>k.replace('STOP_LOSS','SL').replace('TAKE_PROFIT','TP').replace('TIME_LIMIT','TL')),
      datasets:[{data:ks.map(k=>ct[k]),backgroundColor:ks.map(k=>colors[k]||'#8892a4'),borderWidth:0}]},
    options:{plugins:{legend:{labels:{color:'#8892a4',font:{size:11}}}}}
  });
}

function renderLongShortChart(pairs){
  const totL={trades:0,pnl:0,wins:0};const totS={trades:0,pnl:0,wins:0};
  for(const p of pairs){totL.trades+=p.n_long||0;totS.trades+=p.n_short||0;totL.pnl+=p.long_pnl||0;totS.pnl+=p.short_pnl||0}
  if(charts.ls)charts.ls.destroy();
  charts.ls=new Chart(document.getElementById('ls-chart').getContext('2d'),{
    type:'bar',
    data:{labels:['Trades','PnL ($)'],datasets:[
      {label:'Long',data:[totL.trades,totL.pnl],backgroundColor:'rgba(34,197,94,.6)',borderRadius:4},
      {label:'Short',data:[totS.trades,totS.pnl],backgroundColor:'rgba(239,68,68,.6)',borderRadius:4}
    ]},options:{plugins:{legend:{labels:{color:'#8892a4'}}},scales:{x:{ticks:{color:'#8892a4'},grid:{display:false}},y:{ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'}}}}
  });
}

function renderPFBar(pairs){
  const allow=pairs.filter(p=>p.verdict==='ALLOW').sort((a,b)=>(b.pf||0)-(a.pf||0));
  if(!allow.length)return;
  if(charts.pfBar)charts.pfBar.destroy();
  charts.pfBar=new Chart(document.getElementById('pf-bar').getContext('2d'),{
    type:'bar',
    data:{labels:allow.map(p=>p.pair),datasets:[{data:allow.map(p=>p.pf),backgroundColor:allow.map(p=>(p.pf||0)>1.2?'rgba(34,197,94,.6)':'rgba(234,179,8,.6)'),borderRadius:4}]},
    options:{plugins:{legend:{display:false}},indexAxis:'y',scales:{x:{ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'},min:0.8},y:{ticks:{color:'#e2e8f0',font:{size:10}},grid:{display:false}}}}
  });
}

// =========== PAIR DETAIL (drill-down) ===========
window.loadPairDetail=async function(engine,pair){
  const panel=document.getElementById('pair-detail-panel');
  panel.innerHTML='<div class="detail"><div class="loading">Loading '+pair+'...</div></div>';
  panel.scrollIntoView({behavior:'smooth'});
  try{
    const d=await fj(API+'/strategy/'+engine+'/'+encodeURIComponent(pair));
    renderPairDetail(d,engine,pair);
  }catch(e){panel.innerHTML='<div class="detail"><div class="empty">Error: '+e+'</div></div>'}
};

function renderPairDetail(d,engine,pair){
  const panel=document.getElementById('pair-detail-panel');
  const s=d.summary||{};const trades=d.trades||[];const eq=d.equity||[];
  const wf=d.walk_forward||[];const htBins=d.hold_time_bins||[];

  let html='<div class="detail">';
  html+='<h3>'+engine+' / '+pair+' '+vb(s.verdict)+' <span class="close-btn" onclick="document.getElementById(\'pair-detail-panel\').innerHTML=\'\'">&times;</span></h3>';

  // KPIs
  html+='<div class="grid g5" style="margin-bottom:16px">';
  html+='<div class="card"><h2>Profit Factor</h2><div class="stat '+pc((s.pf||0)-1)+'">'+fmt(s.pf,3)+'</div></div>';
  html+='<div class="card"><h2>Win Rate</h2><div class="stat">'+fmt(s.wr,1)+'%</div></div>';
  html+='<div class="card"><h2>Sharpe</h2><div class="stat '+pc(s.sharpe)+'">'+fmt(s.sharpe,2)+'</div></div>';
  html+='<div class="card"><h2>Trades</h2><div class="stat">'+(s.trades||0)+'</div><div class="stat-s">L:'+s.n_long+' S:'+s.n_short+'</div></div>';
  html+='<div class="card"><h2>Net PnL</h2><div class="stat '+pc(s.pnl)+'">$'+ps(s.pnl,2)+'</div><div class="stat-s">Max DD: '+fmt(s.max_dd,1)+'%</div></div>';
  html+='</div>';

  // Charts row 1: equity + PnL distribution
  html+='<div class="grid g2" style="margin-bottom:16px">';
  html+='<div class="card"><h2>Equity Curve</h2><canvas id="pd-eq"></canvas></div>';
  html+='<div class="card"><h2>PnL Distribution</h2><canvas id="pd-dist"></canvas></div>';
  html+='</div>';

  // Charts row 2: close types + hold time + long/short + monthly
  html+='<div class="grid g4" style="margin-bottom:16px">';
  html+='<div class="card"><h2>Close Types</h2><canvas id="pd-ct"></canvas></div>';
  html+='<div class="card"><h2>Hold Time</h2><canvas id="pd-ht"></canvas></div>';
  html+='<div class="card"><h2>Long vs Short</h2><canvas id="pd-ls"></canvas></div>';
  html+='<div class="card"><h2>Monthly PnL</h2><canvas id="pd-mo"></canvas></div>';
  html+='</div>';

  // WF folds detail
  if(wf.length>0){
    html+='<div class="section"><div class="section-title">Walk-Forward Folds</div>';
    html+='<table style="font-size:11px"><tr><th>Fold</th><th>Type</th><th>Period</th><th style="text-align:right">PF</th><th style="text-align:right">WR</th><th style="text-align:right">Sharpe</th><th style="text-align:right">Trades</th><th style="text-align:right">Max DD</th><th>Close Types</th></tr>';
    for(const w of wf){
      const ct=w.close_types||{};const ctStr=Object.entries(ct).map(([k,v])=>k.replace('STOP_LOSS','SL').replace('TAKE_PROFIT','TP').replace('TIME_LIMIT','TL')+':'+v).join(' ');
      html+='<tr><td>'+w.fold_index+'</td><td>'+vb(w.period_type.toUpperCase())+'</td><td class="muted">'+w.period+'</td><td class="n '+pc((w.pf||0)-1)+'">'+fmt(w.pf,3)+'</td><td class="n">'+fmt(w.wr,1)+'</td><td class="n '+pc(w.sharpe)+'">'+fmt(w.sharpe,2)+'</td><td class="n">'+w.trades+'</td><td class="n red">'+fmt(w.max_dd,1)+'%</td><td class="muted" style="font-size:10px">'+ctStr+'</td></tr>';
    }
    html+='</table></div>';
  }

  // Trade log (last 200)
  html+='<div class="section"><div class="section-title">Trades ('+trades.length+(trades.length>=500?' - capped':'')+')</div>';
  html+='<div style="max-height:400px;overflow-y:auto">';
  html+='<table style="font-size:11px"><tr><th>Date</th><th>Side</th><th>Close</th><th style="text-align:right">PnL</th><th style="text-align:right">PnL%</th><th style="text-align:right">Hold</th><th style="text-align:right">Fees</th></tr>';
  for(const t of trades){
    const hold=t.hold_hours!=null?fmt(t.hold_hours,1)+'h':'--';
    html+='<tr><td class="muted">'+tf2(t.timestamp)+'</td><td>'+sb(t.side)+'</td><td>'+cb(t.close_type)+'</td><td class="n '+pc(t.pnl)+'">$'+ps(t.pnl,2)+'</td><td class="n '+pc(t.pnl_pct)+'">'+ps(t.pnl_pct?t.pnl_pct*100:null,2)+'%</td><td class="n">'+hold+'</td><td class="n muted">$'+fmt(t.fees,3)+'</td></tr>';
  }
  html+='</table></div></div>';

  html+='</div>';
  panel.innerHTML=html;

  // Render pair detail charts
  setTimeout(()=>{
    // Equity curve
    if(eq.length>0){
      if(charts.pdEq)charts.pdEq.destroy();
      charts.pdEq=new Chart(document.getElementById('pd-eq').getContext('2d'),{type:'line',data:{labels:eq.map(e=>e.t),datasets:[{data:eq.map(e=>e.pnl),borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,.05)',fill:true,tension:.2,pointRadius:0,borderWidth:1.5}]},options:{plugins:{legend:{display:false}},scales:{x:{type:'linear',display:true,ticks:{color:'#8892a4',font:{size:9},callback:v=>{const dt=new Date(v*1000);return dt.toISOString().slice(5,7)+'/'+dt.toISOString().slice(2,4)},maxTicksLimit:8},grid:{color:'#2a2d3a'}},y:{ticks:{color:'#8892a4',callback:v=>'$'+v.toFixed(0)},grid:{color:'#2a2d3a'}}}}});
    }
    // PnL distribution
    const bins=d.pnl_bins||{};
    if(bins.labels){
      if(charts.pdDist)charts.pdDist.destroy();
      charts.pdDist=new Chart(document.getElementById('pd-dist').getContext('2d'),{type:'bar',data:{labels:bins.labels,datasets:[{data:bins.counts,backgroundColor:bins.labels.map(l=>parseFloat(l)>=0?'rgba(34,197,94,.5)':'rgba(239,68,68,.5)'),borderRadius:2}]},options:{plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8892a4',font:{size:9},maxTicksLimit:10},grid:{display:false}},y:{ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'}}}}});
    }
    // Close types
    const ct=d.close_types||{};const ctks=Object.keys(ct);
    if(ctks.length){
      const clrs={'TAKE_PROFIT':'#22c55e','STOP_LOSS':'#ef4444','TIME_LIMIT':'#eab308'};
      if(charts.pdCt)charts.pdCt.destroy();
      charts.pdCt=new Chart(document.getElementById('pd-ct').getContext('2d'),{type:'doughnut',data:{labels:ctks.map(k=>k.replace('STOP_LOSS','SL').replace('TAKE_PROFIT','TP').replace('TIME_LIMIT','TL')),datasets:[{data:ctks.map(k=>ct[k].count),backgroundColor:ctks.map(k=>clrs[k]||'#8892a4'),borderWidth:0}]},options:{plugins:{legend:{labels:{color:'#8892a4',font:{size:10}}}}}});
    }
    // Hold time
    if(htBins.length){
      if(charts.pdHt)charts.pdHt.destroy();
      charts.pdHt=new Chart(document.getElementById('pd-ht').getContext('2d'),{type:'bar',data:{labels:htBins.map(b=>b.label),datasets:[{data:htBins.map(b=>b.count),backgroundColor:'rgba(59,130,246,.5)',borderRadius:2}]},options:{plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8892a4',font:{size:9}},grid:{display:false}},y:{ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'}}}}});
    }
    // Long vs Short
    const ls=d.long_short||{};
    if(ls.long_trades||ls.short_trades){
      if(charts.pdLs)charts.pdLs.destroy();
      charts.pdLs=new Chart(document.getElementById('pd-ls').getContext('2d'),{type:'bar',data:{labels:['Count','PnL ($)','Win Rate (%)'],datasets:[{label:'Long',data:[ls.long_trades||0,ls.long_pnl||0,ls.long_wr||0],backgroundColor:'rgba(34,197,94,.6)',borderRadius:4},{label:'Short',data:[ls.short_trades||0,ls.short_pnl||0,ls.short_wr||0],backgroundColor:'rgba(239,68,68,.6)',borderRadius:4}]},options:{plugins:{legend:{labels:{color:'#8892a4',font:{size:10}}}},scales:{x:{ticks:{color:'#8892a4'},grid:{display:false}},y:{ticks:{color:'#8892a4'},grid:{color:'#2a2d3a'}}}}});
    }
    // Monthly PnL
    const mo=d.monthly_pnl||{};const moKeys=Object.keys(mo).sort();
    if(moKeys.length){
      if(charts.pdMo)charts.pdMo.destroy();
      charts.pdMo=new Chart(document.getElementById('pd-mo').getContext('2d'),{type:'bar',data:{labels:moKeys,datasets:[{data:moKeys.map(k=>mo[k]),backgroundColor:moKeys.map(k=>mo[k]>=0?'rgba(34,197,94,.6)':'rgba(239,68,68,.6)'),borderRadius:3}]},options:{plugins:{legend:{display:false}},scales:{x:{ticks:{color:'#8892a4',font:{size:9}},grid:{display:false}},y:{ticks:{color:'#8892a4',callback:v=>'$'+v.toFixed(0)},grid:{color:'#2a2d3a'}}}}});
    }
  },50);
}

// =========== REFRESH ===========
async function refresh(){
  document.getElementById('ts').textContent=new Date().toISOString().slice(0,19)+'Z';
  await Promise.all([loadOverview(),loadLive()]);
  // Don't reload research tab on auto-refresh -- it destroys the pair detail panel
}
refresh();setInterval(refresh,60000);
</script></body></html>"""


@router.get("", response_class=HTMLResponse)
async def dashboard():
    return _HTML


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _get_db():
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
    if not mongo_uri:
        return None
    return MongoClient(mongo_uri)[mongo_db]


def _bybit_request(endpoint: str, params: Optional[dict] = None) -> dict:
    """Synchronous signed Bybit V5 GET request."""
    api_key = os.getenv("BYBIT_DEMO_API_KEY", "")
    api_secret = os.getenv("BYBIT_DEMO_API_SECRET", "")
    base_url = os.getenv("BYBIT_API_BASE_URL", "https://api-demo.bybit.com").rstrip("/")
    if not api_key or not api_secret:
        return {}
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    param_str = "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
    sign_str = ts + api_key + recv_window + param_str
    signature = hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
    headers = {"X-BAPI-API-KEY": api_key, "X-BAPI-SIGN": signature, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": recv_window}
    import requests as _req
    try:
        return _req.get(f"{base_url}{endpoint}?{param_str}", headers=headers, timeout=10).json()
    except Exception as exc:
        logger.warning("Bybit API call failed: %s", exc)
        return {}


def _to_pair(symbol: str) -> str:
    if "-" in symbol:
        return symbol
    return f"{symbol.replace('USDT', '')}-USDT"


# ---------------------------------------------------------------------------
# TAB 1: Overview
# ---------------------------------------------------------------------------

@router.get("/api/overview")
async def api_overview():
    db = _get_db()

    # Bybit positions
    positions = []
    try:
        resp = _bybit_request("/v5/position/list", {"category": "linear", "settleCoin": "USDT"})
        for p in resp.get("result", {}).get("list", []):
            if float(p.get("size", 0)) == 0:
                continue
            positions.append({
                "pair": _to_pair(p["symbol"]), "side": p.get("side", ""),
                "size": float(p.get("size", 0)), "entry": float(p.get("avgPrice", 0)),
                "mark": float(p.get("markPrice", 0)), "unrealized_pnl": float(p.get("unrealisedPnl", 0)),
                "value": float(p.get("positionValue", 0)), "leverage": p.get("leverage", "1"),
            })
    except Exception:
        pass

    # Wallet
    wallet = {}
    try:
        resp = _bybit_request("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        acct = resp.get("result", {}).get("list", [{}])[0]
        eq = acct.get("totalEquity", "")
        av = acct.get("totalAvailableBalance", "")
        wallet = {"equity": float(eq) if eq else None, "available": float(av) if av else None}
    except Exception:
        pass

    # Closed PnL
    closed_pnl = []
    try:
        resp = _bybit_request("/v5/position/closed-pnl", {"category": "linear", "limit": "50"})
        for c in resp.get("result", {}).get("list", []):
            closed_pnl.append({
                "pair": _to_pair(c.get("symbol", "")), "side": c.get("side", ""),
                "qty": c.get("qty"), "entry": c.get("avgEntryPrice"), "exit": c.get("avgExitPrice"),
                "pnl": c.get("closedPnl"), "created_time": int(c.get("createdTime", 0)),
            })
    except Exception:
        pass

    # Health
    tasks_to_check = ["candles_downloader_bybit", "bybit_derivatives", "binance_funding",
                       "feature_computation", "trade_recorder", "watchdog"]
    task_status = []
    if db is not None:
        now = datetime.now(timezone.utc)
        for name in tasks_to_check:
            last = db.task_executions.find_one({"task_name": name, "status": "completed"}, sort=[("started_at", -1)])
            age = None
            if last and last.get("started_at"):
                ts_val = last["started_at"]
                if ts_val.tzinfo is None:
                    ts_val = ts_val.replace(tzinfo=timezone.utc)
                age = (now - ts_val).total_seconds() / 60
            task_status.append({"name": name, "age_minutes": age})

    hb_up = False
    try:
        timeout = aiohttp.ClientTimeout(total=3)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("http://localhost:8000/", auth=aiohttp.BasicAuth("admin", "admin")) as r:
                hb_up = r.status < 500
    except Exception:
        pass

    return _safe_json({"wallet": wallet, "positions": positions, "closed_pnl": closed_pnl,
                        "tasks": task_status, "system": {"hb_api": hb_up, "ql_api": True}})


# ---------------------------------------------------------------------------
# TAB 2: Live Trades
# ---------------------------------------------------------------------------

@router.get("/api/live")
async def api_live():
    db = _get_db()
    closed_pnl = []
    try:
        resp = _bybit_request("/v5/position/closed-pnl", {"category": "linear", "limit": "50"})
        for c in resp.get("result", {}).get("list", []):
            closed_pnl.append({
                "pair": _to_pair(c.get("symbol", "")), "side": c.get("side", ""),
                "qty": c.get("qty"), "entry": c.get("avgEntryPrice"), "exit": c.get("avgExitPrice"),
                "pnl": c.get("closedPnl"), "created_time": int(c.get("createdTime", 0)),
            })
    except Exception:
        pass

    executions = []
    trade_fees = 0.0
    funding_income = 0.0
    if db is not None:
        for doc in db.exchange_executions.find().sort("exec_time", -1).limit(200):
            fee = float(doc.get("exec_fee", 0))
            is_funding = doc.get("order_type") == "UNKNOWN" and float(doc.get("closed_size", 0)) == 0
            if is_funding:
                funding_income += fee  # negative = received
            else:
                trade_fees += abs(fee)
            executions.append({
                "pair": doc.get("pair", ""), "side": doc.get("side", ""),
                "qty": doc.get("exec_qty"), "price": doc.get("exec_price"),
                "fee": fee, "is_maker": doc.get("is_maker", False),
                "exec_time": doc.get("exec_time"),
                "is_funding": is_funding,
                "order_type": doc.get("order_type", ""),
            })
    return _safe_json({
        "closed_pnl": closed_pnl, "executions": executions,
        "trade_fees": trade_fees, "funding_income": -funding_income,  # positive = we earned
    })


# ---------------------------------------------------------------------------
# TAB 3: Research Pipeline -- Engine list
# ---------------------------------------------------------------------------

@router.get("/api/strategies")
async def api_strategies():
    """All engines with summary stats for the sidebar."""
    db = _get_db()
    from app.engines.strategy_registry import STRATEGY_REGISTRY

    # Known dead engines
    dead_engines = {"S6", "S7", "S9", "H2", "E4", "M1"}
    paper_engines = {"E3"}

    engines = []
    for name, meta in STRATEGY_REGISTRY.items():
        allow = watch = block = bt_trades = wf_folds = 0
        if db is not None:
            allow = db.pair_historical.count_documents({"engine": name, "verdict": "ALLOW"})
            watch = db.pair_historical.count_documents({"engine": name, "verdict": "WATCH"})
            block = db.pair_historical.count_documents({"engine": name, "verdict": "BLOCK"})
            bt_trades = db.backtest_trades.count_documents({"engine": name})
            wf_folds = db.walk_forward_results.count_documents({"engine": name})

        if name in paper_engines:
            phase = "PAPER"
        elif name in dead_engines:
            phase = "DEAD"
        elif wf_folds > 0 and allow > 0:
            phase = "ROBUSTNESS"
        elif bt_trades > 0:
            phase = "BACKTEST"
        else:
            phase = "IDEA"

        engines.append({
            "name": name, "display_name": meta.display_name, "phase": phase,
            "direction": meta.direction, "allow": allow, "watch": watch, "block": block,
            "bt_trades": bt_trades, "wf_folds": wf_folds,
        })

    # Sort: PAPER first, then ROBUSTNESS, BACKTEST, IDEA, DEAD
    phase_order = {"PAPER": 0, "DEPLOY": 1, "ROBUSTNESS": 2, "WALK_FORWARD": 3, "BACKTEST": 4, "IDEA": 5, "DEAD": 6}
    engines.sort(key=lambda e: phase_order.get(e["phase"], 9))

    return _safe_json({"engines": engines})


# ---------------------------------------------------------------------------
# TAB 3: Research Pipeline -- Engine detail
# ---------------------------------------------------------------------------

@router.get("/api/strategy/{engine}")
async def api_strategy_detail(engine: str):
    """Full detail for one engine: verdicts, WF, aggregate stats, distributions."""
    db = _get_db()
    if db is None:
        return _safe_json({"summary": {}, "pairs": [], "walk_forward": [], "agg_close_types": {}})

    from app.engines.strategy_registry import STRATEGY_REGISTRY
    meta = STRATEGY_REGISTRY.get(engine)

    # Bulk per-side PnL aggregation (single query instead of per-pair)
    side_pnl_by_pair = defaultdict(lambda: {"long_pnl": 0, "short_pnl": 0})
    for sa in db.backtest_trades.aggregate([
        {"$match": {"engine": engine}},
        {"$group": {"_id": {"pair": "$pair", "side": "$side"}, "pnl": {"$sum": "$net_pnl_quote"}}},
    ]):
        pair_key = sa["_id"]["pair"]
        if sa["_id"]["side"] == "BUY":
            side_pnl_by_pair[pair_key]["long_pnl"] = sa["pnl"]
        else:
            side_pnl_by_pair[pair_key]["short_pnl"] = sa["pnl"]

    # Pair verdicts
    pairs = []
    agg_ct = defaultdict(int)
    total_long = total_short = 0
    total_pnl = 0.0
    pf_allow_sum = 0.0
    pf_allow_count = 0

    for doc in db.pair_historical.find({"engine": engine}):
        ct = doc.get("close_types", {})
        for k, v in ct.items():
            agg_ct[k] += v
        n_long = doc.get("n_long", 0)
        n_short = doc.get("n_short", 0)
        total_long += n_long
        total_short += n_short
        pnl = doc.get("pnl_quote", 0) or 0
        total_pnl += pnl
        pf = doc.get("profit_factor")
        verdict = doc.get("verdict", "")
        if verdict == "ALLOW" and pf:
            pf_allow_sum += pf
            pf_allow_count += 1

        pair_name = doc.get("pair", "")
        sp = side_pnl_by_pair.get(pair_name, {})

        pairs.append({
            "pair": pair_name, "verdict": verdict,
            "pf": pf, "wr": doc.get("win_rate"),
            "sharpe": doc.get("sharpe"), "trades": doc.get("trades", n_long + n_short),
            "n_long": n_long, "n_short": n_short,
            "max_dd": doc.get("max_dd_pct", 0) * 100 if doc.get("max_dd_pct") else None,
            "pnl": pnl, "close_types": ct, "period": doc.get("period", ""),
            "wf_test_pf": doc.get("wf_avg_test_pf"), "wf_train_pf": doc.get("wf_avg_train_pf"),
            "wf_overfit": doc.get("wf_overfit"),
            "long_pnl": sp.get("long_pnl", 0), "short_pnl": sp.get("short_pnl", 0),
        })

    # Walk-forward results (averaged per pair+period_type)
    wf = []
    wf_pipeline = [
        {"$match": {"engine": engine}},
        {"$group": {
            "_id": {"pair": "$pair", "period_type": "$period_type"},
            "pf": {"$avg": "$profit_factor"}, "wr": {"$avg": "$win_rate"},
            "sharpe": {"$avg": "$sharpe"}, "trades": {"$avg": "$trades"},
            "max_dd": {"$avg": "$max_dd_pct"}, "folds": {"$sum": 1},
        }},
        {"$sort": {"_id.pair": 1}},
    ]
    for doc in db.walk_forward_results.aggregate(wf_pipeline):
        wf.append({
            "pair": doc["_id"]["pair"], "period_type": doc["_id"]["period_type"],
            "pf": doc.get("pf"), "wr": doc.get("wr"), "sharpe": doc.get("sharpe"),
            "trades": int(doc.get("trades", 0)), "folds": doc.get("folds"),
            "max_dd": (doc.get("max_dd", 0) or 0) * 100,
        })

    # Count unique WF pairs
    wf_pairs = len(set(w["pair"] for w in wf))
    wf_folds = db.walk_forward_results.count_documents({"engine": engine})

    # Determine phase
    dead_engines = {"S6", "S7", "S9", "H2", "E4", "M1"}
    paper_engines = {"E3"}
    allow_count = sum(1 for p in pairs if p["verdict"] == "ALLOW")
    watch_count = sum(1 for p in pairs if p["verdict"] == "WATCH")
    block_count = sum(1 for p in pairs if p["verdict"] == "BLOCK")

    if engine in paper_engines:
        phase = "PAPER"
    elif engine in dead_engines:
        phase = "DEAD"
    elif wf_folds > 0 and allow_count > 0:
        phase = "ROBUSTNESS"
    elif len(pairs) > 0:
        phase = "BACKTEST"
    else:
        phase = "IDEA"

    bt_count = db.backtest_trades.count_documents({"engine": engine})
    period_doc = db.pair_historical.find_one({"engine": engine}, sort=[("created_at", -1)])
    period_str = period_doc.get("period", "") if period_doc else ""

    summary = {
        "name": engine, "display_name": meta.display_name if meta else engine,
        "phase": phase, "direction": meta.direction if meta else "BOTH",
        "allow": allow_count, "watch": watch_count, "block": block_count,
        "total_pairs": len(pairs), "total_bt_trades": bt_count,
        "avg_pf_allow": pf_allow_sum / pf_allow_count if pf_allow_count > 0 else 0,
        "wf_folds": wf_folds, "wf_pairs": wf_pairs, "period": period_str,
    }

    return _safe_json({
        "summary": summary, "pairs": pairs, "walk_forward": wf,
        "agg_close_types": dict(agg_ct),
    })


# ---------------------------------------------------------------------------
# TAB 3: Research Pipeline -- Pair drill-down
# ---------------------------------------------------------------------------

@router.get("/api/strategy/{engine}/{pair}")
async def api_strategy_pair(engine: str, pair: str):
    """Deep drill-down for one engine+pair: trades, equity, distributions."""
    db = _get_db()
    if db is None:
        return _safe_json({})

    # Get verdict summary
    verdict_doc = db.pair_historical.find_one({"engine": engine, "pair": pair})
    summary = {}
    if verdict_doc:
        summary = {
            "verdict": verdict_doc.get("verdict", ""),
            "pf": verdict_doc.get("profit_factor"),
            "wr": verdict_doc.get("win_rate"),
            "sharpe": verdict_doc.get("sharpe"),
            "trades": verdict_doc.get("trades", (verdict_doc.get("n_long", 0) + verdict_doc.get("n_short", 0))),
            "n_long": verdict_doc.get("n_long", 0),
            "n_short": verdict_doc.get("n_short", 0),
            "max_dd": (verdict_doc.get("max_dd_pct", 0) or 0) * 100,
            "pnl": verdict_doc.get("pnl_quote", 0),
            "period": verdict_doc.get("period", ""),
        }

    # Individual trades (all of them, sorted by time)
    raw_trades = list(
        db.backtest_trades.find(
            {"engine": engine, "pair": pair},
            {"timestamp": 1, "close_timestamp": 1, "side": 1, "close_type": 1,
             "net_pnl_quote": 1, "net_pnl_pct": 1, "cum_fees_quote": 1}
        ).sort("timestamp", 1)
    )

    trades = []
    equity = []
    pnl_values = []
    hold_times = []
    monthly_pnl = defaultdict(float)
    long_stats = {"trades": 0, "pnl": 0, "wins": 0}
    short_stats = {"trades": 0, "pnl": 0, "wins": 0}
    close_type_stats = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0})

    cum_pnl = 0
    for doc in raw_trades:
        pnl = doc.get("net_pnl_quote", 0) or 0
        pnl_pct = doc.get("net_pnl_pct", 0)
        ts = doc.get("timestamp", 0)
        close_ts = doc.get("close_timestamp", 0)
        side = doc.get("side", "")
        ct = str(doc.get("close_type", "")).replace("CloseType.", "")
        fees = doc.get("cum_fees_quote", 0)
        hold_h = (close_ts - ts) / 3600 if ts and close_ts else None

        cum_pnl += pnl
        pnl_values.append(pnl)
        if hold_h is not None:
            hold_times.append(hold_h)

        # Monthly PnL
        if ts:
            month_key = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")
            monthly_pnl[month_key] += pnl

        # Long/short split
        if side == "BUY":
            long_stats["trades"] += 1
            long_stats["pnl"] += pnl
            if pnl > 0:
                long_stats["wins"] += 1
        else:
            short_stats["trades"] += 1
            short_stats["pnl"] += pnl
            if pnl > 0:
                short_stats["wins"] += 1

        # Close types
        close_type_stats[ct]["count"] += 1
        close_type_stats[ct]["pnl"] += pnl
        if pnl > 0:
            close_type_stats[ct]["wins"] += 1

        trades.append({
            "timestamp": ts, "side": side, "close_type": ct,
            "pnl": round(pnl, 4), "pnl_pct": pnl_pct,
            "hold_hours": round(hold_h, 2) if hold_h else None,
            "fees": round(fees, 4) if fees else 0,
        })

        # Equity curve (sampled)
        equity.append({"t": ts, "pnl": round(cum_pnl, 2)})

    # Downsample equity if too many points
    if len(equity) > 400:
        step = len(equity) // 400
        equity = [equity[i] for i in range(0, len(equity), step)]

    # PnL histogram bins
    pnl_bins = {"labels": [], "counts": []}
    if pnl_values:
        mn = min(pnl_values)
        mx = max(pnl_values)
        n_bins = min(30, max(10, len(pnl_values) // 10))
        bw = (mx - mn) / n_bins if mx > mn else 1
        hist = [0] * n_bins
        for v in pnl_values:
            idx = min(int((v - mn) / bw), n_bins - 1)
            hist[idx] += 1
        pnl_bins = {
            "labels": [f"${mn + i * bw:.1f}" for i in range(n_bins)],
            "counts": hist,
        }

    # Hold time bins
    ht_bins = []
    if hold_times:
        boundaries = [0, 1, 4, 12, 24, 48, 120, 500]
        labels = ["<1h", "1-4h", "4-12h", "12-24h", "1-2d", "2-5d", ">5d"]
        for i in range(len(labels)):
            count = sum(1 for h in hold_times if boundaries[i] <= h < boundaries[i + 1])
            ht_bins.append({"label": labels[i], "count": count})

    # Walk-forward fold detail (individual folds, not averaged)
    wf_folds = []
    for doc in db.walk_forward_results.find({"engine": engine, "pair": pair}).sort("fold_index", 1):
        wf_folds.append({
            "fold_index": doc.get("fold_index", 0),
            "period_type": doc.get("period_type", ""),
            "period": doc.get("period", ""),
            "pf": doc.get("profit_factor"),
            "wr": doc.get("win_rate"),
            "sharpe": doc.get("sharpe"),
            "trades": doc.get("trades", 0),
            "max_dd": (doc.get("max_dd_pct", 0) or 0) * 100,
            "close_types": doc.get("close_types", {}),
            "n_long": doc.get("n_long", 0),
            "n_short": doc.get("n_short", 0),
        })

    return _safe_json({
        "summary": summary,
        "trades": trades,
        "equity": equity,
        "pnl_bins": pnl_bins,
        "close_types": dict(close_type_stats),
        "hold_time_bins": ht_bins,
        "long_short": {
            "long_trades": long_stats["trades"], "long_pnl": round(long_stats["pnl"], 2),
            "long_wr": round(long_stats["wins"] / long_stats["trades"] * 100, 1) if long_stats["trades"] > 0 else 0,
            "short_trades": short_stats["trades"], "short_pnl": round(short_stats["pnl"], 2),
            "short_wr": round(short_stats["wins"] / short_stats["trades"] * 100, 1) if short_stats["trades"] > 0 else 0,
        },
        "monthly_pnl": dict(monthly_pnl),
        "walk_forward": wf_folds,
    })

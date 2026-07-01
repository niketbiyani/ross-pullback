"""
Flask dashboard with Server-Sent Events for live alert streaming.
Run via main.py; open http://localhost:5050 in a browser.

Top section — RVOL Leaderboard:
  Top 20 stocks ranked by cumulative today volume ÷ (avg daily volume × elapsed
  session fraction). Equivalent to TradingView Rel Vol. Refreshes every 15 s.
  A ratio of 5 means on pace for 5× normal daily volume today.

Bottom section — MACD Alerts table:
  EMA  green  — episode-level EMA clear (V1 logic)
  EMA  amber  — only W2 fresh-window EMA clear (V2 logic)
  EMA  gray   — EMA was touched, no clear
  RSI  green  — RSI reached extreme from episode start (V1 logic)
  RSI  amber  — RSI reached extreme in W2 fresh window (V2 logic)
  RSI  gray   — no RSI extreme reached

Volume badge on alert rows (today's cumulative intraday vs avg daily):
  green  — >= 100% of avg daily volume already traded today
  amber  — 50–100%
  gray   — < 50% (thin, use caution)
"""
import json
import logging
import time
from typing import Callable

from flask import Flask, Response, jsonify

from alert_manager import AlertManager

logger = logging.getLogger(__name__)

_HTML = r'''
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Ross Pullback Scanner</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d0d;color:#e0e0e0;font-family:monospace;font-size:13px}
header{padding:10px 16px;background:#111827;border-bottom:1px solid #1f2937;
       display:flex;align-items:center;gap:16px}
h1{font-size:15px;color:#60a5fa;letter-spacing:.5px}
#status{font-size:11px;padding:2px 8px;border-radius:3px;background:#1f2937;color:#6b7280}
#status.live{background:#052e16;color:#4ade80}
#count{font-size:11px;color:#6b7280;margin-left:auto}

/* ── RVOL leaderboard ──────────────────────────────────────────────── */
#leaderboard{background:#080d14;border-bottom:2px solid #1f2937}
#lb-hdr{display:flex;align-items:center;gap:10px;padding:6px 14px 5px;flex-wrap:wrap}
.lb-title{color:#60a5fa;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase}
#lb-updated{font-size:10px;color:#374151;margin-left:auto}
.lb-explain{font-size:10px;color:#374151}
#lb-scroll{max-height:160px;overflow-y:auto}
#lb-scroll::-webkit-scrollbar{width:3px}
#lb-scroll::-webkit-scrollbar-thumb{background:#1f2937}
#lb-table{width:100%;border-collapse:collapse}
#lb-table th{padding:4px 10px;color:#4b5563;font-weight:normal;font-size:10px;
             border-bottom:1px solid #0f172a;white-space:nowrap;background:#080d14;
             position:sticky;top:0}
#lb-table td{padding:4px 10px;border-bottom:1px solid #0a0f18;white-space:nowrap;font-size:11px}
#lb-table tr:hover td{background:#0c1420}
.ratio-hi{color:#4ade80;font-weight:bold}
.ratio-md{color:#fb923c;font-weight:bold}
.ratio-lo{color:#9ca3af}
.bs-bar{display:inline-block;width:60px;height:6px;background:#1e293b;
        border-radius:3px;overflow:hidden;vertical-align:middle;margin:0 4px}
.bs-b{display:inline-block;height:100%;background:#4ade80;float:left}
.bs-s{display:inline-block;height:100%;background:#f87171;float:right}
.lb-empty td{color:#1f2937;font-size:11px;padding:18px 10px;text-align:center}
#lb-rank{color:#4b5563;font-size:10px;width:24px}

/* ── filters ───────────────────────────────────────────────────── */
#filters{padding:6px 16px;background:#0f172a;border-bottom:1px solid #1f2937;
         display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.fl{color:#4b5563;font-size:11px;margin-right:2px}
.fb{padding:3px 9px;border:1px solid #374151;border-radius:3px;
    background:#1f2937;color:#9ca3af;cursor:pointer;font-size:11px;font-family:monospace}
.fb.on{background:#1e3a5f;border-color:#3b82f6;color:#e0e0e0}

/* ── main alert table ──────────────────────────────────────────── */
.scroller{overflow-y:auto;height:calc(100vh - 248px)}
table{width:100%;border-collapse:collapse}
thead{position:sticky;top:0;background:#0f172a;z-index:5}
th{padding:7px 10px;text-align:left;color:#6b7280;font-weight:normal;
   border-bottom:1px solid #1f2937;white-space:nowrap;font-size:11px}
td{padding:5px 10px;border-bottom:1px solid #111827;white-space:nowrap}
tr:hover td{background:#111827}
@keyframes hl{from{background:#0d3321}to{background:transparent}}
.new td{animation:hl 2s ease-out}
.SHORT{color:#f87171}.LONG{color:#4ade80}
.WN{color:#94a3b8}.tf{color:#38bdf8}

/* ── shared badge styles ───────────────────────────────────────── */
.b{display:inline-block;padding:1px 6px;border-radius:3px;
   font-size:10px;font-weight:bold;letter-spacing:.4px;margin-right:3px}
.bGn{background:#052e16;border:1px solid #166534;color:#4ade80}
.bAm{background:#431407;border:1px solid #9a3412;color:#fb923c}
.bGy{background:#111827;border:1px solid #1f2937;color:#374151}
.vpct{font-size:9px;color:#6b7280;margin-left:2px}
</style>
</head>
<body>
<header>
  <h1>Ross Pullback Scanner</h1>
  <span id="status">connecting…</span>
  <span id="count"></span>
</header>

<!-- ── RVOL Leaderboard ─────────────────────────────────────────────── -->
<div id="leaderboard">
  <div id="lb-hdr">
    <span class="lb-title">Relative Volume — Top 20</span>
    <span class="lb-explain">today cumulative vol ÷ (avg daily × session % elapsed)</span>
    <span id="lb-updated"></span>
  </div>
  <div id="lb-scroll">
    <table id="lb-table">
      <thead><tr>
        <th id="lb-rank">#</th>
        <th>Symbol</th>
        <th>Rel Vol</th>
        <th>Today Vol</th>
        <th>Avg Daily</th>
        <th>Buyers → Sellers</th>
      </tr></thead>
      <tbody id="lb-tbody">
        <tr class="lb-empty"><td colspan="6">Waiting for live data…</td></tr>
      </tbody>
    </table>
  </div>
</div>

<!-- ── MACD Alert filters ────────────────────────────────────────── -->
<div id="filters">
  <span class="fl">Dir:</span>
  <button class="fb on" data-g="dir" data-v="ALL">All</button>
  <button class="fb" data-g="dir" data-v="SHORT">Short</button>
  <button class="fb" data-g="dir" data-v="LONG">Long</button>
  <span class="fl" style="margin-left:8px">TF:</span>
  <button class="fb on" data-g="tf" data-v="0">All</button>
  <button class="fb" data-g="tf" data-v="1">1m</button>
  <button class="fb" data-g="tf" data-v="3">3m</button>
  <button class="fb" data-g="tf" data-v="5">5m</button>
  <button class="fb" data-g="tf" data-v="15">15m</button>
  <span class="fl" style="margin-left:8px">Wave:</span>
  <button class="fb on" data-g="wave" data-v="0">All</button>
  <button class="fb" data-g="wave" data-v="1">W1</button>
  <button class="fb" data-g="wave" data-v="2">W2</button>
  <button class="fb" data-g="wave" data-v="3">W3+</button>
  <span class="fl" style="margin-left:8px">Quality:</span>
  <button class="fb on" data-g="tier" data-v="ALL">All</button>
  <button class="fb" data-g="tier" data-v="V1">V1 only</button>
  <button class="fb" data-g="tier" data-v="V2">V2 only</button>
  <button class="fb" data-g="tier" data-v="QUAL">V1 + V2</button>
  <span class="fl" style="margin-left:8px">Vol ≥</span>
  <input id="vol-input" type="number" min="0" max="999" value="50"
         style="width:58px;padding:2px 6px;background:#1f2937;border:1px solid #374151;
                border-radius:3px;color:#e0e0e0;font-size:11px;font-family:monospace">
  <span class="fl">% avg</span>
</div>

<!-- ── MACD Alert table ──────────────────────────────────────────── -->
<div class="scroller">
<table>
<thead>
<tr>
  <th>Time</th><th>Symbol</th><th>TF</th><th>Dir</th><th>Wave</th>
  <th>Entry</th><th>SL</th><th>SL%</th><th>RSI@entry</th>
  <th>EMA &nbsp; RSI</th><th>Volume</th><th>Ep Bars</th>
</tr>
</thead>
<tbody id="tb"></tbody>
</table>
</div>

<script>
/* ================================================================
   RVOL Leaderboard — polls /api/rvol-leaderboard every 15s
   ================================================================ */
function fmtV(v){
  return v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':v.toFixed(0);
}
function ratioCls(r){return r>=5?'ratio-hi':r>=2?'ratio-md':'ratio-lo';}

function renderLeaderboard(rows){
  const tbody=document.getElementById('lb-tbody');
  if(!rows||!rows.length){
    tbody.innerHTML='<tr class="lb-empty"><td colspan="6">No data yet — waiting for market open</td></tr>';
    return;
  }
  tbody.innerHTML=rows.map((r,i)=>{
    const bp=Math.round(r.buyer_pct*100);
    const sp=100-bp;
    return `<tr>
      <td id="lb-rank">${i+1}</td>
      <td><b>${r.symbol}</b></td>
      <td class="${ratioCls(r.ratio)}">${r.ratio.toFixed(2)}×</td>
      <td>${fmtV(r.today_vol)}</td>
      <td style="color:#4b5563">${fmtV(r.avg_daily)}</td>
      <td>
        <span style="color:#4ade80">${bp}%</span>
        <span class="bs-bar"><span class="bs-b" style="width:${bp}%"></span><span class="bs-s" style="width:${sp}%"></span></span>
        <span style="color:#f87171">${sp}%</span>
      </td>
    </tr>`;
  }).join('');
  const now=new Date();
  document.getElementById('lb-updated').textContent=
    'updated '+now.toTimeString().slice(0,5);
}

function fetchLeaderboard(){
  fetch('./api/rvol-leaderboard').then(r=>r.json()).then(renderLeaderboard).catch(()=>{});
}
fetchLeaderboard();
setInterval(fetchLeaderboard, 15000);

/* ================================================================
   MACD Alerts
   ================================================================ */
const F={dir:'ALL',tf:'0',wave:'0',tier:'ALL',vol:'0.5'};
let alerts=[];

document.querySelectorAll('.fb').forEach(b=>{
  b.addEventListener('click',()=>{
    const g=b.dataset.g;
    document.querySelectorAll(`.fb[data-g="${g}"]`).forEach(x=>x.classList.remove('on'));
    b.classList.add('on');
    F[g]=b.dataset.v;
    render();
  });
});
const volInput=document.getElementById('vol-input');
volInput.addEventListener('input',()=>{
  const v=parseFloat(volInput.value);
  F.vol=isNaN(v)?'0':String(v/100);
  render();
});

function emaCls(a){return a.ema_clear?'bGn':a.ema_clear_v2?'bAm':'bGy';}
function rsiCls(a){return a.rsi_extreme?'bGn':a.rsi_extreme_v2?'bAm':'bGy';}
function badges(a){
  return `<span class="b ${emaCls(a)}">EMA</span><span class="b ${rsiCls(a)}">RSI</span>`;
}
function volBadge(a){
  const v=a.today_volume||0, r=a.rel_volume||0;
  if(v===0) return '<span class="b bGy">—</span>';
  const vs=v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v.toFixed(0);
  const cls=r>=1.0?'bGn':r>=0.5?'bAm':'bGy';
  return `<span class="b ${cls}">${vs}</span><span class="vpct">${Math.round(r*100)}%</span>`;
}
function tier(a){
  if(a.ema_clear&&a.rsi_extreme) return 'V1';
  if(a.ema_clear_v2&&a.rsi_extreme_v2) return 'V2';
  return 'raw';
}
function ok(a){
  if(F.dir!=='ALL'&&a.direction!==F.dir) return false;
  if(F.tf!=='0'&&String(a.tf)!==F.tf) return false;
  if(F.wave==='1'&&a.wave_num!==1) return false;
  if(F.wave==='2'&&a.wave_num!==2) return false;
  if(F.wave==='3'&&a.wave_num<3) return false;
  const t=tier(a);
  if(F.tier==='V1'&&t!=='V1') return false;
  if(F.tier==='V2'&&t!=='V2') return false;
  if(F.tier==='QUAL'&&t==='raw') return false;
  const vt=parseFloat(F.vol);
  if(vt>0&&(a.rel_volume||0)<vt) return false;
  return true;
}
function render(){
  const scroller=document.querySelector('.scroller');
  const savedScroll=scroller.scrollTop;
  const rows=alerts.filter(ok).sort((a,b)=>b.ts-a.ts);
  document.getElementById('count').textContent=rows.length+' alerts';
  document.getElementById('tb').innerHTML=rows.map((a,i)=>`
  <tr class="${i<3?'new':''}">
    <td>${a.time_ist}</td>
    <td><b>${a.symbol}</b></td>
    <td class="tf">${a.tf}m</td>
    <td class="${a.direction}">${a.direction}</td>
    <td class="WN">W${a.wave_num}</td>
    <td>${a.entry_price.toFixed(2)}</td>
    <td>${a.sl_level.toFixed(2)}</td>
    <td>${a.sl_pct_str}</td>
    <td>${a.rsi_at_entry.toFixed(1)}</td>
    <td>${badges(a)}</td>
    <td>${volBadge(a)}</td>
    <td style="color:#6b7280">${a.ep_len_so_far}</td>
  </tr>`).join('');
  scroller.scrollTop=savedScroll;
}
function mergeAlerts(incoming){
  const seen=new Set(alerts.map(a=>a.symbol+':'+a.ts+':'+a.tf));
  incoming.forEach(a=>{
    const k=a.symbol+':'+a.ts+':'+a.tf;
    if(!seen.has(k)){alerts.push(a);seen.add(k);}
  });
  render();
}
fetch('./api/alerts').then(r=>r.json()).then(d=>{alerts=d;render();});

/* ================================================================
   SSE stream — MACD alerts only
   ================================================================ */
const es=new EventSource('./stream');
es.onopen=()=>{
  const s=document.getElementById('status');
  s.textContent='live';s.className='live';
  fetch('./api/alerts').then(r=>r.json()).then(mergeAlerts);
  fetchLeaderboard();
};
es.onerror=()=>{
  document.getElementById('status').textContent='reconnecting…';
  document.getElementById('status').className='';
};
es.addEventListener('alert',e=>{
  alerts.unshift(JSON.parse(e.data));
  if(alerts.length>5000) alerts.pop();
  render();
});
</script>
</body>
</html>
'''


def create_app(alert_mgr: AlertManager,
               get_leaderboard: Callable[[], list[dict]]) -> Flask:
    app = Flask(__name__)

    @app.route('/')
    def index():
        return _HTML

    @app.route('/api/alerts')
    def api_alerts():
        return jsonify(alert_mgr.get_all())

    @app.route('/api/rvol-leaderboard')
    def api_rvol_leaderboard():
        return jsonify(get_leaderboard())

    @app.route('/stream')
    def stream():
        q = alert_mgr.subscribe()

        def gen():
            yield 'data: connected\n\n'
            while True:
                if q:
                    yield f'event: alert\ndata: {json.dumps(q.popleft())}\n\n'
                else:
                    time.sleep(0.1)
                    yield ': keepalive\n\n'

        return Response(
            gen(), mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
        )

    return app

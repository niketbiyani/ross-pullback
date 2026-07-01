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

Volume badge on alert rows (today's cumulative intraday volume at alert time):
  green  — >= 500K shares
  amber  — < 500K shares but non-zero
  gray   — no today volume data (historical bootstrap signal)
"""
import json
import logging
import time
from typing import Callable

from flask import Flask, Response, jsonify

from alert_manager import AlertManager

logger = logging.getLogger(__name__)

_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Ross Pullback Scanner</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d0d;color:#e0e0e0;font-family:monospace;font-size:13px;
     display:flex;flex-direction:column;height:100vh;overflow:hidden}
header{flex-shrink:0;padding:8px 16px;background:#111827;border-bottom:1px solid #1f2937;
       display:flex;align-items:center;gap:12px}
h1{font-size:15px;color:#60a5fa;letter-spacing:.5px}
#status{font-size:11px;padding:2px 8px;border-radius:3px;background:#1f2937;color:#6b7280}
#status.live{background:#052e16;color:#4ade80}
#tabs{display:flex;gap:3px;margin-left:auto}
.tab-btn{padding:3px 14px;border:1px solid #374151;border-radius:3px;
         background:#1f2937;color:#6b7280;cursor:pointer;font-size:11px;font-family:monospace}
.tab-btn.on{background:#1e3a5f;border-color:#3b82f6;color:#e0e0e0}
#count{font-size:11px;color:#4b5563;min-width:70px;text-align:right}
.fl{color:#4b5563;font-size:11px;margin-right:2px}
.fb{padding:3px 9px;border:1px solid #374151;border-radius:3px;
    background:#1f2937;color:#9ca3af;cursor:pointer;font-size:11px;font-family:monospace}
.fb.on{background:#1e3a5f;border-color:#3b82f6;color:#e0e0e0}
.num-in{width:68px;padding:2px 6px;background:#1f2937;border:1px solid #374151;
        border-radius:3px;color:#e0e0e0;font-size:11px;font-family:monospace}
.b{display:inline-block;padding:1px 6px;border-radius:3px;
   font-size:10px;font-weight:bold;letter-spacing:.4px;margin-right:3px}
.bGn{background:#052e16;border:1px solid #166534;color:#4ade80}
.bAm{background:#431407;border:1px solid #9a3412;color:#fb923c}
.bGy{background:#111827;border:1px solid #1f2937;color:#374151}

/* ── Alerts tab ───────────────────────────────────────────────────── */
#tab-alerts{flex:1;display:flex;flex-direction:column;overflow:hidden}
#filters{flex-shrink:0;padding:6px 16px;background:#0f172a;border-bottom:1px solid #1f2937;
         display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.scroller{flex:1;overflow-y:auto}
#tab-alerts table{width:100%;border-collapse:collapse}
#tab-alerts thead{position:sticky;top:0;background:#0f172a;z-index:5}
#tab-alerts th{padding:7px 10px;text-align:left;color:#6b7280;font-weight:normal;
               border-bottom:1px solid #1f2937;white-space:nowrap;font-size:11px}
#tab-alerts td{padding:5px 10px;border-bottom:1px solid #111827;white-space:nowrap}
#tab-alerts tr:hover td{background:#111827}
@keyframes hl{from{background:#0d3321}to{background:transparent}}
.new td{animation:hl 2s ease-out}
.SHORT{color:#f87171}.LONG{color:#4ade80}
.WN{color:#94a3b8}.tf{color:#38bdf8}

/* ── Rel Vol tab ──────────────────────────────────────────────────── */
#tab-rvol{flex:1;display:none;flex-direction:column;overflow:hidden}
#rvol-toolbar{flex-shrink:0;padding:6px 14px;background:#080d14;border-bottom:1px solid #1f2937;
              display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.lb-title{color:#60a5fa;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase}
#lb-count{font-size:10px;color:#4b5563}
#lb-updated{font-size:10px;color:#374151;margin-left:auto}
#lb-scroll{flex:1;overflow-y:auto}
#lb-scroll::-webkit-scrollbar{width:3px}
#lb-scroll::-webkit-scrollbar-thumb{background:#1f2937}
#lb-table{width:100%;border-collapse:collapse}
#lb-table thead{position:sticky;top:0;background:#080d14}
#lb-table th{padding:4px 10px;color:#4b5563;font-weight:normal;font-size:10px;
             border-bottom:1px solid #0f172a;white-space:nowrap}
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
</style>
</head>
<body>
<header>
  <h1>Ross Pullback Scanner</h1>
  <span id="status">connecting…</span>
  <div id="tabs">
    <button class="tab-btn on" data-tab="alerts">Alerts</button>
    <button class="tab-btn" data-tab="rvol">Rel Vol</button>
  </div>
  <span id="count"></span>
</header>

<!-- ── Alerts tab ─────────────────────────────────────────────────── -->
<div id="tab-alerts">
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
  <input id="vol-input" type="number" min="0" step="100" value="500" class="num-in">
  <span class="fl">K shares</span>
</div>
<div class="scroller">
<table>
<thead><tr>
  <th>Time</th><th>Symbol</th><th>TF</th><th>Dir</th><th>Wave</th>
  <th>Entry</th><th>SL</th><th>SL%</th><th>RSI@entry</th>
  <th>EMA &nbsp; RSI</th><th>Volume</th><th>Ep Bars</th>
</tr></thead>
<tbody id="tb"></tbody>
</table>
</div>
</div>

<!-- ── Rel Vol tab ────────────────────────────────────────────────── -->
<div id="tab-rvol">
<div id="rvol-toolbar">
  <span class="lb-title">Relative Volume</span>
  <span class="fl" style="margin-left:12px">Vol ≥</span>
  <input id="lb-vol-input" type="number" min="0" step="100" value="500" class="num-in">
  <span class="fl">K shares</span>
  <span id="lb-count"></span>
  <span id="lb-updated"></span>
</div>
<div id="lb-scroll">
<table id="lb-table">
<thead><tr>
  <th style="width:28px">#</th>
  <th>Symbol</th>
  <th>Bar RVOL</th>
  <th>Cum RVOL</th>
  <th>Today Vol</th>
  <th>Avg Daily</th>
  <th>Buyers → Sellers</th>
</tr></thead>
<tbody id="lb-tbody">
  <tr class="lb-empty"><td colspan="7">Waiting for live data…</td></tr>
</tbody>
</table>
</div>
</div>

<script>
/* ================================================================
   Tabs
   ================================================================ */
let currentTab = 'alerts';
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    if (tab === currentTab) return;
    currentTab = tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('on', b.dataset.tab === tab));
    document.getElementById('tab-alerts').style.display = tab === 'alerts' ? 'flex' : 'none';
    document.getElementById('tab-rvol').style.display   = tab === 'rvol'   ? 'flex' : 'none';
    if (tab === 'rvol') fetchLeaderboard();
    document.getElementById('count').textContent = tab === 'alerts' ? (filteredCount + ' alerts') : '';
  });
});

/* ================================================================
   Rel Vol leaderboard
   ================================================================ */
function fmtV(v){
  return v>=1e6?(v/1e6).toFixed(2)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':String(v);
}
function ratioCls(r){return r>=5?'ratio-hi':r>=2?'ratio-md':'ratio-lo';}
function barRvolCls(r){return r>=10?'ratio-hi':r>=2?'ratio-md':'ratio-lo';}

let lbData = [];
let lbMinVol = 500000;

document.getElementById('lb-vol-input').addEventListener('input', e => {
  lbMinVol = (parseFloat(e.target.value) || 0) * 1000;
  renderLeaderboard();
});

function renderLeaderboard() {
  if (currentTab !== 'rvol') return;
  const rows = lbData.filter(r => r.today_vol >= lbMinVol);
  document.getElementById('lb-count').textContent = rows.length + ' symbols';
  const tbody = document.getElementById('lb-tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr class="lb-empty"><td colspan="7">No data — waiting for market open</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map((r, i) => {
    const bp = Math.round(r.buyer_pct * 100), sp = 100 - bp;
    const brvol = r.bar_rvol || 0;
    return `<tr>
      <td style="color:#4b5563;font-size:10px">${i+1}</td>
      <td><b>${r.symbol}</b></td>
      <td class="${barRvolCls(brvol)}">${brvol.toFixed(1)}×</td>
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
  document.getElementById('lb-updated').textContent =
    'updated ' + new Date().toTimeString().slice(0, 5);
}

function fetchLeaderboard() {
  fetch('./api/rvol-leaderboard').then(r => r.json()).then(rows => {
    lbData = rows;
    renderLeaderboard();
  }).catch(() => {});
}
setInterval(fetchLeaderboard, 15000);

/* ================================================================
   Alerts
   ================================================================ */
const F = {dir:'ALL', tf:'0', wave:'0', tier:'ALL', vol:'500000'};
let alerts = [];
let filteredCount = 0;

document.querySelectorAll('.fb').forEach(b => {
  b.addEventListener('click', () => {
    const g = b.dataset.g;
    document.querySelectorAll(`.fb[data-g="${g}"]`).forEach(x => x.classList.remove('on'));
    b.classList.add('on');
    F[g] = b.dataset.v;
    render();
  });
});
document.getElementById('vol-input').addEventListener('input', e => {
  const v = parseFloat(e.target.value);
  F.vol = isNaN(v) ? '0' : String(v * 1000);
  render();
});

function emaCls(a){return a.ema_clear?'bGn':a.ema_clear_v2?'bAm':'bGy';}
function rsiCls(a){return a.rsi_extreme?'bGn':a.rsi_extreme_v2?'bAm':'bGy';}
function badges(a){
  return `<span class="b ${emaCls(a)}">EMA</span><span class="b ${rsiCls(a)}">RSI</span>`;
}
function volBadge(a){
  const v = a.today_volume || 0;
  if (!v) return '<span class="b bGy">—</span>';
  const vs = v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v.toFixed(0);
  return `<span class="b ${v>=500000?'bGn':'bAm'}">${vs}</span>`;
}
function tier(a){
  if (a.ema_clear && a.rsi_extreme) return 'V1';
  if (a.ema_clear_v2 && a.rsi_extreme_v2) return 'V2';
  return 'raw';
}
function ok(a){
  if (F.dir !== 'ALL' && a.direction !== F.dir) return false;
  if (F.tf  !== '0'   && String(a.tf) !== F.tf) return false;
  if (F.wave === '1' && a.wave_num !== 1) return false;
  if (F.wave === '2' && a.wave_num !== 2) return false;
  if (F.wave === '3' && a.wave_num < 3)  return false;
  const t = tier(a);
  if (F.tier === 'V1'   && t !== 'V1')  return false;
  if (F.tier === 'V2'   && t !== 'V2')  return false;
  if (F.tier === 'QUAL' && t === 'raw') return false;
  const vt = parseFloat(F.vol);
  if (vt > 0 && (a.today_volume || 0) < vt) return false;
  return true;
}
function render(){
  const scroller = document.querySelector('.scroller');
  const savedTop = scroller.scrollTop;
  const rows = alerts.filter(ok).sort((a, b) => b.ts - a.ts);
  filteredCount = rows.length;
  if (currentTab === 'alerts')
    document.getElementById('count').textContent = filteredCount + ' alerts';
  document.getElementById('tb').innerHTML = rows.map((a, i) => `
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
  scroller.scrollTop = savedTop;
}
function mergeAlerts(incoming) {
  const seen = new Set(alerts.map(a => a.symbol+':'+a.ts+':'+a.tf));
  incoming.forEach(a => {
    const k = a.symbol+':'+a.ts+':'+a.tf;
    if (!seen.has(k)) { alerts.push(a); seen.add(k); }
  });
  render();
}
fetch('./api/alerts').then(r => r.json()).then(d => { alerts = d; render(); });

/* ================================================================
   SSE
   ================================================================ */
const es = new EventSource('./stream');
es.onopen = () => {
  const s = document.getElementById('status');
  s.textContent = 'live'; s.className = 'live';
  fetch('./api/alerts').then(r => r.json()).then(mergeAlerts);
};
es.onerror = () => {
  document.getElementById('status').textContent = 'reconnecting…';
  document.getElementById('status').className = '';
};
es.addEventListener('alert', e => {
  alerts.unshift(JSON.parse(e.data));
  if (alerts.length > 5000) alerts.pop();
  render();
});
</script>
</body>
</html>
'''


def create_app(alert_mgr: AlertManager,
               get_leaderboard: Callable[[], list[dict]],
               get_debug: Callable[[], dict] | None = None) -> Flask:
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

    @app.route('/api/debug')
    def api_debug():
        if get_debug:
            return jsonify(get_debug())
        return jsonify({'error': 'no debug fn'})

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

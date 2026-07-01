"""
Flask dashboard with Server-Sent Events for live alert streaming.
Run via main.py; open http://localhost:5050 in a browser.

Top section — Vol Spikes:
  Cards showing any 1m bar that exceeded RVOL_SPIKE_THRESHOLD× its historical
  average for that minute-of-day. Each card shows RVOL, close price, and an
  estimated buyer/seller split (same formula as TV's Volume Buyers vs Sellers).

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

from flask import Flask, Response, jsonify

from alert_manager import AlertManager, VolumeAlertManager

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

/* ── vol spike hot-box ─────────────────────────────────────────── */
#hotbox{background:#080d14;border-bottom:2px solid #1f2937;padding:6px 12px 8px}
#hot-hdr{display:flex;align-items:center;gap:10px;margin-bottom:5px}
.hot-title{color:#60a5fa;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase}
.hot-thresh{font-size:10px;color:#6b7280;background:#1f2937;padding:1px 6px;border-radius:2px}
#hot-count{font-size:10px;color:#6b7280;margin-left:auto}
#hot-cards{display:flex;gap:8px;overflow-x:auto;padding-bottom:2px;min-height:96px;
           align-items:flex-start}
#hot-cards::-webkit-scrollbar{height:3px}
#hot-cards::-webkit-scrollbar-thumb{background:#374151;border-radius:2px}
.hc{background:#0f172a;border:1px solid #1f2937;border-radius:5px;
    padding:6px 10px;min-width:155px;max-width:155px;flex-shrink:0;cursor:default;
    transition:border-color .15s}
.hc:hover{border-color:#3b82f6}
.hc-top{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px}
.hc-sym{font-size:12px;font-weight:bold;color:#e0e0e0}
.hc-time{font-size:9px;color:#4b5563}
.hc-mid{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:5px}
.hc-price{font-size:10px;color:#94a3b8}
.hc-rvol{font-size:15px;font-weight:bold}
.hc-bar{height:5px;background:#1e293b;border-radius:3px;overflow:hidden;margin-bottom:3px;display:flex}
.hc-buy{background:#4ade80}
.hc-sell{background:#f87171}
.hc-pct{font-size:9px;color:#4b5563;display:flex;justify-content:space-between}
.hc-vol{font-size:9px;color:#374151;margin-top:3px}
.hot-empty{color:#1f2937;font-size:11px;padding:30px 8px;align-self:center}

/* ── filters ───────────────────────────────────────────────────── */
#filters{padding:6px 16px;background:#0f172a;border-bottom:1px solid #1f2937;
         display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.fl{color:#4b5563;font-size:11px;margin-right:2px}
.fb{padding:3px 9px;border:1px solid #374151;border-radius:3px;
    background:#1f2937;color:#9ca3af;cursor:pointer;font-size:11px;font-family:monospace}
.fb.on{background:#1e3a5f;border-color:#3b82f6;color:#e0e0e0}

/* ── main alert table ──────────────────────────────────────────── */
.scroller{overflow-y:auto;height:calc(100vh - 228px)}
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

<!-- ── Vol Spikes hot-box ───────────────────────────────────────── -->
<div id="hotbox">
  <div id="hot-hdr">
    <span class="hot-title">Vol Spikes</span>
    <span class="hot-thresh" id="hot-thresh"></span>
    <span id="hot-count">0 spikes</span>
  </div>
  <div id="hot-cards"><span class="hot-empty">No spikes yet — waiting for market open</span></div>
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
  <span class="fl" style="margin-left:8px">Vol today:</span>
  <button class="fb on" data-g="vol" data-v="0">All</button>
  <button class="fb" data-g="vol" data-v="0.25">25%+</button>
  <button class="fb" data-g="vol" data-v="0.5">50%+</button>
  <button class="fb" data-g="vol" data-v="1">100%+</button>
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
   MACD Alerts
   ================================================================ */
const F={dir:'ALL',tf:'0',wave:'0',tier:'ALL',vol:'0'};
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
}
fetch('./api/alerts').then(r=>r.json()).then(d=>{alerts=d;render();});

/* ================================================================
   Vol Spikes hot-box
   ================================================================ */
let volSpikes=[];
const HOT_MAX_AGE_MS = 45 * 60 * 1000;   // keep spikes for 45 min

function fmtV(v){
  return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(0)+'K':v.toFixed(0);
}
function rvolColor(r){
  return r>=5?'#4ade80':r>=3?'#fb923c':'#9ca3af';
}
function renderHot(){
  const now=Date.now();
  const rows=volSpikes
    .filter(s=>(now - s.ts*1000) < HOT_MAX_AGE_MS)
    .sort((a,b)=>b.ts-a.ts)
    .slice(0,60);
  document.getElementById('hot-count').textContent=rows.length+' spike'+(rows.length===1?'':'s');
  const el=document.getElementById('hot-cards');
  if(!rows.length){
    el.innerHTML='<span class="hot-empty">No spikes yet — waiting for market open</span>';
    return;
  }
  el.innerHTML=rows.map(s=>{
    const bp=Math.round(s.buyer_pct*100);
    const sp=100-bp;
    return `<div class="hc">
      <div class="hc-top"><b class="hc-sym">${s.symbol}</b><span class="hc-time">${s.time_ist}</span></div>
      <div class="hc-mid">
        <span class="hc-price">₹${s.close.toFixed(2)}</span>
        <span class="hc-rvol" style="color:${rvolColor(s.rvol)}">${s.rvol.toFixed(1)}×</span>
      </div>
      <div class="hc-bar">
        <div class="hc-buy" style="width:${bp}%"></div>
        <div class="hc-sell" style="width:${sp}%"></div>
      </div>
      <div class="hc-pct"><span style="color:#4ade80">B ${bp}%</span><span style="color:#f87171">S ${sp}%</span></div>
      <div class="hc-vol">${fmtV(s.volume)} / ${fmtV(s.avg_volume)} avg</div>
    </div>`;
  }).join('');
}
fetch('./api/vol-spikes').then(r=>r.json()).then(d=>{volSpikes=d;renderHot();});
setInterval(renderHot, 60000);   // age out stale cards every minute

/* ================================================================
   SSE stream — handles both alert and vol_spike events
   ================================================================ */
const es=new EventSource('./stream');
es.onopen=()=>{const s=document.getElementById('status');s.textContent='live';s.className='live';};
es.onerror=()=>{document.getElementById('status').textContent='reconnecting…';document.getElementById('status').className='';};

es.addEventListener('alert',e=>{
  alerts.unshift(JSON.parse(e.data));
  if(alerts.length>1000) alerts.pop();
  render();
});
es.addEventListener('vol_spike',e=>{
  volSpikes.unshift(JSON.parse(e.data));
  if(volSpikes.length>500) volSpikes.pop();
  renderHot();
});

/* Set threshold label from server */
fetch('./api/config').then(r=>r.json()).then(d=>{
  document.getElementById('hot-thresh').textContent='RVOL ≥ '+d.rvol_threshold+'×';
});
</script>
</body>
</html>
'''


def create_app(alert_mgr: AlertManager, vol_alert_mgr: VolumeAlertManager) -> Flask:
    app = Flask(__name__)

    @app.route('/')
    def index():
        return _HTML

    @app.route('/api/alerts')
    def api_alerts():
        return jsonify(alert_mgr.get_all())

    @app.route('/api/vol-spikes')
    def api_vol_spikes():
        return jsonify(vol_alert_mgr.get_all())

    @app.route('/api/config')
    def api_config():
        from config import Config
        return jsonify({'rvol_threshold': Config.RVOL_SPIKE_THRESHOLD})

    @app.route('/stream')
    def stream():
        q_macd = alert_mgr.subscribe()
        q_vol  = vol_alert_mgr.subscribe()

        def gen():
            yield 'data: connected\n\n'
            while True:
                sent = False
                if q_macd:
                    yield f'event: alert\ndata: {json.dumps(q_macd.popleft())}\n\n'
                    sent = True
                if q_vol:
                    yield f'event: vol_spike\ndata: {json.dumps(q_vol.popleft())}\n\n'
                    sent = True
                if not sent:
                    time.sleep(0.1)
                    yield ': keepalive\n\n'

        return Response(
            gen(), mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
        )

    return app

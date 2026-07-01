"""
Flask dashboard with Server-Sent Events for live alert streaming.
Run via main.py; open http://localhost:5050 in a browser.

Each alert row shows two mini-badges in the Filter column:
  EMA  green  — episode-level EMA clear (V1 logic)
  EMA  amber  — only W2 fresh-window EMA clear (V2 logic)
  EMA  gray   — EMA was touched, no clear
  RSI  green  — RSI reached extreme from episode start (V1 logic)
  RSI  amber  — RSI reached extreme in W2 fresh window (V2 logic)
  RSI  gray   — no RSI extreme reached

Two greens = V1 quality.  Two ambers (or one green one amber) = V2 quality.
"""
import json
import logging
import time

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
#filters{padding:6px 16px;background:#0f172a;border-bottom:1px solid #1f2937;
         display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.fl{color:#4b5563;font-size:11px;margin-right:2px}
.fb{padding:3px 9px;border:1px solid #374151;border-radius:3px;
    background:#1f2937;color:#9ca3af;cursor:pointer;font-size:11px;font-family:monospace}
.fb.on{background:#1e3a5f;border-color:#3b82f6;color:#e0e0e0}
.scroller{overflow-y:auto;height:calc(100vh - 82px)}
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
/* filter badges */
.b{display:inline-block;padding:1px 6px;border-radius:3px;
   font-size:10px;font-weight:bold;letter-spacing:.4px;margin-right:3px}
.bGn{background:#052e16;border:1px solid #166534;color:#4ade80}  /* green  = V1 */
.bAm{background:#431407;border:1px solid #9a3412;color:#fb923c}  /* amber  = V2 */
.bGy{background:#111827;border:1px solid #1f2937;color:#374151}  /* gray   = off */
</style>
</head>
<body>
<header>
  <h1>Ross Pullback Scanner</h1>
  <span id="status">connecting…</span>
  <span id="count"></span>
</header>
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
</div>
<div class="scroller">
<table>
<thead>
<tr>
  <th>Time</th><th>Symbol</th><th>TF</th><th>Dir</th><th>Wave</th>
  <th>Entry</th><th>SL</th><th>SL%</th><th>RSI@entry</th>
  <th>EMA &nbsp; RSI</th><th>Ep Bars</th>
</tr>
</thead>
<tbody id="tb"></tbody>
</table>
</div>
<script>
const F={dir:'ALL',tf:'0',wave:'0',tier:'ALL'};
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

/* ---------- badge logic ---------- */
function emaCls(a){
  if(a.ema_clear)    return 'bGn';   // green  = V1 episode-level clear
  if(a.ema_clear_v2) return 'bAm';  // amber  = V2 fresh-window clear
  return 'bGy';                      // gray   = touched / not clear
}
function rsiCls(a){
  if(a.rsi_extreme)    return 'bGn'; // green  = V1 episode-level extreme
  if(a.rsi_extreme_v2) return 'bAm';// amber  = V2 fresh-window extreme
  return 'bGy';                      // gray   = not reached
}
function badges(a){
  return `<span class="b ${emaCls(a)}">EMA</span><span class="b ${rsiCls(a)}">RSI</span>`;
}

/* ---------- tier for filter button ---------- */
function tier(a){
  if(a.ema_clear && a.rsi_extreme)          return 'V1';
  if(a.ema_clear_v2 && a.rsi_extreme_v2)   return 'V2';
  return 'raw';
}

/* ---------- row filter ---------- */
function ok(a){
  if(F.dir!=='ALL'&&a.direction!==F.dir)return false;
  if(F.tf!=='0'&&String(a.tf)!==F.tf)return false;
  if(F.wave==='1'&&a.wave_num!==1)return false;
  if(F.wave==='2'&&a.wave_num!==2)return false;
  if(F.wave==='3'&&a.wave_num<3)return false;
  const t=tier(a);
  if(F.tier==='V1'&&t!=='V1')return false;
  if(F.tier==='V2'&&t!=='V2')return false;
  if(F.tier==='QUAL'&&t==='raw')return false;
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
    <td style="color:#6b7280">${a.ep_len_so_far}</td>
  </tr>`).join('');
}

fetch('./api/alerts').then(r=>r.json()).then(d=>{alerts=d;render();});

const es=new EventSource('./stream');
es.onopen=()=>{const s=document.getElementById('status');s.textContent='live';s.className='live';};
es.onerror=()=>{document.getElementById('status').textContent='reconnecting…';};
es.addEventListener('alert',e=>{
  alerts.unshift(JSON.parse(e.data));
  if(alerts.length>1000)alerts.pop();
  render();
});
</script>
</body>
</html>
'''


def create_app(alert_mgr: AlertManager) -> Flask:
    app = Flask(__name__)

    @app.route('/')
    def index():
        return _HTML

    @app.route('/api/alerts')
    def api_alerts():
        return jsonify(alert_mgr.get_all())

    @app.route('/stream')
    def stream():
        q = alert_mgr.subscribe()

        def gen():
            yield 'data: connected\n\n'
            while True:
                if q:
                    d = q.popleft()
                    yield f'event: alert\ndata: {json.dumps(d)}\n\n'
                else:
                    time.sleep(0.1)
                    yield ': keepalive\n\n'

        return Response(
            gen(), mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
        )

    return app

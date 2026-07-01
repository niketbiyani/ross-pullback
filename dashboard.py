"""
Flask dashboard with Server-Sent Events for live alert streaming.
Run via main.py; open http://localhost:5050 in a browser.
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
.WN{color:#94a3b8}
.Y{color:#4ade80}.N{color:#f87171}.NA{color:#6b7280}
.tf{color:#38bdf8}
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
  <button class="fb" data-g="ema" data-v="1">EMA Clear</button>
  <button class="fb" data-g="rsi" data-v="1">RSI Extreme</button>
  <button class="fb" data-g="both" data-v="1">Both Filters</button>
</div>
<div class="scroller">
<table>
<thead>
<tr>
  <th>Time</th><th>Symbol</th><th>TF</th><th>Dir</th><th>Wave</th>
  <th>Entry</th><th>SL</th><th>SL%</th><th>RSI@entry</th>
  <th>EMA Clear</th><th>RSI Extreme</th><th>Ep Bars</th>
</tr>
</thead>
<tbody id="tb"></tbody>
</table>
</div>
<script>
const F={dir:'ALL',tf:'0',wave:'0',ema:'0',rsi:'0',both:'0'};
let alerts=[];

document.querySelectorAll('.fb').forEach(b=>{
  b.addEventListener('click',()=>{
    const g=b.dataset.g;
    // Toggle logic: exclusive within dir/tf/wave groups; toggle for filter groups
    if(['dir','tf','wave'].includes(g)){
      document.querySelectorAll(`.fb[data-g="${g}"]`).forEach(x=>x.classList.remove('on'));
      b.classList.add('on');
      F[g]=b.dataset.v;
    } else {
      b.classList.toggle('on');
      F[g]=b.classList.contains('on')?'1':'0';
      if(g==='both'&&F[g]==='1'){F.ema='0';F.rsi='0';}
    }
    render();
  });
});

function ok(a){
  if(F.dir!=='ALL'&&a.direction!==F.dir)return false;
  if(F.tf!=='0'&&String(a.tf)!==F.tf)return false;
  if(F.wave==='1'&&a.wave_num!==1)return false;
  if(F.wave==='2'&&a.wave_num!==2)return false;
  if(F.wave==='3'&&a.wave_num<3)return false;
  if(F.ema==='1'&&!a.ema_clear)return false;
  if(F.rsi==='1'&&!a.rsi_extreme)return false;
  if(F.both==='1'&&!(a.ema_clear&&a.rsi_extreme))return false;
  return true;
}

function yn(v){return v?'<span class="Y">YES</span>':'<span class="N">NO</span>';}

function render(){
  const rows=alerts.filter(ok);
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
    <td>${yn(a.ema_clear)}</td>
    <td>${yn(a.rsi_extreme)}</td>
    <td style="color:#6b7280">${a.ep_len_so_far}</td>
  </tr>`).join('');
}

fetch('/api/alerts').then(r=>r.json()).then(d=>{alerts=d;render();});

const es=new EventSource('/stream');
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

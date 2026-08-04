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
import os
import time
from typing import Callable

from flask import Flask, Response, jsonify, request

from alert_manager import AlertManager
from config import Config

logger = logging.getLogger(__name__)

_HTML = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Ross Pullback Scanner</title>
<script src="./static/lightweight-charts.js"></script>
<script src="./static/tv.js"></script>
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
.fb.tf-btn.active{background:#2563eb !important;border-color:#3b82f6 !important;color:#ffffff !important}
.num-in{width:68px;padding:2px 6px;background:#1f2937;border:1px solid #374151;
        border-radius:3px;color:#e0e0e0;font-size:11px;font-family:monospace}
.b{display:inline-block;padding:1px 6px;border-radius:3px;
   font-size:10px;font-weight:bold;letter-spacing:.4px;margin-right:3px}
.bGn{background:#052e16;border:1px solid #166534;color:#4ade80}
.bAm{background:#431407;border:1px solid #9a3412;color:#fb923c}
.bGy{background:#111827;border:1px solid #1f2937;color:#374151}
.date-in{width:108px;padding:2px 6px;background:#1f2937;border:1px solid #374151;
         border-radius:3px;color:#e0e0e0;font-size:11px;font-family:monospace}
.date-in::-webkit-calendar-picker-indicator{filter:invert(0.5)}
#hist-panel{flex:1;overflow-y:auto;display:none}
#hist-table{width:100%;border-collapse:collapse}
#hist-table thead{position:sticky;top:0;background:#080d14}
#hist-table th{padding:4px 10px;color:#4b5563;font-weight:normal;font-size:10px;
               border-bottom:1px solid #0f172a;white-space:nowrap}
#hist-table td{padding:5px 10px;border-bottom:1px solid #0a0f18;white-space:nowrap;font-size:11px}
#hist-table tr:hover td{background:#0c1420}

/* ── Alerts tab ───────────────────────────────────────────────────── */
#tab-alerts{flex:1;display:none;flex-direction:column;overflow:hidden}
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
.SHORT{color:#f87171}.LONG{color:#4ade80}.MOM{color:#facc15;font-weight:bold}
.WN{color:#94a3b8}.tf{color:#38bdf8}

/* ── Rel Vol (Movers) tab with Split Screen ───────────────────────── */
#tab-rvol{flex:1;display:flex;flex-direction:row;overflow:hidden}
#left-pane{width:40%;display:flex;flex-direction:column;border-right:1px solid #1f2937;overflow:hidden}
#right-pane{width:60%;background:#151924;display:flex;flex-direction:column;position:relative}

#rvol-toolbar{flex-shrink:0;padding:6px 14px;background:#080d14;border-bottom:1px solid #1f2937;
              display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.lb-title{color:#60a5fa;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase}
#lb-count{font-size:10px;color:#4b5563}
#lb-updated{font-size:10px;color:#374151;margin-left:auto}
#lb-scroll{flex:1;overflow-y:auto}
#lb-scroll::-webkit-scrollbar{width:3px}
#lb-scroll::-webkit-scrollbar-thumb{background:#1f2937}
#lb-table{width:100%;border-collapse:collapse}
#lb-table thead{position:sticky;top:0;background:#080d14;z-index:10}
#lb-table th{padding:6px 10px;color:#4b5563;font-weight:normal;font-size:10px;
             border-bottom:1px solid #0f172a;white-space:nowrap}
#lb-table td{padding:5px 10px;border-bottom:1px solid #0a0f18;white-space:nowrap;font-size:11px}

/* Hover & Active Styles */
.sym-header:hover td{background:#141c2c}
.ep-row:hover td{background:#1b253c !important}
.active-row td{background:#1e3a5f !important;color:#ffffff !important}

.ratio-hi{color:#4ade80;font-weight:bold}
.ratio-md{color:#fb923c;font-weight:bold}
.ratio-lo{color:#9ca3af}
.lb-empty td{color:#4b5563;font-size:11px;padding:18px 10px;text-align:center}

.exp-btn{display:inline-block;width:12px;text-align:center;font-size:9px;color:#60a5fa;margin-right:4px;cursor:pointer}
.toggle-btn{color:#38bdf8;cursor:pointer;font-size:10px;font-weight:bold;text-decoration:underline;user-select:none}
.toggle-btn:hover{color:#60a5fa}

/* ── Sortable column headers ──────────────────────────────────────── */
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:#9ca3af}
th.sort-on{color:#60a5fa}
th.sort-on::after{content:' ▼';font-size:8px;vertical-align:middle}
th.sort-on.asc::after{content:' ▲'}
</style>
</head>
<body>
<div id="boot-banner" style="display:none;background:#1c1108;border-bottom:1px solid #92400e;
     padding:6px 16px;font-size:12px;color:#fb923c;text-align:center;letter-spacing:.3px">
  ⏳ Bootstrap in progress — loading bar data &amp; warming up indicators. Alerts will appear once complete.
</div>
<header>
  <h1>Ross Pullback Scanner</h1>
  <span id="status">connecting…</span>
  <span id="count" style="margin-left:auto"></span>
</header>

<div id="tab-rvol">
  <div id="left-pane">
    <!-- Top Section: Default Screener (Top Volume) -->
    <div style="height:42%; display:flex; flex-direction:column; border-bottom:1px solid #1f2937; overflow:hidden">
      <div style="padding:6px 14px;background:#080d14;border-bottom:1px solid #1f2937;display:flex;align-items:center;flex-shrink:0">
        <span style="color:#60a5fa;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase">Default TV Screener</span>
      </div>
      <div class="scroller" style="flex:1;overflow-y:auto">
        <table id="screener-table" style="width:100%;border-collapse:collapse">
          <thead>
            <tr style="position:sticky;top:0;background:#080d14;z-index:5">
              <th class="sortable-screener sort-on" data-col="symbol" style="padding:6px 10px;color:#4b5563;font-weight:normal;font-size:10px;text-align:left;border-bottom:1px solid #1f2937;cursor:pointer;user-select:none">Symbol</th>
              <th class="sortable-screener" data-col="close" style="padding:6px 10px;color:#4b5563;font-weight:normal;font-size:10px;text-align:right;border-bottom:1px solid #1f2937;cursor:pointer;user-select:none">Price</th>
              <th class="sortable-screener" data-col="overnight_chg" style="padding:6px 10px;color:#4b5563;font-weight:normal;font-size:10px;text-align:right;border-bottom:1px solid #1f2937;cursor:pointer;user-select:none">Change%</th>
              <th class="sortable-screener" data-col="today_volume" style="padding:6px 10px;color:#4b5563;font-weight:normal;font-size:10px;text-align:right;border-bottom:1px solid #1f2937;cursor:pointer;user-select:none">Volume</th>
            </tr>
          </thead>
          <tbody id="screener-tb"></tbody>
        </table>
      </div>
    </div>

    <!-- Bottom Section: Live Rapid Momentum Alerts -->
    <div style="height:58%; display:flex; flex-direction:column; overflow:hidden">
      <div style="padding:6px 14px;background:#080d14;border-bottom:1px solid #1f2937;display:flex;align-items:center;flex-shrink:0;gap:12px">
        <span style="color:#facc15;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase">Live Rapid Momentum Alerts</span>
        <span style="font-size:10px;color:#94a3b8;margin-left:auto">Min Spike%:</span>
        <input id="alert-min-spike" type="number" step="0.1" value="0.0" style="width:50px;background:#1e293b;border:1px solid #475569;color:#fff;border-radius:3px;padding:1px 4px;font-size:11px;text-align:right" />
        <span id="alert-count" style="font-size:10px;color:#4b5563">0 alerts</span>
      </div>
      <div class="scroller" style="flex:1;overflow-y:auto">
        <table style="width:100%;border-collapse:collapse">
          <thead style="position:sticky;top:0;background:#080d14;z-index:5">
            <tr>
              <th class="sortable-alerts" data-col="ts" style="padding:6px 10px;text-align:left;color:#4b5563;font-weight:normal;border-bottom:1px solid #1f2937;font-size:10px;cursor:pointer;user-select:none">Time</th>
              <th class="sortable-alerts" data-col="symbol" style="padding:6px 10px;text-align:left;color:#4b5563;font-weight:normal;border-bottom:1px solid #1f2937;font-size:10px;cursor:pointer;user-select:none">Symbol</th>
              <th class="sortable-alerts" data-col="tf" style="padding:6px 10px;text-align:left;color:#4b5563;font-weight:normal;border-bottom:1px solid #1f2937;font-size:10px;cursor:pointer;user-select:none">TF</th>
              <th class="sortable-alerts" data-col="pct" style="padding:6px 10px;text-align:right;color:#4b5563;font-weight:normal;border-bottom:1px solid #1f2937;font-size:10px;cursor:pointer;user-select:none">Spike%</th>
            </tr>
          </thead>
          <tbody id="rapid-tb"></tbody>
        </table>
      </div>
    </div>
  </div>
  
  <div id="right-pane">
    <div style="padding:6px 14px;background:#080d14;border-bottom:1px solid #1f2937;display:flex;align-items:center;gap:10px">
      <span style="color:#60a5fa;font-size:11px;font-weight:bold;letter-spacing:.5px;text-transform:uppercase" id="chart-title">TradingView Live Chart</span>
      <div id="chart-tf-selector" style="display:none;margin-left:20px;display:flex;gap:4px">
        <button class="fb tf-btn active" id="tf-btn-1" onclick="changeChartTf(1)">1m</button>
        <button class="fb tf-btn" id="tf-btn-5" onclick="changeChartTf(5)">5m</button>
        <button class="fb tf-btn" id="tf-btn-15" onclick="changeChartTf(15)">15m</button>
      </div>
      <label style="margin-left:12px;color:#9ca3af;font-size:11px;display:flex;align-items:center;gap:4px;cursor:pointer">
        <input type="checkbox" id="premium-chart-chk" onchange="togglePremiumChart()" style="cursor:pointer">
        Premium Chart
      </label>
      <button id="chart-expand-btn" class="fb" style="margin-left:auto;font-size:10px;padding:2px 6px">Fullscreen Chart</button>
    </div>
    <div id="tv-placeholder" style="flex:1;display:flex;justify-content:center;align-items:center;color:#4b5563;font-family:monospace;font-size:12px;text-align:center">
      Select a stock row or alert from the list to load the TradingView chart
    </div>
    <div id="tv-widget-container" style="flex:1;width:100%;height:100%;display:none;overflow:hidden;background:#151924;position:relative">
      <div id="tv-widget-built-in" style="width:100%;height:100%;position:absolute;top:0;left:0"></div>
      <iframe id="tv-widget-premium" style="width:100%;height:100%;display:none;border:none;position:absolute;top:0;left:0"></iframe>
    </div>
  </div>
</div>

<script>
const ENABLE_PULLBACKS = true;
/* ================================================================
   State Variables
   ================================================================ */
const _today = new Date(Date.now() + 5.5 * 3600 * 1000).toISOString().slice(0, 10);

/* TradingView Widget & Iframe state */
let currentSymbol = null;
let currentTf = 1;
let usePremiumChart = false;

function togglePremiumChart() {
  usePremiumChart = document.getElementById('premium-chart-chk').checked;
  if (currentSymbol) {
    loadTVChart(currentSymbol, currentTf);
  }
}

function changeChartTf(tf) {
  if (!currentSymbol) return;
  loadTVChart(currentSymbol, tf);
}

function updateTfButtons(tf) {
  document.querySelectorAll('.tf-btn').forEach(btn => btn.classList.remove('active'));
  const activeBtn = document.getElementById(`tf-btn-${tf}`);
  if (activeBtn) activeBtn.classList.add('active');
}

function loadTVChart(symbol, tf) {
  currentSymbol = symbol;
  currentTf = tf;
  updateTfButtons(tf);
  
  const placeholder = document.getElementById('tv-placeholder');
  const container = document.getElementById('tv-widget-container');
  const builtInDiv = document.getElementById('tv-widget-built-in');
  const premiumIframe = document.getElementById('tv-widget-premium');
  
  document.getElementById('chart-title').textContent = `${symbol} — ${tf}m Chart`;
  document.getElementById('chart-tf-selector').style.display = 'flex';
  
  placeholder.style.display = 'none';
  container.style.display = 'flex';
  
  if (usePremiumChart) {
    builtInDiv.style.display = 'none';
    premiumIframe.style.display = 'block';
    premiumIframe.src = `https://in.tradingview.com/chart/?symbol=NSE:${symbol}&interval=${tf}`;
  } else {
    premiumIframe.style.display = 'none';
    builtInDiv.style.display = 'block';
    builtInDiv.innerHTML = '';
    
    let tvInterval = "1";
    if (tf === 5) tvInterval = "5";
    if (tf === 15) tvInterval = "15";
    
    new TradingView.widget({
      "autosize": true,
      "symbol": "NSE:" + symbol,
      "interval": tvInterval,
      "timezone": "Asia/Kolkata",
      "theme": "dark",
      "style": "1",
      "locale": "en",
      "enable_publishing": false,
      "hide_side_toolbar": false,
      "allow_symbol_change": true,
      "container_id": "tv-widget-built-in",
      "studies": [
        "RSI@tv-basicstudies",
        "MACD@tv-basicstudies",
        "MAExp@tv-basicstudies"
      ]
    });
  }
}


/* Toggle Chart Fullscreen */
let chartFullscreen = false;
document.getElementById('chart-expand-btn').addEventListener('click', () => {
  chartFullscreen = !chartFullscreen;
  const leftPane = document.getElementById('left-pane');
  const rightPane = document.getElementById('right-pane');
  const btn = document.getElementById('chart-expand-btn');
  
  if (chartFullscreen) {
    leftPane.style.display = 'none';
    rightPane.style.width = '100%';
    btn.textContent = 'Show List';
  } else {
    leftPane.style.display = 'flex';
    leftPane.style.width = '40%';
    rightPane.style.width = '60%';
    btn.textContent = 'Fullscreen Chart';
  }
});



let alerts = [];
let screenerData = [];
let screenerSortCol = 'today_volume';
let screenerSortDir = -1;
let alertsSortCol = 'ts';
let alertsSortDir = -1;

function fetchScreener() {
  return fetch('./api/tv-screener')
    .then(r => r.json())
    .then(rows => {
      screenerData = rows;
      renderScreener();
    });
}

function renderScreener() {
  const tb = document.getElementById('screener-tb');
  if (!tb) return;
  
  const sorted = [...screenerData].sort((a, b) => {
    let va = a[screenerSortCol];
    let vb = b[screenerSortCol];
    if (typeof va === 'string') return screenerSortDir * va.localeCompare(vb);
    return screenerSortDir * (va - vb);
  });
  
  tb.innerHTML = sorted.map(row => {
    return `<tr data-sym="${row.symbol}" style="cursor:pointer">
      <td style="padding:6px 10px;text-align:left"><b>${row.symbol}</b></td>
      <td style="padding:6px 10px;text-align:right">${row.close.toFixed(2)}</td> 
      <td style="padding:6px 10px;text-align:right" class="${row.overnight_chg >= 0 ? 'LONG' : 'SHORT'}">${row.overnight_chg >= 0 ? '+' : ''}${row.overnight_chg.toFixed(2)}%</td>
      <td style="padding:6px 10px;text-align:right">${(row.today_volume/1000000).toFixed(2)}M</td>
    </tr>`;
  }).join('');
  
  document.querySelectorAll('#screener-tb tr').forEach(row => {
    row.addEventListener('click', () => {
      document.querySelectorAll('#screener-tb tr, #rapid-tb tr').forEach(r => r.classList.remove('active-row'));
      row.classList.add('active-row');
      loadTVChart(row.dataset.sym, 1);
    });
    row.classList.toggle('active-row', row.dataset.sym === currentSymbol);
  });
}

document.querySelectorAll('.sortable-screener').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (screenerSortCol === col) {
      screenerSortDir *= -1;
    } else {
      screenerSortCol = col;
      screenerSortDir = -1;
    }
    document.querySelectorAll('.sortable-screener').forEach(h => {
      h.classList.remove('sort-on', 'asc');
    });
    th.classList.add('sort-on');
    if (screenerSortDir === 1) {
      th.classList.add('asc');
    }
    renderScreener();
  });
});

function okRapid(a) {
  return a.type === 'rapid';
}

function renderRapids() {
  const tb = document.getElementById('rapid-tb');
  if (!tb) return;
  
  const minSpikeVal = parseFloat(document.getElementById('alert-min-spike')?.value || '0.0');
  
  const rawRows = alerts.filter(okRapid);
  const filteredRows = rawRows.filter(a => {
    const pct = a.spike_pct || a.pct || 0.0;
    return pct >= minSpikeVal;
  });
  
  const sorted = [...filteredRows].sort((a, b) => {
    let va = a[alertsSortCol];
    let vb = b[alertsSortCol];
    if (alertsSortCol === 'pct') {
      va = a.spike_pct || a.pct || 0.0;
      vb = b.spike_pct || b.pct || 0.0;
    }
    if (typeof va === 'string') return alertsSortDir * va.localeCompare(vb);
    return alertsSortDir * (va - vb);
  });
  
  document.getElementById('alert-count').textContent = sorted.length + ' alerts';
  
  tb.innerHTML = sorted.map((a, i) => {
    return `<tr class="${i<3?'new':''}" data-sym="${a.symbol}" data-tf="${a.tf}" style="cursor:pointer">
      <td style="padding:6px 10px;text-align:left">${a.time_ist}</td>
      <td style="padding:6px 10px;text-align:left"><b>${a.symbol}</b></td>
      <td style="padding:6px 10px;text-align:left" class="tf">${a.tf}m</td>
      <td style="padding:6px 10px;text-align:right" class="LONG">${(a.spike_pct || a.pct || 0.0).toFixed(2)}%</td>
    </tr>`;
  }).join('');
  
  document.querySelectorAll('#rapid-tb tr').forEach(row => {
    row.addEventListener('click', () => {
      document.querySelectorAll('#screener-tb tr, #rapid-tb tr').forEach(r => r.classList.remove('active-row'));
      row.classList.add('active-row');
      loadTVChart(row.dataset.sym, parseInt(row.dataset.tf));
    });
    row.classList.toggle('active-row', row.dataset.sym === currentSymbol);
  });
}

document.querySelectorAll('.sortable-alerts').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    if (alertsSortCol === col) {
      alertsSortDir *= -1;
    } else {
      alertsSortCol = col;
      alertsSortDir = -1;
    }
    document.querySelectorAll('.sortable-alerts').forEach(h => {
      h.classList.remove('sort-on', 'asc');
    });
    th.classList.add('sort-on');
    if (alertsSortDir === 1) {
      th.classList.add('asc');
    }
    renderRapids();
  });
});

document.getElementById('alert-min-spike').addEventListener('input', () => {
  renderRapids();
});

function pollDashboardData() {
  fetchScreener()
    .then(() => {
      const s = document.getElementById('status');
      s.textContent = 'live'; s.className = 'live';
    })
    .catch(() => {
      const s = document.getElementById('status');
      s.textContent = 'connecting…'; s.className = '';
    });
    
  fetch('./api/alerts')
    .then(r => r.json())
    .then(d => {
      alerts = d;
      renderRapids();
    })
    .catch(() => {});
}
setInterval(pollDashboardData, 1000);

pollDashboardData();

function checkBootstrap() {
  fetch('./api/debug').then(r => r.json()).then(d => {
    const banner = document.getElementById('boot-banner');
    if (d.is_live) {
      banner.style.display = 'none';
    } else {
      banner.style.display = 'block';
      setTimeout(checkBootstrap, 3000);
    }
  }).catch(() => setTimeout(checkBootstrap, 5000));
}
checkBootstrap();
</script>
</body>
</html>
'''


def compute_ema(prices: list[float], period: int) -> list[float]:
    if not prices:
        return []
    ema = []
    alpha = 2.0 / (period + 1.0)
    current = prices[0]
    for p in prices:
        current = p * alpha + current * (1.0 - alpha)
        ema.append(current)
    return ema

def compute_rsi(prices: list[float], period: int = 14) -> list[float]:
    if len(prices) < 2:
        return [50.0] * len(prices)
    rsi = []
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    avg_gain = sum(d for d in deltas[:period] if d > 0) / period
    avg_loss = sum(-d for d in deltas[:period] if d < 0) / period
    
    for i in range(period):
        rsi.append(50.0)
        
    for i in range(period, len(prices)):
        d = deltas[i-1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi.append(100.0 - (100.0 / (1.0 + rs)))
    return rsi

def compute_macd(prices: list[float], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
    ema12 = compute_ema(prices, fast_period)
    ema26 = compute_ema(prices, slow_period)
    macd_line = []
    for e12, e26 in zip(ema12, ema26):
        macd_line.append(e12 - e26)
        
    signal_line = compute_ema(macd_line, signal_period)
    macd_hist = [m - s for m, s in zip(macd_line, signal_line)]
    return macd_line, signal_line, macd_hist

def create_app(alert_mgr: AlertManager,
               get_leaderboard: Callable[[], list[dict]],
               get_debug: Callable[[], dict] | None = None,
               rescan_ref: dict | None = None,
               get_peak_momentum: Callable[[], dict] | None = None,
               js_defaults: dict | None = None,
               title: str | None = None,
               extra_js: str | None = None) -> Flask:
    import re as _re
    _html = _HTML
    if title:
        _html = _html.replace('Ross Pullback Scanner', title, 2)  # <title> + <h1>
    if js_defaults:
        for var, val in js_defaults.items():
            _html = _re.sub(rf'(let {var}\s*=\s*)[\d.]+;', rf'\g<1>{val};', _html)
    if extra_js:
        _html = _html.replace('</script>\n</body>', extra_js + '\n</script>\n</body>')

    app = Flask(__name__)

    @app.route('/static/lightweight-charts.js')
    def static_lightweight_charts():
        _here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(_here, 'lightweight-charts.js')
        if os.path.exists(path):
            with open(path) as f:
                content = f.read()
            return Response(content, mimetype='application/javascript')
        return 'Not Found', 404

    @app.route('/static/tv.js')
    @app.route('/scanner/static/tv.js')
    def static_tv_js():
        _here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(_here, 'tv.js')
        if os.path.exists(path):
            with open(path) as f:
                content = f.read()
            return Response(content, mimetype='application/javascript')
        return 'Not Found', 404

    @app.route('/')
    @app.route('/scanner')
    @app.route('/scanner/')
    def index():
        html_content = _html.replace("const ENABLE_PULLBACKS = true;", f"const ENABLE_PULLBACKS = {str(Config.ENABLE_PULLBACKS).lower()};")
        resp = Response(html_content, mimetype='text/html')
        resp.headers['Cache-Control'] = 'no-store'
        return resp

    @app.route('/api/alerts')
    @app.route('/scanner/api/alerts')
    def api_alerts():
        data = alert_mgr.get_all()
        
        # Filter for today's date in IST (since scanner runs on Indian market schedule)
        from datetime import datetime, timezone, timedelta
        ist = datetime.now(tz=timezone(timedelta(hours=5, minutes=30)))
        today_str = ist.strftime("%Y-%m-%d")
        
        today_data = [d for d in data if d.get('date_iso') == today_str]

        if get_peak_momentum:
            pm = get_peak_momentum()
            for d in today_data:
                if 'peak_mom_pct' not in d:
                    mom_ev = pm.get(d.get('symbol', ''))
                    d['peak_mom_pct'] = mom_ev['pct'] if mom_ev else 0.0
        return jsonify(today_data)

    @app.route('/api/rvol-leaderboard')
    @app.route('/scanner/api/rvol-leaderboard')
    def api_rvol_leaderboard():
        return jsonify(get_leaderboard())

    @app.route('/api/debug')
    @app.route('/scanner/api/debug')
    def api_debug():
        if get_debug:
            return jsonify(get_debug())
        return jsonify({'error': 'no debug fn'})

    @app.route('/api/rescan', methods=['POST'])
    @app.route('/scanner/api/rescan', methods=['POST'])
    def api_rescan():
        fn = rescan_ref.get('fn') if rescan_ref else None
        if fn is None:
            return jsonify({'error': 'not ready — bootstrap still running'}), 503
        n = fn()
        return jsonify({'new_symbols': n})

    @app.route('/api/bars')
    def api_bars():
        symbol = request.args.get('symbol')
        tf = int(request.args.get('tf', 1))
        
        # Load cache
        from config import Config
        cache_path = os.path.join(Config.BARS_CACHE_DIR, f"{symbol}.json")
        if not os.path.exists(cache_path):
            return jsonify([])
            
        try:
            with open(cache_path) as f:
                cache = json.load(f)
            dates = sorted(cache.keys())
            if not dates:
                return jsonify([])
            # Combine the last 3 trading days of data for chart context
            recent_dates = dates[-3:]
            bars_1m = []
            for d in recent_dates:
                bars_1m.extend(cache[d])
            
            # Resample bars
            if tf == 1:
                resampled = [{'time': b['ts'], 'open': b['open'], 'high': b['high'], 'low': b['low'], 'close': b['close']} for b in bars_1m]
            else:
                groups = {}
                for b in bars_1m:
                    gts = (b['ts'] // (tf * 60)) * (tf * 60)
                    if gts not in groups:
                        groups[gts] = {'time': gts, 'open': b['open'], 'high': b['high'], 'low': b['low'], 'close': b['close']}
                    else:
                        g = groups[gts]
                        g['high']  = max(g['high'], b['high'])
                        g['low']   = min(g['low'], b['low'])
                        g['close'] = b['close']
                resampled = sorted(groups.values(), key=lambda x: x['time'])
                
            # Compute technical indicators (EMA, RSI, MACD)
            if resampled:
                closes = [x['close'] for x in resampled]
                ema20 = compute_ema(closes, 20)
                ema50 = compute_ema(closes, 50)
                rsi = compute_rsi(closes, 14)
                macd_line, macd_signal, macd_hist = compute_macd(closes)
                
                for idx, r in enumerate(resampled):
                    r['ema20']     = round(ema20[idx], 2)     if idx < len(ema20) else None
                    r['ema50']     = round(ema50[idx], 2)     if idx < len(ema50) else None
                    r['rsi']       = round(rsi[idx], 2)       if idx < len(rsi) else 50.0
                    r['macd']      = round(macd_line[idx], 3)   if idx < len(macd_line) else 0.0
                    r['macd_sig']  = round(macd_signal[idx], 3) if idx < len(macd_signal) else 0.0
                    r['macd_hist'] = round(macd_hist[idx], 3)   if idx < len(macd_hist) else 0.0

            # Shift timestamps by +5.5 hours (19800 seconds) to display in IST on Lightweight Charts
            for b in resampled:
                b['time'] += 19800
                
            return jsonify(resampled)
        except Exception as e:
            logger.error("Error serving api/bars for %s: %s", symbol, e)
            return jsonify([])

    @app.route('/stream')
    def stream():
        q = alert_mgr.subscribe()

        def gen():
            try:
                yield 'data: connected\n\n'
                last_ka = time.time()
                while True:
                    if q:
                        yield f'event: alert\ndata: {json.dumps(q.popleft())}\n\n'
                        last_ka = time.time()
                    else:
                        time.sleep(0.25)
                        if time.time() - last_ka >= 15:
                            yield ': keepalive\n\n'
                            last_ka = time.time()
            finally:
                alert_mgr.unsubscribe(q)

        return Response(
            gen(), mimetype='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'}
        )

    return app

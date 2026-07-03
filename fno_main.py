"""
FNO Micro-Pullback Scanner
===========================
MACD micro-pullback signals on 15 / 30 / 60-min bars for the NSE F&O universe.
MOM (momentum) signals detected on 1-min, 5-min and 15-min bars.
1 month of bar data for accurate MACD warmup; signals from the past 7 trading
days shown on the dashboard calendar.

Data lifecycle:
  Bootstrap: loads last 22 trading days from disk cache; only calls the API for
  days not yet cached.  The live feed saves today's bars to cache on every poll,
  so tomorrow's bootstrap finds yesterday already cached (no extra API call).

Dashboard:  http://localhost:5051
"""
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta

from dhanhq import DhanContext

from fno_config import FnoConfig

# Swap Config in dhan_feed before any transitively-dependent imports
import dhan_feed as _df
_df.Config = FnoConfig

from dhan_feed import bootstrap, LiveFeed, _load_cache, _trading_days, _resample
from strategy_engine import StrategyEngine, BarRecord, Alert
from indicators import IndicatorSet
from alert_manager import AlertManager
from dashboard import create_app
from fno_universe import build_fno_universe

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-5s %(name)s — %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Timeframes on which momentum windows are computed
_MOM_TFS  = frozenset({1, 5, 15})
# Timeframes on which the MACD micro-pullback engine runs
_MACD_TFS = frozenset({15, 30, 60})


def main():
    errors = FnoConfig.validate()
    if errors:
        for e in errors:
            logger.error('Config: %s', e)
        sys.exit(1)

    _here = os.path.dirname(os.path.abspath(__file__))
    ctx   = DhanContext(FnoConfig.DHAN_CLIENT_ID, FnoConfig.DHAN_ACCESS_TOKEN)

    # Separate alert directory so FNO alerts don't mix with the intraday scanner
    _alerts_dir = os.path.join(_here, 'fno_alerts')
    os.makedirs(_alerts_dir, exist_ok=True)
    alert_mgr = AlertManager(persist_dir=_alerts_dir)

    # ── state ─────────────────────────────────────────────────────────────────
    ind_sets:  dict[tuple, IndicatorSet]   = {}
    engines:   dict[tuple, StrategyEngine] = {}
    bootstrap_last_ts: dict[tuple, int]    = {}

    is_live = False

    def _trading_day() -> date:
        d = date.today()
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    _active_date     = _trading_day()
    _cutoff_date     = _active_date - timedelta(days=FnoConfig.STRATEGY_LOOKBACK_DAYS)
    _strategy_cutoff = int(datetime(
        _cutoff_date.year, _cutoff_date.month, _cutoff_date.day,
    ).timestamp())

    today_volumes: dict[str, float] = {}
    today_bars:    dict[str, deque] = {}
    today_highs:   dict[str, float] = {}
    today_lows:    dict[str, float] = {}
    today_opens:   dict[str, float] = {}
    prev_closes:   dict[str, float] = {}
    overnight_chg: dict[str, float] = {}

    # MOM tracking keyed by (symbol, tf) to support 1/5/15-min independently
    bar_history:    dict[tuple, deque] = {}
    mom_last_fired: dict[tuple, int]   = {}
    peak_momentum:  dict[str, dict]    = {}   # best MOM event per symbol (across all TFs)
    moves_log:      dict[str, list]    = {}   # symbol → [{ts, pct, window, tf}, ...]

    alerted_symbols: set[str] = set()

    _bootstrap_prev_close: dict[str, float] = {}

    # ── helpers ───────────────────────────────────────────────────────────────
    def _ist_time(ts: int) -> str:
        m = (ts // 60 + 330) % (24 * 60)
        return f"{m // 60:02d}:{m % 60:02d}"

    def _ts_to_date_label(ts: int) -> str:
        from datetime import timezone, timedelta as _td
        ist = datetime.fromtimestamp(ts, tz=timezone(_td(hours=5, minutes=30)))
        return ist.strftime("%d-%b")

    # ── leaderboard ───────────────────────────────────────────────────────────
    def compute_leaderboard() -> list[dict]:
        if _active_date == date.today():
            now_ist_min = (int(time.time()) // 60 + 330) % (24 * 60)
            elapsed_min = min(max(now_ist_min - 555, 1), 375)
        else:
            elapsed_min = 375

        rows = []
        for symbol in list(alerted_symbols):
            today_vol = today_volumes.get(symbol, 0.0)
            o_chg     = overnight_chg.get(symbol, 0.0)

            open_p        = today_opens.get(symbol, 0.0)
            day_range_pct = 0.0
            if open_p > 0:
                h = today_highs.get(symbol, 0.0)
                l = today_lows.get(symbol, 0.0)
                if h > l > 0:
                    day_range_pct = round((h - l) / open_p * 100, 2)

            bars      = today_bars.get(symbol)
            buyer_pct = 0.5
            if bars:
                tv = tb = 0.0
                for bar_d in bars:
                    vol_b = bar_d.get('volume', 0.0)
                    rng   = max(bar_d.get('high', 0.0) - bar_d.get('low', 0.0), 1e-6)
                    tv   += vol_b
                    tb   += vol_b * max(bar_d.get('close', 0.0) - bar_d.get('low', 0.0), 0.0) / rng
                if tv > 0:
                    buyer_pct = tb / tv

            base = {
                'symbol':        symbol,
                'bar_rvol':      0.0,
                'overnight_chg': o_chg,
                'rs_coverage':   0.0,
                'rs_elapsed':    0,
                'day_range_pct': day_range_pct,
                'cb_ratio':      0.0,
                'cb_pct':        0.0,
                'cb_time':       '',
                'ratio':         0.0,
                'today_vol':     round(today_vol),
                'avg_daily':     0,
                'elapsed':       elapsed_min,
                'buyer_pct':     round(buyer_pct, 3),
                'seller_pct':    round(1.0 - buyer_pct, 3),
            }

            moves = moves_log.get(symbol, [])
            if not moves:
                mom_ev = peak_momentum.get(symbol)
                if mom_ev:
                    moves = [mom_ev]
                else:
                    moves = [{'ts': 0, 'pct': 0.0, 'window': 0, 'tf': 1}]

            for mv in moves:
                row = dict(base)
                row['peak_mom_pct']  = mv['pct']
                row['peak_mom_win']  = mv.get('window', 0)
                row['peak_mom_time'] = _ist_time(mv['ts']) if mv.get('ts') else ''
                row['peak_mom_tf']   = mv.get('tf', 1)
                row['peak_mom_date'] = _ts_to_date_label(mv['ts']) if mv.get('ts') else ''
                rows.append(row)

        rows.sort(key=lambda x: x['overnight_chg'], reverse=True)
        return rows

    # ── alert callback (MACD) ─────────────────────────────────────────────────
    def on_alert(alert: Alert):
        if not is_live:
            return
        if date.fromtimestamp(alert.ts) < _cutoff_date:
            return
        alert.today_volume = today_volumes.get(alert.symbol, 0.0)
        alert.rel_volume   = 0.0
        alerted_symbols.add(alert.symbol)
        alert_mgr.add(alert)

    # ── MOM detection (shared logic across TFs) ───────────────────────────────
    def _check_mom(symbol: str, tf: int, bar_ts: int, open_p: float, high: float, low: float):
        """Update bar_history for (symbol, tf) and fire a live MOM alert if threshold met."""
        mom_key = (symbol, tf)
        if mom_key not in bar_history:
            bar_history[mom_key] = deque(maxlen=5)
        bh = bar_history[mom_key]
        # Reset on day boundary — prevents overnight gaps inflating the window range
        if bh and date.fromtimestamp(bh[-1]['ts']) != date.fromtimestamp(bar_ts):
            bh.clear()
        bh.append({'open': open_p, 'high': high, 'low': low, 'ts': bar_ts})

        hist     = list(bar_history[mom_key])
        best_pct = 0.0
        best_win = 1
        for n in range(1, len(hist) + 1):
            win = hist[-n:]
            ref = win[0]['open']
            if ref > 0:
                move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref * 100
                if move > best_pct:
                    best_pct = move
                    best_win = n

        bar_date = date.fromtimestamp(bar_ts)
        if bar_date == _active_date:
            if best_pct > peak_momentum.get(symbol, {}).get('pct', 0.0):
                peak_momentum[symbol] = {'ts': bar_ts, 'pct': round(best_pct, 2), 'window': best_win, 'tf': tf}
            if best_pct >= FnoConfig.MOVER_MIN_PCT:
                alerted_symbols.add(symbol)

            if is_live and best_pct >= FnoConfig.MOVER_MIN_PCT:
                if bar_ts - mom_last_fired.get(mom_key, 0) >= 300:
                    mom_last_fired[mom_key] = bar_ts
                    now_ts = int(time.time())
                    lag_s  = now_ts - (bar_ts + tf * 60)
                    logger.info('MOM %s %dm %.2f%% bar=%s lag=%ds',
                                symbol, tf, best_pct, _ist_time(bar_ts), lag_s)
                    if symbol not in moves_log:
                        moves_log[symbol] = []
                    dup_key = (bar_ts, tf)
                    if not any((m['ts'], m.get('tf', 1)) == dup_key for m in moves_log[symbol]):
                        moves_log[symbol].append(
                            {'ts': bar_ts, 'pct': round(best_pct, 2), 'window': best_win, 'tf': tf}
                        )

        return best_pct

    # ── bar callback ──────────────────────────────────────────────────────────
    def on_bar(symbol: str, tf: int, bar):
        nonlocal is_live
        b      = bar if isinstance(bar, dict) else bar.__dict__
        bar_ts = b.get('ts', b.get('timestamp', 0))
        high   = b.get('high',   0.0)
        low    = b.get('low',    0.0)
        open_p = b.get('open',   0.0)
        close  = b.get('close',  0.0)
        vol    = b.get('volume', 0.0)

        # ── 1-min intraday state ─────────────────────────────────────────────
        if tf == 1:
            bar_date      = date.fromtimestamp(bar_ts)
            is_active_bar = (bar_date == _active_date)

            if is_active_bar:
                if is_live:
                    today_volumes[symbol] = today_volumes.get(symbol, 0.0) + vol
                if symbol not in today_bars:
                    today_bars[symbol] = deque(maxlen=10)
                today_bars[symbol].append(b)

                if symbol not in today_opens:                 today_opens[symbol] = open_p
                if high > today_highs.get(symbol, 0.0):      today_highs[symbol] = high
                if symbol not in today_lows or low < today_lows[symbol]:
                    today_lows[symbol] = low

                prev_c = prev_closes.get(symbol, 0.0)
                if prev_c > 0 and close > 0:
                    overnight_chg[symbol] = round((close - prev_c) / prev_c * 100, 2)
            else:
                if not is_live:
                    _bootstrap_prev_close[symbol] = close

        # ── MOM detection (1-min, 5-min, 15-min) ────────────────────────────
        if tf in _MOM_TFS:
            _check_mom(symbol, tf, bar_ts, open_p, high, low)
            if tf not in _MACD_TFS:
                return  # 1-min and 5-min: MOM only, no MACD engine

        # ── MACD strategy (15-min, 30-min, 60-min) ──────────────────────────
        if tf not in _MACD_TFS:
            return

        key = (symbol, tf)
        if key not in ind_sets:
            ind_sets[key] = IndicatorSet()
            engines[key]  = StrategyEngine(symbol, tf, on_alert)

        vals = ind_sets[key].update(bar)
        if vals is None:
            return

        if bar_ts < _strategy_cutoff:
            if bar_ts > bootstrap_last_ts.get(key, 0):
                bootstrap_last_ts[key] = bar_ts
            return

        rec = BarRecord(
            ts=bar_ts,
            open=open_p, high=high, low=low, close=close, volume=vol,
            macd=vals['macd'],   signal=vals['signal'],
            ema50=vals['ema50'], rsi=vals['rsi'],
        )
        engines[key].update(rec)
        if bar_ts > bootstrap_last_ts.get(key, 0):
            bootstrap_last_ts[key] = bar_ts

    # ── dashboard ─────────────────────────────────────────────────────────────
    _rescan_ref: dict = {}

    _FNO_MOVERS_JS = r"""
// FNO Scanner — Movers tab: remove Vol/Overnight/CumRVOL, add TF filter + Date column
(function(){
  var lbTfFilter = 0;   // 0 = All

  // Replace toolbar (MOM filter, TF filter, Rescan, Date)
  document.getElementById('rvol-toolbar').innerHTML = [
    '<span class="lb-title">Movers — F&amp;O alerted stocks</span>',
    '<span class="fl" style="margin-left:12px">TF:</span>',
    '<button class="fb on" data-lbtf="0">All</button>',
    '<button class="fb" data-lbtf="1">1m</button>',
    '<button class="fb" data-lbtf="5">5m</button>',
    '<button class="fb" data-lbtf="15">15m</button>',
    '<span class="fl" style="margin-left:12px">MOM ≥</span>',
    '<input id="mom-input" type="number" min="0" step="0.5" value="0" class="num-in">',
    '<span class="fl">%</span>',
    '<button id="rescan-btn" class="fb" style="margin-left:8px">↺ Rescan</button>',
    '<span class="fl" style="margin-left:12px">Date:</span>',
    '<input id="rvol-date" type="date" class="date-in">',
    '<span id="lb-count"></span>',
    '<span id="lb-updated"></span>',
  ].join('');

  document.querySelectorAll('[data-lbtf]').forEach(function(btn){
    btn.addEventListener('click', function(){
      document.querySelectorAll('[data-lbtf]').forEach(function(b){ b.classList.remove('on'); });
      btn.classList.add('on');
      lbTfFilter = parseInt(btn.dataset.lbtf, 10);
      renderLeaderboard();
    });
  });
  document.getElementById('mom-input').value = lbMinMom;
  document.getElementById('mom-input').addEventListener('input', function(e){
    lbMinMom = parseFloat(e.target.value) || 0; renderLeaderboard();
  });
  document.getElementById('rescan-btn').addEventListener('click', function(){
    var btn = document.getElementById('rescan-btn');
    btn.textContent = '↺ scanning…'; btn.disabled = true;
    fetch('./api/rescan',{method:'POST'}).then(function(r){return r.json();})
      .then(function(d){
        btn.textContent = d.new_symbols>0 ? '↺ +'+d.new_symbols+' found' : '↺ done';
        setTimeout(function(){btn.textContent='↺ Rescan';btn.disabled=false;},3000);
        fetchLeaderboard();
      }).catch(function(){btn.textContent='↺ Rescan';btn.disabled=false;});
  });
  var rdInput = document.getElementById('rvol-date');
  rdInput.value = _today;
  rdInput.addEventListener('change', function(e){
    rvolDateFilter = e.target.value || _today;
    if (currentTab === 'rvol') applyRvolDate();
  });

  // Replace table header: # | Symbol | TF | Momentum | Date | Time | Day Rng %
  document.querySelector('#lb-table thead tr').innerHTML = [
    '<th style="width:22px">#</th>',
    '<th class="sortable" data-tbl="lb" data-col="symbol">Symbol</th>',
    '<th class="sortable" data-tbl="lb" data-col="peak_mom_tf">TF</th>',
    '<th class="sortable sort-on" data-tbl="lb" data-col="peak_mom_pct">Momentum</th>',
    '<th class="sortable" data-tbl="lb" data-col="peak_mom_date">Date</th>',
    '<th class="sortable" data-tbl="lb" data-col="peak_mom_time">Time</th>',
    '<th class="sortable" data-tbl="lb" data-col="day_range_pct">Day Rng %</th>',
  ].join('');
  lbSortCol = 'peak_mom_pct'; lbSortDir = -1;
  document.querySelectorAll('#lb-table th.sortable').forEach(function(th){
    th.addEventListener('click', function(){
      lbSortDir = (lbSortCol===th.dataset.col) ? -lbSortDir : -1;
      lbSortCol = th.dataset.col;
      markSortHeader('lb', lbSortCol, lbSortDir);
      renderLeaderboard();
    });
  });
  markSortHeader('lb', lbSortCol, lbSortDir);

  // Override renderLeaderboard — filter by TF and MOM, show TF + Date columns
  window.renderLeaderboard = function(){
    if (currentTab !== 'rvol') return;
    var filtered = lbData.filter(function(r){
      if ((r.peak_mom_pct||0) < lbMinMom) return false;
      if (lbTfFilter !== 0 && (r.peak_mom_tf||1) !== lbTfFilter) return false;
      return true;
    });
    var rows = applySort(filtered, lbSortCol, lbSortDir);
    document.getElementById('lb-count').textContent = rows.length + ' symbols';
    var tbody = document.getElementById('lb-tbody');
    if (!rows.length){
      tbody.innerHTML = '<tr class="lb-empty"><td colspan="7">No alerts yet — populates as signals fire</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(function(r, i){
      var tf = r.peak_mom_tf ? r.peak_mom_tf+'m' : '—';
      var momStr = r.peak_mom_pct
        ? '<span class="'+(r.peak_mom_pct>=3?'ratio-hi':r.peak_mom_pct>=1.5?'ratio-md':'ratio-lo')+'">'+r.peak_mom_pct.toFixed(2)+'%</span>'
          +' <span style="color:#4b5563;font-size:10px">'+r.peak_mom_win+'b</span>'
        : '<span style="color:#1f2937">—</span>';
      return '<tr>'
        +'<td style="color:#4b5563;font-size:10px">'+(i+1)+'</td>'
        +'<td><b>'+r.symbol+'</b></td>'
        +'<td style="color:#38bdf8;font-size:11px">'+tf+'</td>'
        +'<td>'+momStr+'</td>'
        +'<td style="color:#60a5fa;font-size:11px">'+(r.peak_mom_date||'—')+'</td>'
        +'<td style="color:#60a5fa;font-size:11px">'+(r.peak_mom_time||'—')+'</td>'
        +'<td style="color:#9ca3af">'+(r.day_range_pct>0?r.day_range_pct.toFixed(2)+'%':'—')+'</td>'
        +'</tr>';
    }).join('');
    document.getElementById('lb-updated').textContent = 'updated '+new Date().toTimeString().slice(0,5);
  };
})();
"""

    app = create_app(
        alert_mgr, compute_leaderboard,
        rescan_ref=_rescan_ref,
        get_peak_momentum=lambda: peak_momentum,
        js_defaults={'lbMinVol': 0, 'lbMinMom': 0},
        title='FNO Scanner',
        extra_js=_FNO_MOVERS_JS,
    )
    logger.info('Dashboard → http://%s:%d', FnoConfig.DASHBOARD_HOST, FnoConfig.DASHBOARD_PORT)

    flask_thread = threading.Thread(
        target=lambda: app.run(
            host=FnoConfig.DASHBOARD_HOST, port=FnoConfig.DASHBOARD_PORT,
            threaded=True, use_reloader=False,
        ),
        daemon=True, name='FnoDashboard',
    )
    flask_thread.start()
    time.sleep(1)

    # ── universe ──────────────────────────────────────────────────────────────
    symbols = build_fno_universe()
    if not symbols:
        logger.error('FNO universe empty — check Dhan scrip master connectivity')
        sys.exit(1)
    logger.info('FNO universe: %d symbols loaded', len(symbols))

    # ── Phase 1: populate Movers from today's disk cache ─────────────────────
    def _scan_today_bars():
        """Quick startup scan: read today's cached 1-min bars, resample, run MOM on 1/5/15-min."""
        today = _active_date.isoformat()
        local_last_fired: dict[tuple, int] = {}   # (name, tf) -> last fired ts

        for sec in symbols:
            name    = sec['symbol']
            bars_1m = sorted(_load_cache(name).get(today, []), key=lambda x: x['ts'])
            if not bars_1m:
                continue

            scan_events: list[dict] = []

            # OHLCV aggregates from 1-min bars
            vol_total = t_high = t_open = t_close = 0.0
            t_low = float('inf')
            for b in bars_1m:
                vol    = b.get('volume', 0.0)
                high   = b.get('high',   0.0)
                low    = b.get('low',    0.0)
                open_p = b.get('open',   0.0)
                close  = b.get('close',  0.0)
                vol_total += vol
                if t_open == 0.0:           t_open = open_p
                if high > t_high:           t_high = high
                if low > 0 and low < t_low: t_low  = low
                t_close = close

            # MOM scan on 1-min, 5-min, 15-min
            for tf_scan in (1, 5, 15):
                bars_tf = bars_1m if tf_scan == 1 else _resample(bars_1m, tf_scan)
                bh: deque = deque(maxlen=5)
                for b in bars_tf:
                    ts     = b.get('ts',   0)
                    open_p = b.get('open', 0.0)
                    high   = b.get('high', 0.0)
                    low    = b.get('low',  0.0)
                    bh.append({'open': open_p, 'high': high, 'low': low, 'ts': ts})
                    hist     = list(bh)
                    best_pct = 0.0
                    best_win = 1
                    for n in range(1, len(hist) + 1):
                        win = hist[-n:]
                        ref = win[0]['open']
                        if ref > 0:
                            move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref * 100
                            if move > best_pct:
                                best_pct = move
                                best_win = n
                    if best_pct > peak_momentum.get(name, {}).get('pct', 0.0):
                        peak_momentum[name] = {'ts': ts, 'pct': round(best_pct, 2), 'window': best_win, 'tf': tf_scan}
                    if best_pct >= FnoConfig.MOVER_MIN_PCT:
                        alerted_symbols.add(name)
                    mom_key = (name, tf_scan)
                    if best_pct >= FnoConfig.MOVER_MIN_PCT and ts - local_last_fired.get(mom_key, 0) >= 300:
                        local_last_fired[mom_key] = ts
                        scan_events.append({'ts': ts, 'pct': round(best_pct, 2), 'window': best_win, 'tf': tf_scan})

            # Merge: only add (ts, tf) pairs not already in moves_log
            existing = {(m['ts'], m.get('tf', 1)) for m in moves_log.get(name, [])}
            if name not in moves_log:
                moves_log[name] = []
            for ev in scan_events:
                k = (ev['ts'], ev.get('tf', 1))
                if k not in existing:
                    moves_log[name].append(ev)
                    existing.add(k)

            if vol_total > 0:                        today_volumes[name] = vol_total
            if t_high > 0:                           today_highs[name]   = t_high
            if t_low < float('inf') and t_low > 0:  today_lows[name]    = t_low
            if t_open > 0 and name not in today_opens: today_opens[name] = t_open
            prev_c = prev_closes.get(name, 0.0)
            if prev_c > 0 and t_close > 0:
                overnight_chg[name] = round((t_close - prev_c) / prev_c * 100, 2)

    _scan_today_bars()
    logger.info('Phase 1 complete — %d symbols with data, %d with MOM signals',
                len(today_volumes), len(alerted_symbols))

    # ── Phase 2: full bootstrap (22 days) ────────────────────────────────────
    _bootstrap_done = threading.Event()

    def _backfill_history():
        """Run MOM (1/5/15-min) and MACD (15/30/60-min) for the past 7 trading days."""
        all_days  = _trading_days(FnoConfig.HISTORY_DAYS)
        today_str = date.today().isoformat()
        past_days = sorted(d for d in all_days if d < today_str)[-FnoConfig.STRATEGY_LOOKBACK_DAYS:]
        if not past_days:
            logger.info('Backfill: no past days in cache')
            return

        past_days_set = set(past_days)
        logger.info('Backfill: %d past days (%s → %s)', len(past_days), past_days[0], past_days[-1])

        for sec in symbols:
            name = sec['symbol']
            try:
                cache   = _load_cache(name)
                bars_1m = sorted(
                    [b for day in all_days for b in cache.get(day, [])],
                    key=lambda x: x['ts'],
                )
                if not bars_1m:
                    continue

                # MOM on 1-min, 5-min, 15-min for past days
                for tf_mom in (1, 5, 15):
                    bars_tf     = bars_1m if tf_mom == 1 else _resample(bars_1m, tf_mom)
                    bh: deque   = deque(maxlen=5)
                    mom_last_bf = 0   # last fired ts for this symbol/TF combo

                    for b in bars_tf:
                        ts    = b.get('ts', 0)
                        b_day = date.fromtimestamp(ts).isoformat()
                        # Reset on day boundary to prevent gap-up/gap-down inflating range
                        if bh and date.fromtimestamp(bh[-1]['ts']).isoformat() != b_day:
                            bh.clear()
                        bh.append({
                            'open': b.get('open', 0.0),
                            'high': b.get('high', 0.0),
                            'low':  b.get('low',  0.0),
                            'ts':   ts,
                        })
                        if b_day not in past_days_set:
                            continue
                        hist     = list(bh)
                        best_pct = 0.0
                        best_win = 1
                        for n in range(1, len(hist) + 1):
                            win = hist[-n:]
                            ref = win[0]['open']
                            if ref > 0:
                                move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref * 100
                                if move > best_pct:
                                    best_pct = move
                                    best_win = n
                        if best_pct >= FnoConfig.MOVER_MIN_PCT and ts - mom_last_bf >= 300:
                            mom_last_bf = ts
                            alert_mgr.add_historical_event({
                                'alert_type': 'MOM',
                                'symbol':      name,
                                'tf':          tf_mom,
                                'pct':         round(best_pct, 2),
                                'window':      best_win,
                                'ts':          ts,
                                'time_ist':    _ist_time(ts),
                                'date_ist':    _ts_to_date_label(ts),
                                'today_volume': 0.0,
                                '_key':        f"{name}:MOM:{tf_mom}:{ts}",
                            }, b_day)

                # MACD on 15/30/60-min for past days
                for tf in (15, 30, 60):
                    tf_bars        = _resample(bars_1m, tf)
                    ind            = IndicatorSet()
                    alerts_by_day: dict[str, list] = {}

                    def _cb(a: Alert, _dst=alerts_by_day, _pds=past_days_set):
                        a_day = date.fromtimestamp(a.ts).isoformat()
                        if a_day in _pds:
                            _dst.setdefault(a_day, []).append(a)

                    engine = StrategyEngine(name, tf, _cb)
                    for b_dict in tf_bars:
                        b_dict = b_dict if isinstance(b_dict, dict) else b_dict.__dict__
                        vals   = ind.update(b_dict)
                        if vals is None:
                            continue
                        rec = BarRecord(
                            ts=b_dict.get('ts', 0),
                            open=b_dict.get('open', 0.0),  high=b_dict.get('high', 0.0),
                            low=b_dict.get('low',  0.0),   close=b_dict.get('close', 0.0),
                            volume=b_dict.get('volume', 0.0),
                            macd=vals['macd'],   signal=vals['signal'],
                            ema50=vals['ema50'], rsi=vals['rsi'],
                        )
                        engine.update(rec)

                    for day_str, day_alerts in alerts_by_day.items():
                        for alert in day_alerts:
                            alert_mgr.add_historical(alert, day_str)

            except Exception as e:
                logger.debug('Backfill %s: %s', name, e)

        logger.info('Backfill complete — %d total alerts', len(alert_mgr.get_all()))

    def _run_bootstrap():
        nonlocal is_live
        logger.info('Bootstrap: %d symbols × %d days (1-min data → resample to 5/15/30/60-min)',
                    len(symbols), FnoConfig.HISTORY_DAYS)
        try:
            bootstrap(ctx, symbols, on_bar)
        except Exception as e:
            logger.warning('Bootstrap incomplete (%s) — continuing with partial state', e)

        prev_closes.update(_bootstrap_prev_close)
        _bootstrap_prev_close.clear()
        logger.info('Prev closes ready: %d symbols', len(prev_closes))

        is_live = True
        logger.info('Phase 2 complete — MACD alerts active, starting live feed')
        _bootstrap_done.set()
        threading.Thread(target=_backfill_history, daemon=True, name='FnoBackfill').start()

    threading.Thread(target=_run_bootstrap, daemon=True, name='FnoBootstrap').start()
    logger.info('Bootstrap running in background — Movers tab live from Phase 1 cache')

    try:
        while not _bootstrap_done.is_set():
            _bootstrap_done.wait(timeout=1.0)
    except KeyboardInterrupt:
        logger.info('Shutting down during bootstrap...')
        return

    # ── rescan (on-demand via dashboard button) ───────────────────────────────
    def rescan_today() -> int:
        before = set(alerted_symbols)
        _scan_today_bars()
        n_new = len(alerted_symbols - before)
        logger.info('Rescan: %d new symbols (total: %d)', n_new, len(alerted_symbols))
        return n_new

    _rescan_ref['fn'] = rescan_today

    # ── live feed ─────────────────────────────────────────────────────────────
    # All FNO symbols are always "active" — poll all every cycle (~20 s for 200 stocks)
    feed = LiveFeed(ctx, symbols, on_bar,
                    priority_fn=lambda: {s['symbol'] for s in symbols})
    feed._last_ts.update(bootstrap_last_ts)
    logger.info('Live feed seeded with %d timestamps, polling %d symbols every cycle',
                len(bootstrap_last_ts), len(symbols))
    feed.start()

    # ── heartbeat ─────────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(300)
            logger.info('Heartbeat — engines: %d  alerts: %d  leaderboard: %d',
                        len(ind_sets), len(alert_mgr.get_all()), len(alerted_symbols))
    except KeyboardInterrupt:
        logger.info('Shutting down...')
        feed.stop()


if __name__ == '__main__':
    main()

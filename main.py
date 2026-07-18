"""
Ross Cameron Micro-Pullback Scanner
====================================
Scans the NSE equity universe across 1/3/5/15-min timeframes for
MACD micro-pullback setups. Requires a Dhan API account with live
market data subscription.

Usage:
    python main.py

Dashboard:
    http://localhost:5050
"""
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from dataclasses import asdict
from datetime import date, datetime, timedelta

from dhanhq import DhanContext

from config import Config
from strategy_engine import Alert
from universe import build_universe
from dhan_feed import LiveFeed, _load_cache, _trading_days, _resample
from alert_manager import AlertManager
from rvol import RvolTracker
from dashboard import create_app

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-5s %(name)s — %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--replay', action='store_true', help="Replay today's session from 1m candles on startup")
    args, unknown = parser.parse_known_args()

    errors = Config.validate()
    if errors:
        for e in errors:
            logger.error('Config: %s', e)
        sys.exit(1)

    _here = os.path.dirname(os.path.abspath(__file__))
    ctx       = DhanContext(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)
    alert_mgr = AlertManager(persist_dir=_here)

    # ── state stores ─────────────────────────────────────────────────────────
    ind_sets: dict[tuple, IndicatorSet]   = {}
    engines:  dict[tuple, StrategyEngine] = {}
    # Last bar timestamp fed to each strategy engine during bootstrap.
    # Seeded into LiveFeed._last_ts so the live feed never replays bootstrap bars.
    bootstrap_last_ts: dict[tuple, int] = {}

    is_live = False

    def _trading_day() -> date:
        """Current trading day. Steps back over weekends only.
        Before market open the leaderboard will simply be empty until bars arrive.
        """
        d = date.today()
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    _active_date     = _trading_day()
    _cutoff_date     = _active_date - timedelta(days=4)
    _strategy_cutoff = int(datetime(
        _cutoff_date.year, _cutoff_date.month, _cutoff_date.day
    ).timestamp())
    if _active_date != date.today():
        logger.info('Dry-run mode — replaying %s (today is %s)', _active_date, date.today())

    _state_file = os.path.join(_here, f"movers_{_active_date}.json")

    # Today's intraday volume (cumulative shares) — used for MACD alert rel_volume
    today_volumes: dict[str, float] = {}
    avg_volumes:   dict[str, float] = {}

    # RVOL leaderboard: rolling window of last 10 1m bars (today only) per symbol
    # Stores full bar dicts so we can compute volume + buyer/seller split
    today_bars: dict[str, deque] = {}   # symbol → deque(maxlen=10) of bar dicts

    # Per-minute-slot RVOL trackers — populated after universe build, used in leaderboard
    rvol_trackers: dict[str, RvolTracker] = {}

    # ── price-action state ────────────────────────────────────────────────────
    today_highs:      dict[str, float] = {}
    today_lows:       dict[str, float] = {}
    today_opens:      dict[str, float] = {}
    avg_daily_ranges: dict[str, float] = {}
    prev_closes:      dict[str, float] = {}  # last close before _active_date
    overnight_chg:    dict[str, float] = {}  # live % change vs prev_close

    bar_history:   dict[str, dict[int, deque]] = {}  # symbol -> tf -> 5-bar rolling window
    peak_momentum: dict[str, dict]  = {}  # best multi-bar % move on _active_date
    range_speed:   dict[str, dict]  = {}  # how fast stock covered its avg daily range
    moves_log: dict[str, list[dict]] = {} # symbol → [{ts, pct, window, tf, peak_ts}] one entry per MOM event

    alerted_symbols: set[str] = set()     # symbols with any alert (MACD or MOM) today
    active_symbols:  set[str] = set()     # symbols with MACD engines (≥2% move + ≥1M vol)
    _bootstrapping:  set[str] = set()     # symbols being history-replayed right now

    rolling_bar_ranges: dict[str, deque] = {}
    last_rapid_alerts:  dict[tuple, dict] = {}
    consol_breaks:      dict[str, dict]  = {}

    _bootstrap_days:       dict[str, dict[str, dict]] = {}
    _bootstrap_prev_close: dict[str, float]           = {}

    # ── state persistence (survives scanner restarts within same trading day) ──

    def _load_state():
        try:
            if not os.path.exists(_state_file):
                return
            with open(_state_file) as f:
                data = json.load(f)
            alerted_symbols.update(data.get('alerted_symbols', []))
            for sym, v in data.get('peak_momentum', {}).items():
                if v.get('pct', 0) > peak_momentum.get(sym, {}).get('pct', 0):
                    peak_momentum[sym] = v
            logger.info('Restored movers state: %d alerted symbols, %d momentum peaks',
                        len(alerted_symbols), len(peak_momentum))
        except Exception as e:
            logger.warning('Failed to load movers state: %s', e)

    def _save_state():
        try:
            with open(_state_file, 'w') as f:
                json.dump({
                    'alerted_symbols': list(alerted_symbols),
                    'peak_momentum':   peak_momentum,
                }, f)
            # Clean up previous-day state files
            for fn in os.listdir(_here):
                if fn.startswith('movers_') and fn.endswith('.json') and fn != os.path.basename(_state_file):
                    try:
                        os.remove(os.path.join(_here, fn))
                    except OSError:
                        pass
        except Exception as e:
            logger.warning('Failed to save movers state: %s', e)

    # ── leaderboard computation ───────────────────────────────────────────────

    def _ist_time(ts: int) -> str:
        m = (ts // 60 + 330) % (24 * 60)
        return f"{m // 60:02d}:{m % 60:02d}"

    def _ts_to_date(ts: int) -> str:
        from datetime import timezone, timedelta as _td
        ist = datetime.fromtimestamp(ts, tz=timezone(_td(hours=5, minutes=30)))
        return ist.strftime("%d-%b")

    def _ts_to_date_iso(ts: int) -> str:
        from datetime import timezone, timedelta as _td
        ist = datetime.fromtimestamp(ts, tz=timezone(_td(hours=5, minutes=30)))
        return ist.strftime("%Y-%m-%d")

    def compute_leaderboard() -> list[dict]:
        # Use the active trading date's wall-clock position for elapsed calc;
        # in dry-run mode we estimate elapsed as end-of-day (375 min).
        if _active_date == date.today():
            now_ist_min  = (int(time.time()) // 60 + 330) % (24 * 60)
            elapsed_min  = min(max(now_ist_min - 555, 1), 375)
        else:
            elapsed_min = 375  # full session elapsed for dry-run
        elapsed_frac = elapsed_min / 375.0

        rows = []
        for symbol in list(alerted_symbols):
            today_vol = today_volumes.get(symbol, 0.0)
            if today_vol <= 0:
                continue
            avg_daily = avg_volumes.get(symbol, 0.0)
            if avg_daily <= 0:
                continue

            ratio   = today_vol / (avg_daily * elapsed_frac)
            bars    = today_bars.get(symbol)
            tracker = rvol_trackers.get(symbol)

            # Bar RVOL — volume spike on most recent bar vs same time-slot historical avg
            bar_rvol = 0.0
            if bars and tracker:
                last_b   = bars[-1]
                bar_rvol = tracker.bar_rvol(last_b.get('ts', 0), last_b.get('volume', 0.0))

            # Today's intraday range %
            day_range_pct = 0.0
            open_p = today_opens.get(symbol, 0.0)
            if open_p > 0:
                h = today_highs.get(symbol, 0.0)
                l = today_lows.get(symbol, 0.0)
                if h > l > 0:
                    day_range_pct = round((h - l) / open_p * 100, 2)

            # Overnight change (live % from previous day's close)
            o_chg = overnight_chg.get(symbol, 0.0)

            # Range speed — % of avg daily range covered and how fast
            rs_ev       = range_speed.get(symbol)
            rs_coverage = rs_ev['coverage']     if rs_ev else 0.0
            rs_elapsed  = rs_ev['elapsed_mins'] if rs_ev else 0

            # Consolidation break event
            cb_ev    = consol_breaks.get(symbol)
            cb_ratio = cb_ev['ratio'] if cb_ev else 0.0
            cb_pct   = cb_ev['pct']   if cb_ev else 0.0
            cb_time  = _ist_time(cb_ev['ts']) if cb_ev else ''

            # Buyer/seller split from last ≤10 bars
            buyer_pct = 0.5
            if bars:
                tv = 0.0; tb = 0.0
                for bar_d in bars:
                    vol_b = bar_d.get('volume', 0.0)
                    rng   = max(bar_d.get('high', 0.0) - bar_d.get('low', 0.0), 1e-6)
                    tv   += vol_b
                    tb   += vol_b * max(bar_d.get('close', 0.0) - bar_d.get('low', 0.0), 0.0) / rng
                if tv > 0:
                    buyer_pct = tb / tv

            base = {
                'symbol':        symbol,
                'bar_rvol':      round(bar_rvol, 1),
                'overnight_chg': o_chg,
                'rs_coverage':   rs_coverage,
                'rs_elapsed':    rs_elapsed,
                'day_range_pct': day_range_pct,
                'cb_ratio':      cb_ratio,
                'cb_pct':        cb_pct,
                'cb_time':       cb_time,
                'today_vol':     round(today_vol),
                'avg_daily':     round(avg_daily),
                'elapsed':       elapsed_min,
                'buyer_pct':     round(buyer_pct, 3),
                'seller_pct':    round(1.0 - buyer_pct, 3),
            }

            # One row per MOM event; fall back to peak_momentum if moves_log is empty
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
                rows.append(row)

        rows.sort(key=lambda x: x['overnight_chg'], reverse=True)
        return rows

    # ── callbacks ─────────────────────────────────────────────────────────────

    def on_alert(alert: Alert):
        # Bootstrap replay builds indicator/episode state only — suppress all alerts.
        # Live-feed bars (is_live=True) surface alerts; dynamic mover activation
        # (which runs after is_live=True) surfaces today's bars from the cache.
        if not is_live:
            return
        if date.fromtimestamp(alert.ts) != _active_date:
            return
        today_vol = today_volumes.get(alert.symbol, 0.0)
        avg_vol   = avg_volumes.get(alert.symbol, 0.0)
        # Attach live context for alerts on the active trading date
        if date.fromtimestamp(alert.ts) == _active_date:
            alert.today_volume = today_vol
            alert.rel_volume   = (today_vol / avg_vol) if avg_vol > 0 else 0.0
            open_p = today_opens.get(alert.symbol, 0.0)
            if open_p > 0:
                h = today_highs.get(alert.symbol, 0.0)
                l = today_lows.get(alert.symbol, 0.0)
                if h > l > 0:
                    alert.day_range_pct = round((h - l) / open_p * 100, 2)
        alerted_symbols.add(alert.symbol)
        alert_mgr.add(alert)

    def on_quote_update(symbol: str, ltp: float, volume: float):
        if not is_live:
            return
        today_volumes[symbol] = volume
        
        # Intraday OHLC
        if symbol not in today_opens:
            today_opens[symbol] = ltp
        if ltp > today_highs.get(symbol, 0.0):
            today_highs[symbol] = ltp
        if symbol not in today_lows or ltp < today_lows[symbol]:
            today_lows[symbol] = ltp

        # Overnight change (live %, updated on each quote)
        prev_c = prev_closes.get(symbol, 0.0)
        if prev_c > 0:
            overnight_chg[symbol] = round((ltp - prev_c) / prev_c * 100, 2)
            
        # Check if overnight change is >= 2.0%
        # to qualify as a mover on the leaderboard
        if overnight_chg[symbol] >= 2.0:
            alerted_symbols.add(symbol)

    def on_bar(symbol: str, tf: int, bar):
        b      = bar if isinstance(bar, dict) else bar.__dict__
        bar_ts = b.get('ts', b.get('timestamp', 0))

        if tf in (1, 3, 5):
            high     = b.get('high', 0.0)
            low      = b.get('low', 0.0)
            open_p   = b.get('open', 0.0)
            close    = b.get('close', 0.0)

            # Check if the bar's date is today (pre-warming check)
            from datetime import datetime, timezone, timedelta
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            bar_date = datetime.fromtimestamp(bar_ts, tz=ist_tz).date().isoformat()
            is_today = (bar_date == _active_date.isoformat())
            
            if not is_today:
                if tf == 1:
                    ref = open_p if open_p > 0 else (low if low > 0 else 1.0)
                    bar_range_pct = (high - low) / ref * 100 if (high > low and ref > 0) else 0.0
                    if symbol not in rolling_bar_ranges:
                        rolling_bar_ranges[symbol] = deque(maxlen=20)
                    rolling_bar_ranges[symbol].append(bar_range_pct)
                
                if symbol not in bar_history:
                    bar_history[symbol] = {}
                if tf not in bar_history[symbol]:
                    bar_history[symbol][tf] = deque(maxlen=5)
                bar_history[symbol][tf].append({'open': open_p, 'high': high, 'low': low, 'ts': bar_ts})
                return

            if tf == 1:
                vol      = b.get('volume', 0.0)
                # Accumulate bar data
                if symbol not in today_bars:
                    today_bars[symbol] = deque(maxlen=10)
                today_bars[symbol].append(b)

                # Intraday OHLC
                if symbol not in today_opens:
                    today_opens[symbol] = open_p
                if high > today_highs.get(symbol, 0.0):
                    today_highs[symbol] = high
                if symbol not in today_lows or low < today_lows[symbol]:
                    today_lows[symbol] = low

                # Overnight change
                prev_c = prev_closes.get(symbol, 0.0)
                if prev_c > 0 and close > 0:
                    overnight_chg[symbol] = round((close - prev_c) / prev_c * 100, 2)

                # Bar range % vs open
                ref = open_p if open_p > 0 else (low if low > 0 else 1.0)
                bar_range_pct = (high - low) / ref * 100 if (high > low and ref > 0) else 0.0

                # Range speed: % of avg daily range covered and how fast
                h_t = today_highs.get(symbol, 0.0)
                l_t = today_lows.get(symbol, 0.0)
                op_t = today_opens.get(symbol, open_p)
                if op_t > 0 and h_t > l_t:
                    today_rng = (h_t - l_t) / op_t * 100
                    avg_dr    = avg_daily_ranges.get(symbol, 0.0)
                    if avg_dr > 0:
                        coverage = round(today_rng / avg_dr * 100, 1)
                        m_ist    = (bar_ts // 60 + 330) % (24 * 60)
                        elapsed  = max(m_ist - 555, 1)
                        existing_rs = range_speed.get(symbol)
                        if existing_rs is None or coverage > existing_rs.get('coverage', 0.0):
                            range_speed[symbol] = {'ts': bar_ts, 'coverage': coverage, 'elapsed_mins': elapsed}

                # Consolidation break: current bar ≥3× avg of last 10 bars before it
                if symbol not in rolling_bar_ranges:
                    rolling_bar_ranges[symbol] = deque(maxlen=20)
                rolling_bar_ranges[symbol].append(bar_range_pct)
                rng_list = list(rolling_bar_ranges[symbol])
                if len(rng_list) >= 8:
                    prev_bars = rng_list[-min(11, len(rng_list)):-1]
                    if len(prev_bars) >= 5:
                        prev_avg = sum(prev_bars) / len(prev_bars)
                        if prev_avg > 0 and bar_range_pct / prev_avg >= 3.0 and bar_range_pct >= 0.15:
                            consol_breaks[symbol] = {
                                'ts':    bar_ts,
                                'ratio': round(bar_range_pct / prev_avg, 1),
                                'pct':   round(bar_range_pct, 2),
                            }

            # Momentum calculation for TF 1, 3, and 5
            if symbol not in bar_history:
                bar_history[symbol] = {}
            if tf not in bar_history[symbol]:
                bar_history[symbol][tf] = deque(maxlen=5)
            bar_history[symbol][tf].append({'open': open_p, 'high': high, 'low': low, 'ts': bar_ts})
            hist = list(bar_history[symbol][tf])
            
            best_pct = 0.0
            best_win = 1
            for n in range(1, len(hist) + 1):
                win = hist[-n:]
                if len(win) > 0:
                    low_idx = min(range(len(win)), key=lambda idx: win[idx]['low'])
                    high_idx = max(range(len(win)), key=lambda idx: win[idx]['high'])
                    if low_idx <= high_idx and high_idx == len(win) - 1:
                        ref_p = win[low_idx]['low'] if win[low_idx]['low'] > 0 else win[0]['open']
                        if ref_p > 0:
                            move = (win[high_idx]['high'] - win[low_idx]['low']) / ref_p * 100
                            if move > best_pct:
                                best_pct = move
                                best_win = n

            # Peak momentum across all TFs
            if symbol not in peak_momentum:
                peak_momentum[symbol] = {}
            current_peak = peak_momentum[symbol].get('pct', 0.0)
            if best_pct > current_peak:
                peak_momentum[symbol] = {
                    'ts': bar_ts,
                    'pct': round(best_pct, 2),
                    'window': best_win,
                    'tf': tf
                }

            # ── Rapid Momentum Alert (No MACD logic) ──────────────────────────
            if best_pct >= Config.RAPID_MIN_PCT:
                key = (symbol, tf)
                last_alert = last_rapid_alerts.get(key)
                
                should_alert = False
                if last_alert is None:
                    should_alert = True
                else:
                    # Alert if 5 minutes have passed, OR if the move has extended by >= 0.5%
                    time_passed = (bar_ts - last_alert['ts']) >= 300
                    extended = (best_pct - last_alert['pct']) >= 0.5
                    if time_passed or extended:
                        should_alert = True
                
                if should_alert:
                    last_rapid_alerts[key] = {'ts': bar_ts, 'pct': best_pct}
                    
                    # Compute relative volume & day range
                    today_vol = today_volumes.get(symbol, 0.0)
                    avg_daily = avg_volumes.get(symbol, 0.0)
                    
                    # Use the active trading date's wall-clock position for elapsed calc
                    if _active_date == date.today():
                        now_ist_min  = (int(time.time()) // 60 + 330) % (24 * 60)
                        elapsed_min  = min(max(now_ist_min - 555, 1), 375)
                    else:
                        elapsed_min = 375
                    elapsed_frac = elapsed_min / 375.0
                    
                    rel_vol = round(today_vol / (avg_daily * elapsed_frac), 2) if (avg_daily > 0 and elapsed_frac > 0) else 0.0
                    
                    day_range_pct = 0.0
                    op_t = today_opens.get(symbol, 0.0)
                    if op_t > 0:
                        h_t = today_highs.get(symbol, 0.0)
                        l_t = today_lows.get(symbol, 0.0)
                        if h_t > l_t > 0:
                            day_range_pct = round((h_t - l_t) / op_t * 100, 2)
                    
                    # Generate alert event dictionary
                    rapid_evt = {
                        'symbol':        symbol,
                        'tf':            tf,
                        'direction':     'LONG',
                        'wave_num':      0,
                        'entry_price':   close,
                        'sl_level':      low,
                        'sl_distance':   close - low,
                        'sl_pct':        round((close - low) / close if close > 0 else 0.0, 4),
                        'swing_level':   open_p,
                        'rsi_at_entry':  0.0,
                        'ema_clear':     True,
                        'rsi_extreme':   True,
                        'ema_clear_v2':  True,
                        'rsi_extreme_v2':True,
                        'ep_len_so_far': best_win,
                        'ts':            bar_ts,
                        'type':          'rapid',  # identifies as rapid momentum alert
                        'today_volume':  today_vol,
                        'rel_volume':    rel_vol,
                        'day_range_pct': day_range_pct,
                        'time_ist':      _ist_time(bar_ts),
                        'date_ist':      _ts_to_date(bar_ts),
                        'date_iso':      _ts_to_date_iso(bar_ts),
                        'sl_pct_str':    f"{((close - low) / close * 100):.2f}%" if close > 0 else "0.00%",
                        '_key':          f"RAPID:{symbol}:{tf}:{bar_ts}:{best_pct:.2f}"
                    }
                    
                    # Add to alert manager so it persists and broadcasts to SSE
                    alert_mgr.add_event(rapid_evt)

            # Any ≥ MOVER_MIN_PCT move immediately qualifies symbol for Movers leaderboard
            if best_pct >= Config.MOVER_MIN_PCT:
                alerted_symbols.add(symbol)
                
                if symbol not in moves_log:
                    moves_log[symbol] = []
                
                # Check for active episode within 10 minutes of the last peak AND matching timeframe
                active_ep = None
                for ep in moves_log[symbol]:
                    if ep.get('tf') == tf and abs(bar_ts - ep.get('peak_ts', ep['ts'])) <= 600:
                        active_ep = ep
                        break
                
                if active_ep:
                    # Update active episode if new pct is higher
                    if best_pct > active_ep['pct']:
                        active_ep['pct']     = round(best_pct, 2)
                        active_ep['window']  = best_win
                        active_ep['tf']      = tf
                        active_ep['peak_ts'] = bar_ts
                        active_ep['ts']      = bar_ts  # update timestamp to show latest high on the table
                else:
                    # Create new separate episode
                    moves_log[symbol].append({
                        'ts':      bar_ts,
                        'peak_ts': bar_ts,
                        'pct':     round(best_pct, 2),
                        'window':  best_win,
                        'tf':      tf
                    })

                if is_live:
                    now_ts = int(time.time())
                    lag_s  = now_ts - (bar_ts + 60 * tf)
                    logger.info('MOM %s %.2f%% (%dm) bar=%s detected=%s lag=%ds',
                                symbol, best_pct, tf, _ist_time(bar_ts),
                                _ist_time(now_ts), lag_s)

    # ── dashboard ─────────────────────────────────────────────────────────────
    def get_debug() -> dict:
        avg_nonzero  = sum(1 for v in avg_volumes.values() if v > 0)
        today_nonzero = sum(1 for v in today_volumes.values() if v > 0)
        sample_avg   = {k: v for k, v in list(avg_volumes.items())[:5]}
        sample_today = {k: v for k, v in list(today_volumes.items())[:5]}
        return {
            'avg_volumes_total':   len(avg_volumes),
            'avg_volumes_nonzero': avg_nonzero,
            'avg_volumes_sample':  sample_avg,
            'today_volumes_total':   len(today_volumes),
            'today_volumes_nonzero': today_nonzero,
            'today_volumes_sample':  sample_today,
            'is_live': is_live,
        }

    _rescan_ref: dict = {}
    app = create_app(alert_mgr, compute_leaderboard, get_debug, _rescan_ref,
                     get_peak_momentum=lambda: peak_momentum)
    logger.info('Dashboard → http://%s:%d', Config.DASHBOARD_HOST, Config.DASHBOARD_PORT)

    flask_thread = threading.Thread(
        target=lambda: app.run(
            host=Config.DASHBOARD_HOST, port=Config.DASHBOARD_PORT,
            threaded=True, use_reloader=False,
        ),
        daemon=True, name='Dashboard'
    )
    flask_thread.start()
    time.sleep(1)

    # ── universe ──────────────────────────────────────────────────────────────
    logger.info('Building universe (turnover filter >= ₹%.0fL)...',
                Config.TURNOVER_THRESHOLD / 1e5)
    symbols = build_universe(ctx)
    if not symbols:
        logger.error('Empty universe — aborting')
        sys.exit(1)
    logger.info('Universe: %d symbols', len(symbols))

    for s in symbols:
        name = s['symbol']
        avg_vol = s.get('avg_daily_volume', 0.0)
        avg_volumes[name] = avg_vol
        prev_closes[name] = s.get('close', 0.0)
        avg_daily_ranges[name] = s.get('avg_daily_range', 0.0)
        
        # Populate RvolTracker with average Daily Volume scaled down to 1-minute slots
        tracker = RvolTracker(name)
        tracker.hist_mean = avg_vol / 375.0 if avg_vol > 0 else 1.0
        tracker.hist_rng_mean = s.get('avg_daily_range', 0.0) / 375.0 if s.get('avg_daily_range', 0.0) > 0 else 0.1
        tracker._ready = True
        rvol_trackers[name] = tracker

    # ── shared today-bar scan (Phase 1 + Rescan button) ─────────────────────────
    def _scan_today_bars():
        """Read today's cached 1-min bars for every symbol and update Movers state.
        Used by Phase 1 (pre-bootstrap) and the dashboard Rescan button."""
        today = _active_date.isoformat()
        local_last_fired: dict[str, int] = {}
        for sec in symbols:
            name = sec['symbol']
            bars = sorted(_load_cache(name).get(today, []), key=lambda x: x['ts'])
            if not bars:
                continue
            scan_events: list[dict] = []
            bh = deque(maxlen=5)
            vol_total = t_high = t_open = t_close = 0.0
            t_low = float('inf')
            for b in bars:
                ts     = b.get('ts', 0)
                open_p = b.get('open', 0.0)
                high   = b.get('high', 0.0)
                low    = b.get('low', 0.0)
                close  = b.get('close', 0.0)
                vol    = b.get('volume', 0.0)
                vol_total += vol
                if t_open == 0.0:    t_open = open_p
                if high > t_high:    t_high = high
                if low > 0 and low < t_low: t_low = low
                t_close = close
                bh.append({'open': open_p, 'high': high, 'low': low, 'ts': ts})
                hist = list(bh)
                best_pct = 0.0; best_win = 1
                for n in range(1, len(hist) + 1):
                    win = hist[-n:]
                    if len(win) > 0:
                        low_idx = min(range(len(win)), key=lambda idx: win[idx]['low'])
                        high_idx = max(range(len(win)), key=lambda idx: win[idx]['high'])
                        if low_idx <= high_idx and high_idx == len(win) - 1:
                            ref_p = win[low_idx]['low'] if win[low_idx]['low'] > 0 else win[0]['open']
                            if ref_p > 0:
                                move = (win[high_idx]['high'] - win[low_idx]['low']) / ref_p * 100
                                if move > best_pct: best_pct = move; best_win = n
                if best_pct > peak_momentum.get(name, {}).get('pct', 0.0):
                    peak_momentum[name] = {'ts': ts, 'pct': round(best_pct, 2), 'window': best_win}
                if best_pct >= Config.MOVER_MIN_PCT:
                    alerted_symbols.add(name)
                    
                    if name not in moves_log:
                        moves_log[name] = []
                    
                    # Check for active episode within 10 minutes of the last peak AND matching timeframe
                    active_ep = None
                    for ep in moves_log[name]:
                        if ep.get('tf') == 1 and abs(ts - ep.get('peak_ts', ep['ts'])) <= 600:
                            active_ep = ep
                            break
                    
                    if active_ep:
                        if best_pct > active_ep['pct']:
                            active_ep['pct']     = round(best_pct, 2)
                            active_ep['window']  = best_win
                            active_ep['peak_ts'] = ts
                            active_ep['ts']      = ts
                    else:
                        moves_log[name].append({
                            'ts':      ts,
                            'peak_ts': ts,
                            'pct':     round(best_pct, 2),
                            'window':  best_win,
                            'tf':      1  # default rescan tf
                        })
            if vol_total > 0:   today_volumes[name]  = vol_total
            if t_high  > 0:     today_highs[name]    = t_high
            if t_low   < float('inf') and t_low > 0: today_lows[name] = t_low
            if t_open  > 0 and name not in today_opens: today_opens[name] = t_open
            prev_c = prev_closes.get(name, 0.0)
            if prev_c > 0 and t_close > 0:
                overnight_chg[name] = round((t_close - prev_c) / prev_c * 100, 2)

            peak_pct = peak_momentum.get(name, {}).get('pct', 0.0)
            if peak_pct >= Config.MOVER_MIN_PCT and vol_total >= Config.MOVER_MIN_VOLUME:
                active_symbols.add(name)
                alerted_symbols.add(name)

    # ── Phase 1: instant Movers from disk cache ───────────────────────────────
    _load_state()  # restore alerted_symbols + peak_momentum from previous run today
    _scan_today_bars()
    logger.info('Phase 1 complete — %d movers pre-populated, dashboard live', len(alerted_symbols))

    def _bootstrap_new_mover(sec: dict):
        """Replay all cached bars for a newly-detected mover to build indicator state.
        Live bars for this symbol are blocked until replay is done."""
        name = sec['symbol']
        _bootstrapping.add(name)
        try:
            logger.info('New mover activated: %s — replaying cached history', name)
            needed  = _trading_days(Config.HISTORY_DAYS)
            cache   = _load_cache(name)
            bars_1m = sorted(
                [b for day in needed for b in cache.get(day, [])],
                key=lambda x: x['ts'],
            )
            for tf in Config.TIMEFRAMES:
                bars = bars_1m if tf == 1 else _resample(bars_1m, tf)
                for b in bars:
                    on_bar(name, tf, b)
            logger.info('New mover %s: history done (%d 1m bars)', name, len(bars_1m))
        finally:
            _bootstrapping.discard(name)

    # ── Phase 2: full bootstrap in background ────────────────────────────────
    _bootstrap_done = threading.Event()

    def _backfill_history():
        """Detect movers and run MACD strategy for each of the past 7 trading days.
        Alerts are written to per-day JSONL files and loaded into the Alerts tab history.
        Runs as a daemon thread after Phase 2 completes so it never blocks live trading."""
        all_days = _trading_days(Config.HISTORY_DAYS)   # list of ISO date strings
        today_str = date.today().isoformat()           # calendar today, not active trading date
        past_days = sorted(d for d in all_days if d < today_str)[-7:]

        if not past_days:
            logger.info('Backfill: no past days available in cache')
            return

        logger.info('Backfill: scanning %d past days (%s → %s)',
                    len(past_days), past_days[0], past_days[-1])

        for target_day in past_days:
            target_date_obj = date.fromisoformat(target_day)
            cutoff_date     = target_date_obj - timedelta(days=4)
            strategy_cutoff = int(datetime(
                cutoff_date.year, cutoff_date.month, cutoff_date.day,
            ).timestamp())

            # ── step 1: find movers and generate MOM events for this day ───
            day_movers: list[str] = []
            mom_last_fired_bf: dict[str, int] = {}
            for sec in symbols:
                name  = sec['symbol']
                cache = _load_cache(name)
                day_bars = sorted(cache.get(target_day, []), key=lambda x: x['ts'])
                if not day_bars:
                    continue
                bh        = deque(maxlen=5)
                vol_total = 0.0
                best_pct  = 0.0
                for b in day_bars:
                    vol_total += b.get('volume', 0.0)
                    ts  = b.get('ts', 0)
                    bh.append({
                        'open': b.get('open', 0.0),
                        'high': b.get('high', 0.0),
                        'low':  b.get('low', 0.0),
                        'ts':   ts,
                    })
                    hist = list(bh)
                    bar_best = 0.0; bar_win = 1
                    for n in range(1, len(hist) + 1):
                        win = hist[-n:]
                        if len(win) > 0:
                            low_idx = min(range(len(win)), key=lambda idx: win[idx]['low'])
                            high_idx = max(range(len(win)), key=lambda idx: win[idx]['high'])
                            if low_idx <= high_idx and high_idx == len(win) - 1:
                                ref_p = win[low_idx]['low'] if win[low_idx]['low'] > 0 else win[0]['open']
                                if ref_p > 0:
                                    move = (win[high_idx]['high'] - win[low_idx]['low']) / ref_p * 100
                                    if move > bar_best:
                                        bar_best = move; bar_win = n
                    if bar_best > best_pct:
                        best_pct = bar_best
                    if bar_best >= Config.MOVER_MIN_PCT and ts - mom_last_fired_bf.get(name, 0) >= 300:
                        mom_last_fired_bf[name] = ts
                        alert_mgr.add_historical_event({
                            'alert_type':   'MOM',
                            'symbol':       name,
                            'tf':           1,
                            'pct':          round(bar_best, 2),
                            'window':       bar_win,
                            'ts':           ts,
                            'time_ist':     _ist_time(ts),
                            'date_ist':     _ts_to_date(ts),
                            'today_volume': 0.0,
                            '_key':         f"{name}:MOM:{ts}",
                        }, target_day)
                if best_pct >= Config.MOVER_MIN_PCT and vol_total >= Config.MOVER_MIN_VOLUME:
                    day_movers.append(name)

            logger.info('Backfill %s: %d movers found', target_day, len(day_movers))

            # ── step 2: run MACD strategy for each mover ───────────────────
            for name in day_movers:
                try:
                    cache   = _load_cache(name)
                    # Load all cached days up to and including target_day for indicator warmup
                    bars_1m = sorted(
                        [b for day in all_days if day <= target_day
                         for b in cache.get(day, [])],
                        key=lambda x: x['ts'],
                    )
                    if not bars_1m:
                        continue

                    for tf in Config.TIMEFRAMES:
                        tf_bars = bars_1m if tf == 1 else _resample(bars_1m, tf)
                        ind     = IndicatorSet()
                        collected: list[Alert] = []

                        def _bf_cb(a: Alert, _tgt=target_date_obj, _lst=collected):
                            if date.fromtimestamp(a.ts) == _tgt:
                                _lst.append(a)

                        engine = StrategyEngine(name, tf, _bf_cb)

                        for b in tf_bars:
                            b_dict = b if isinstance(b, dict) else b.__dict__
                            b_ts   = b_dict.get('ts', 0)
                            vals   = ind.update(b)
                            if vals is None or b_ts < strategy_cutoff:
                                continue
                            rec = BarRecord(
                                ts=b_ts,
                                open=b_dict.get('open', 0.0),  high=b_dict.get('high', 0.0),
                                low=b_dict.get('low', 0.0),    close=b_dict.get('close', 0.0),
                                volume=b_dict.get('volume', 0.0),
                                macd=vals['macd'], signal=vals['signal'],
                                ema50=vals['ema50'], rsi=vals['rsi'],
                            )
                            engine.update(rec)

                        for alert in collected:
                            alert_mgr.add_historical(alert, target_day)

                except Exception as e:
                    logger.warning('Backfill %s/%s: skipped — %s', target_day, name, e)

            logger.info('Backfill %s complete', target_day)

        logger.info('Backfill complete for all past days')

    is_live = True
    _bootstrap_done = threading.Event()
    _bootstrap_done.set()

    # ── rescan (on-demand, reuses _scan_today_bars) ───────────────────────────
    def rescan_today() -> int:
        _scan_today_bars()
        return 0

    _rescan_ref['fn'] = rescan_today

    # ── live feed ─────────────────────────────────────────────────────────────
    feed = LiveFeed(ctx, symbols, on_bar, on_quote_update)

    if args.replay:
        logger.info("Replay mode enabled. Fetching today's 1-minute bars...")
        
        # Download bars using client
        from dhanhq import dhanhq
        client = dhanhq(ctx)
        
        from dhan_feed import fetch_today_1m_bars
        bars_map = fetch_today_1m_bars(client, symbols)
        
        # Flatten and sort chronologically
        all_events = []
        for sym, bars in bars_map.items():
            for b in bars:
                all_events.append((b['ts'], sym, b))
        all_events.sort(key=lambda x: x[0])
        
        # Initialize aggregators for replay rollup
        from bar_aggregator import BarAggregator
        replay_aggregators = {s['symbol']: BarAggregator(s['symbol'], on_bar) for s in symbols}
        
        def _run_replay():
            nonlocal is_live
            is_live = True
            logger.info("Starting replay of %d 1-minute bars...", len(all_events))
            
            from datetime import timezone, timedelta
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            today_str = _active_date.isoformat()
            
            for i, (ts, sym, bar) in enumerate(all_events):
                # Check if the bar's date is today (for quote updates/volume tracking)
                bar_date = datetime.fromtimestamp(ts, tz=ist_tz).date().isoformat()
                is_today_bar = (bar_date == today_str)
                
                if is_today_bar:
                    # Accumulate volume for today's total volume (intraday bars volume is per-bar)
                    current_vol = today_volumes.get(sym, 0.0) + bar['volume']
                    today_volumes[sym] = current_vol
                    # Update quote stats (with cumulative volume!)
                    on_quote_update(sym, bar['close'], current_vol)
                
                # Feed bar to aggregator (which handles rollup and calls on_bar for 1m, 3m, 5m, 15m)
                replay_aggregators[sym].feed_historical(bar)
                
                # Throttle slightly to yield CPU control back to OS scheduler
                if i % 2000 == 0:
                    time.sleep(0.005)
            
            # Flush final in-progress bars from aggregators
            for sym, agg in replay_aggregators.items():
                for tf, cur in list(agg._bars.items()):
                    if cur:
                        on_bar(sym, tf, cur)
                        
            logger.info("Replay complete! Transitioning to live polling...")
            feed.start()
            
        threading.Thread(target=_run_replay, daemon=True, name="ReplayToday").start()
    else:
        feed.start()

    # ── heartbeat ─────────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(300)
            _save_state()
            logger.info('Heartbeat — alerted symbols: %d  leaderboard: %d',
                        len(alerted_symbols),
                        len(today_bars))
    except KeyboardInterrupt:
        logger.info('Shutting down...')
        _save_state()
        feed.stop()


if __name__ == '__main__':
    main()

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
import logging
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta

from dhanhq import DhanContext

from config import Config
from indicators import IndicatorSet
from strategy_engine import StrategyEngine, BarRecord, Alert
from universe import build_universe
from dhan_feed import bootstrap, LiveFeed
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
    errors = Config.validate()
    if errors:
        for e in errors:
            logger.error('Config: %s', e)
        sys.exit(1)

    ctx       = DhanContext(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)
    alert_mgr = AlertManager()

    # ── state stores ─────────────────────────────────────────────────────────
    ind_sets: dict[tuple, IndicatorSet]   = {}
    engines:  dict[tuple, StrategyEngine] = {}
    # Last bar timestamp fed to each strategy engine during bootstrap.
    # Seeded into LiveFeed._last_ts so the live feed never replays bootstrap bars.
    bootstrap_last_ts: dict[tuple, int] = {}

    is_live = False

    def _trading_day() -> date:
        """Most recent trading weekday (today if Mon-Fri, else most recent Friday)."""
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

    bar_history:   dict[str, deque] = {}  # 5-bar rolling window for momentum calc
    peak_momentum: dict[str, dict]  = {}  # best multi-bar % move on _active_date
    range_speed:   dict[str, dict]  = {}  # how fast stock covered its avg daily range

    rolling_bar_ranges: dict[str, deque] = {}
    consol_breaks:      dict[str, dict]  = {}

    _bootstrap_days:       dict[str, dict[str, dict]] = {}
    _bootstrap_prev_close: dict[str, float]           = {}

    # ── leaderboard computation ───────────────────────────────────────────────

    def _ist_time(ts: int) -> str:
        m = (ts // 60 + 330) % (24 * 60)
        return f"{m // 60:02d}:{m % 60:02d}"

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
        for symbol, today_vol in list(today_volumes.items()):
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

            # Peak momentum event (best % move in any 1-5 bar window today)
            mom_ev        = peak_momentum.get(symbol)
            peak_mom_pct  = mom_ev['pct']    if mom_ev else 0.0
            peak_mom_win  = mom_ev['window'] if mom_ev else 0
            peak_mom_time = _ist_time(mom_ev['ts']) if mom_ev else ''

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

            rows.append({
                'symbol':        symbol,
                'bar_rvol':      round(bar_rvol, 1),
                'overnight_chg': o_chg,
                'peak_mom_pct':  peak_mom_pct,
                'peak_mom_win':  peak_mom_win,
                'peak_mom_time': peak_mom_time,
                'rs_coverage':   rs_coverage,
                'rs_elapsed':    rs_elapsed,
                'day_range_pct': day_range_pct,
                'cb_ratio':      cb_ratio,
                'cb_pct':        cb_pct,
                'cb_time':       cb_time,
                'ratio':         round(ratio, 2),
                'today_vol':     round(today_vol),
                'avg_daily':     round(avg_daily),
                'elapsed':       elapsed_min,
                'buyer_pct':     round(buyer_pct, 3),
                'seller_pct':    round(1.0 - buyer_pct, 3),
            })

        rows.sort(key=lambda x: x['overnight_chg'], reverse=True)
        return rows

    # ── callbacks ─────────────────────────────────────────────────────────────

    def on_alert(alert: Alert):
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
        alert_mgr.add(alert)

    def on_volume_update(symbol: str, bars: list[dict]):
        """Called by LiveFeed on every poll with ALL of today's closed 1m bars."""
        total = sum(b.get('volume', 0.0) for b in bars)
        today_volumes[symbol] = total
        if bars:
            if symbol not in today_bars:
                today_bars[symbol] = deque(maxlen=10)
            else:
                today_bars[symbol].clear()
            for b in bars[-10:]:
                today_bars[symbol].append(b)

    def on_bar(symbol: str, tf: int, bar):
        b      = bar if isinstance(bar, dict) else bar.__dict__
        bar_ts = b.get('ts', b.get('timestamp', 0))

        if tf == 1:
            vol      = b.get('volume', 0.0)
            high     = b.get('high', 0.0)
            low      = b.get('low', 0.0)
            open_p   = b.get('open', 0.0)
            close    = b.get('close', 0.0)
            bar_date = date.fromtimestamp(bar_ts)
            is_active_bar = (bar_date == _active_date)

            if is_active_bar:
                # Cumulative volume + recent bars
                today_volumes[symbol] = today_volumes.get(symbol, 0.0) + vol
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

                # Overnight change (live %, updated each bar)
                prev_c = prev_closes.get(symbol, 0.0)
                if prev_c > 0 and close > 0:
                    overnight_chg[symbol] = round((close - prev_c) / prev_c * 100, 2)

                # Bar range % vs open
                ref = open_p if open_p > 0 else (low if low > 0 else 1.0)
                bar_range_pct = (high - low) / ref * 100 if (high > low and ref > 0) else 0.0

                # Momentum: best % range in any 1-5 consecutive bars
                if symbol not in bar_history:
                    bar_history[symbol] = deque(maxlen=5)
                bar_history[symbol].append({'open': open_p, 'high': high, 'low': low, 'ts': bar_ts})
                hist = list(bar_history[symbol])
                best_pct = 0.0
                best_win = 1
                for n in range(1, len(hist) + 1):
                    win = hist[-n:]
                    ref_o = win[0]['open']
                    if ref_o > 0:
                        move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref_o * 100
                        if move > best_pct:
                            best_pct = move
                            best_win = n
                if best_pct > peak_momentum.get(symbol, {}).get('pct', 0.0):
                    peak_momentum[symbol] = {'ts': bar_ts, 'pct': round(best_pct, 2), 'window': best_win}

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

            else:
                # Historical bar — accumulate baselines during bootstrap
                if not is_live:
                    _bootstrap_prev_close[symbol] = close
                    d_str    = str(bar_date)
                    sym_days = _bootstrap_days.setdefault(symbol, {})
                    if d_str not in sym_days:
                        sym_days[d_str] = {'high': high, 'low': low, 'open': open_p}
                    else:
                        if high > sym_days[d_str]['high']: sym_days[d_str]['high'] = high
                        if low  < sym_days[d_str]['low']:  sym_days[d_str]['low']  = low

            # Feed RVOL tracker during bootstrap (used for bar_rvol in live mode)
            tracker = rvol_trackers.get(symbol)
            if tracker is not None and not is_live:
                tracker.add_historical(bar_ts, vol, high, low, open_p)

        key = (symbol, tf)
        if key not in ind_sets:
            ind_sets[key] = IndicatorSet()
            engines[key]  = StrategyEngine(symbol, tf, on_alert)

        vals = ind_sets[key].update(bar)
        if vals is None:
            return

        if bar_ts < _strategy_cutoff:
            return

        rec = BarRecord(
            ts=bar_ts,
            open=b.get('open', 0.0), high=b.get('high', 0.0),
            low=b.get('low', 0.0),   close=b.get('close', 0.0),
            volume=b.get('volume', 0.0),
            macd=vals['macd'], signal=vals['signal'],
            ema50=vals['ema50'], rsi=vals['rsi'],
        )
        engines[key].update(rec)
        # Track last bar fed to each engine so the live feed can pick up from here
        if bar_ts > bootstrap_last_ts.get(key, 0):
            bootstrap_last_ts[key] = bar_ts

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

    app = create_app(alert_mgr, compute_leaderboard, get_debug)
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

    avg_volumes.update({s['symbol']: s.get('avg_daily_volume', 0.0) for s in symbols})

    rvol_trackers.update({s['symbol']: RvolTracker(s['symbol']) for s in symbols})

    # ── bootstrap ─────────────────────────────────────────────────────────────
    bootstrap(ctx, symbols, on_bar)

    for t in rvol_trackers.values():
        t.finalize()
    logger.info('RVOL baselines ready: %d symbols', len(rvol_trackers))

    # Compute avg daily range % from bootstrap per-day data
    for sym, days in _bootstrap_days.items():
        day_ranges = []
        for d_data in days.values():
            if d_data['open'] > 0 and d_data['high'] > d_data['low']:
                day_ranges.append((d_data['high'] - d_data['low']) / d_data['open'] * 100)
        if day_ranges:
            avg_daily_ranges[sym] = sum(day_ranges) / len(day_ranges)
    _bootstrap_days.clear()
    logger.info('Avg daily range baselines ready: %d symbols', len(avg_daily_ranges))

    prev_closes.update(_bootstrap_prev_close)
    _bootstrap_prev_close.clear()
    logger.info('Prev closes ready: %d symbols', len(prev_closes))

    # If bhavcopy gave us zero avg_daily_volume (column name mismatch etc.),
    # fall back to deriving it from the RVOL tracker: hist_mean × 375 bars/session.
    missing = sum(1 for sym in rvol_trackers if avg_volumes.get(sym, 0) == 0)
    if missing > 0:
        logger.warning('avg_daily_volume=0 for %d/%d symbols — deriving from RVOL bootstrap data',
                       missing, len(rvol_trackers))
        for sym, tracker in rvol_trackers.items():
            if avg_volumes.get(sym, 0) == 0 and tracker.hist_mean > 0:
                avg_volumes[sym] = tracker.hist_mean * 375

    is_live = True
    logger.info('Live mode active — alerts and leaderboard now updating')

    # ── live feed ─────────────────────────────────────────────────────────────
    feed = LiveFeed(ctx, symbols, on_bar, on_volume_update)
    # Seed last-seen timestamps so the live feed never replays bootstrap bars
    feed._last_ts.update(bootstrap_last_ts)
    logger.info('Live feed seeded with %d bootstrap timestamps', len(bootstrap_last_ts))
    feed.start()

    # ── heartbeat ─────────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(300)
            logger.info('Heartbeat — engines: %d (1m)  alerts: %d  leaderboard symbols: %d',
                        len([k for k in engines if k[1] == 1]),
                        len(alert_mgr.get_all()),
                        len(today_bars))
    except KeyboardInterrupt:
        logger.info('Shutting down...')
        feed.stop()


if __name__ == '__main__':
    main()

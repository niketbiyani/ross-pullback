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

    _cutoff_date     = date.today() - timedelta(days=4)
    _strategy_cutoff = int(datetime(
        _cutoff_date.year, _cutoff_date.month, _cutoff_date.day
    ).timestamp())

    # Today's intraday volume (cumulative shares) — used for MACD alert rel_volume
    today_volumes: dict[str, float] = {}
    avg_volumes:   dict[str, float] = {}

    # RVOL leaderboard: rolling window of last 10 1m bars (today only) per symbol
    # Stores full bar dicts so we can compute volume + buyer/seller split
    today_bars: dict[str, deque] = {}   # symbol → deque(maxlen=10) of bar dicts

    # Per-minute-slot RVOL trackers — populated after universe build, used in leaderboard
    rvol_trackers: dict[str, RvolTracker] = {}

    # ── price-action state ────────────────────────────────────────────────────
    today_highs:    dict[str, float] = {}   # running intraday high
    today_lows:     dict[str, float] = {}   # running intraday low
    today_opens:    dict[str, float] = {}   # first bar open today
    avg_daily_ranges: dict[str, float] = {} # avg (high-low)/open % across history days

    # Events tracked live (today only)
    rolling_bar_ranges: dict[str, deque] = {}  # last 20 bar range %s (consolidation)
    consol_breaks:      dict[str, dict]  = {}  # latest consolidation break event
    peak_price_rvols:   dict[str, dict]  = {}  # best price RVOL bar seen today
    in_play_stocks:     dict[str, dict]  = {}  # stocks with 3%+ opening range

    # Temporary bootstrap accumulator — cleared after finalize
    _bootstrap_days: dict[str, dict[str, dict]] = {}  # symbol → date → {high,low,open}

    # ── leaderboard computation ───────────────────────────────────────────────

    def _ist_time(ts: int) -> str:
        m = (ts // 60 + 330) % (24 * 60)
        return f"{m // 60:02d}:{m % 60:02d}"

    def compute_leaderboard() -> list[dict]:
        now_ist_min  = (int(time.time()) // 60 + 330) % (24 * 60)
        elapsed_min  = min(max(now_ist_min - 555, 1), 375)
        elapsed_frac = elapsed_min / 375.0

        rows = []
        for symbol, today_vol in list(today_volumes.items()):
            if today_vol <= 0:
                continue
            avg_daily = avg_volumes.get(symbol, 0.0)
            if avg_daily <= 0:
                continue

            ratio = today_vol / (avg_daily * elapsed_frac)

            bars    = today_bars.get(symbol)
            tracker = rvol_trackers.get(symbol)

            # Bar RVOL — volume spike on most recent bar
            bar_rvol = 0.0
            if bars and tracker:
                last_b   = bars[-1]
                bar_rvol = tracker.bar_rvol(last_b.get('ts', 0), last_b.get('volume', 0.0))

            # Price RVOL — price-range spike on most recent bar
            price_rvol = 0.0
            if bars and tracker:
                last_b     = bars[-1]
                price_rvol = tracker.bar_price_rvol(
                    last_b.get('ts', 0), last_b.get('high', 0.0),
                    last_b.get('low', 0.0), last_b.get('open', 0.0),
                )

            # Today's intraday range vs historical avg daily range
            day_range_pct  = 0.0
            day_range_rvol = 0.0
            open_p = today_opens.get(symbol, 0.0)
            if open_p > 0:
                h = today_highs.get(symbol, 0.0)
                l = today_lows.get(symbol, 0.0)
                if h > l > 0:
                    day_range_pct = round((h - l) / open_p * 100, 2)
                    avg_dr = avg_daily_ranges.get(symbol, 0.0)
                    if avg_dr > 0:
                        day_range_rvol = round(day_range_pct / avg_dr, 2)

            # Peak price RVOL event today (with time)
            peak_ev   = peak_price_rvols.get(symbol)
            peak_rvol = peak_ev['rvol']   if peak_ev else 0.0
            peak_pct  = peak_ev['pct']    if peak_ev else 0.0
            peak_time = _ist_time(peak_ev['ts']) if peak_ev else ''

            # Consolidation break event (with time)
            cb_ev     = consol_breaks.get(symbol)
            cb_ratio  = cb_ev['ratio'] if cb_ev else 0.0
            cb_pct    = cb_ev['pct']   if cb_ev else 0.0
            cb_time   = _ist_time(cb_ev['ts']) if cb_ev else ''

            # Opening move / in-play flag (with time)
            ip_ev     = in_play_stocks.get(symbol)
            ip_pct    = ip_ev['pct']  if ip_ev else 0.0
            ip_time   = _ist_time(ip_ev['ts']) if ip_ev else ''

            # Buyer/seller split from last ≤10 bars
            buyer_pct = 0.5
            if bars:
                tv = 0.0; tb = 0.0
                for b in bars:
                    vol = b.get('volume', 0.0)
                    rng = max(b.get('high', 0.0) - b.get('low', 0.0), 1e-6)
                    tv += vol
                    tb += vol * max(b.get('close', 0.0) - b.get('low', 0.0), 0.0) / rng
                if tv > 0:
                    buyer_pct = tb / tv

            rows.append({
                'symbol':        symbol,
                'bar_rvol':      round(bar_rvol, 1),
                'price_rvol':    round(price_rvol, 1),
                'day_range_pct': day_range_pct,
                'day_range_rvol': day_range_rvol,
                'avg_day_range': round(avg_daily_ranges.get(symbol, 0.0), 2),
                'peak_rvol':     peak_rvol,
                'peak_pct':      peak_pct,
                'peak_time':     peak_time,
                'cb_ratio':      cb_ratio,
                'cb_pct':        cb_pct,
                'cb_time':       cb_time,
                'ip_pct':        ip_pct,
                'ip_time':       ip_time,
                'ratio':         round(ratio, 2),
                'today_vol':     round(today_vol),
                'avg_daily':     round(avg_daily),
                'elapsed':       elapsed_min,
                'buyer_pct':     round(buyer_pct, 3),
                'seller_pct':    round(1.0 - buyer_pct, 3),
            })

        rows.sort(key=lambda x: x['bar_rvol'], reverse=True)
        return rows

    # ── callbacks ─────────────────────────────────────────────────────────────

    def on_alert(alert: Alert):
        today_vol = today_volumes.get(alert.symbol, 0.0)
        avg_vol   = avg_volumes.get(alert.symbol, 0.0)
        # Only attach live context when the alert is from today
        if date.fromtimestamp(alert.ts) == date.today():
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
            vol    = b.get('volume', 0.0)
            high   = b.get('high', 0.0)
            low    = b.get('low', 0.0)
            open_p = b.get('open', 0.0)
            is_today_bar = date.fromtimestamp(bar_ts) == date.today()

            if is_today_bar:
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

                # Bar range %
                ref = open_p if open_p > 0 else (low if low > 0 else 1.0)
                bar_range_pct = (high - low) / ref * 100 if (high > low and ref > 0) else 0.0

                # Rolling range window for consolidation detection
                if symbol not in rolling_bar_ranges:
                    rolling_bar_ranges[symbol] = deque(maxlen=20)
                rolling_bar_ranges[symbol].append(bar_range_pct)

                rng_win = rolling_bar_ranges[symbol]
                if len(rng_win) >= 6 and bar_range_pct >= 0.3:
                    prev_list = list(rng_win)[:-1]
                    prev_avg  = sum(prev_list) / len(prev_list)
                    if prev_avg > 0 and bar_range_pct / prev_avg >= 3.0:
                        consol_breaks[symbol] = {
                            'ts':    bar_ts,
                            'ratio': round(bar_range_pct / prev_avg, 1),
                            'pct':   round(bar_range_pct, 2),
                        }

                # Price RVOL for this bar (only after baselines are ready)
                tracker = rvol_trackers.get(symbol)
                price_rvol_val = 0.0
                if tracker and is_live:
                    price_rvol_val = tracker.bar_price_rvol(bar_ts, high, low, open_p)
                    existing_peak  = peak_price_rvols.get(symbol)
                    if price_rvol_val > (existing_peak['rvol'] if existing_peak else 0.0):
                        peak_price_rvols[symbol] = {
                            'ts':   bar_ts,
                            'rvol': round(price_rvol_val, 1),
                            'pct':  round(bar_range_pct, 2),
                        }

                # In-play: big opening move in first 10 bars (09:15–09:24)
                m_ist = (bar_ts // 60 + 330) % (24 * 60)
                if m_ist - 555 <= 9 and symbol not in in_play_stocks:
                    op = today_opens.get(symbol, open_p)
                    if op > 0:
                        op_range = (today_highs.get(symbol, high) - today_lows.get(symbol, low)) / op * 100
                        if op_range >= 3.0:
                            in_play_stocks[symbol] = {'ts': bar_ts, 'pct': round(op_range, 1)}
            else:
                # Historical bar — accumulate per-day OHLC for avg daily range
                if not is_live:
                    d_str    = str(date.fromtimestamp(bar_ts))
                    sym_days = _bootstrap_days.setdefault(symbol, {})
                    if d_str not in sym_days:
                        sym_days[d_str] = {'high': high, 'low': low, 'open': open_p}
                    else:
                        if high > sym_days[d_str]['high']: sym_days[d_str]['high'] = high
                        if low  < sym_days[d_str]['low']:  sym_days[d_str]['low']  = low

            # Feed to RVOL tracker during bootstrap (volume + price range)
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

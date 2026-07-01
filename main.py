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

    # ── leaderboard computation ───────────────────────────────────────────────

    def compute_leaderboard() -> list[dict]:
        """
        Rank symbols by cumulative today volume vs expected volume at this point in
        the session: ratio = today_vol / (avg_daily_vol × elapsed_fraction).
        elapsed_fraction = minutes since 9:15 IST / 375 (full session length).
        A ratio of 5 means "on pace for 5× normal daily volume today."
        Returns top 20, most active first.
        """
        now_ist_min  = (int(time.time()) // 60 + 330) % (24 * 60)
        elapsed_min  = min(max(now_ist_min - 555, 1), 375)  # clamp to [1, 375]
        elapsed_frac = elapsed_min / 375.0

        rows = []
        for symbol, today_vol in list(today_volumes.items()):
            if today_vol <= 0:
                continue
            avg_daily = avg_volumes.get(symbol, 0.0)
            if avg_daily <= 0:
                continue

            ratio = today_vol / (avg_daily * elapsed_frac)

            # Buyer/seller split from last ≤10 bars
            bars      = today_bars.get(symbol)
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
                'symbol':     symbol,
                'ratio':      round(ratio, 2),
                'today_vol':  round(today_vol),
                'avg_daily':  round(avg_daily),
                'elapsed':    elapsed_min,
                'buyer_pct':  round(buyer_pct, 3),
                'seller_pct': round(1.0 - buyer_pct, 3),
            })

        rows.sort(key=lambda x: x['ratio'], reverse=True)
        return rows[:20]

    # ── callbacks ─────────────────────────────────────────────────────────────

    def on_alert(alert: Alert):
        if is_live:
            today_vol = today_volumes.get(alert.symbol, 0.0)
            avg_vol   = avg_volumes.get(alert.symbol, 0.0)
            alert.today_volume = today_vol
            alert.rel_volume   = (today_vol / avg_vol) if avg_vol > 0 else 0.0
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
            vol = b.get('volume', 0.0)
            if date.fromtimestamp(bar_ts) == date.today():
                today_volumes[symbol] = today_volumes.get(symbol, 0.0) + vol
                if symbol not in today_bars:
                    today_bars[symbol] = deque(maxlen=10)
                today_bars[symbol].append(b)
            # Feed historical bars to RVOL tracker during bootstrap
            tracker = rvol_trackers.get(symbol)
            if tracker is not None and not is_live:
                tracker.add_historical(bar_ts, vol)

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

    rvol_trackers: dict[str, RvolTracker] = {
        s['symbol']: RvolTracker(s['symbol']) for s in symbols
    }

    # ── bootstrap ─────────────────────────────────────────────────────────────
    bootstrap(ctx, symbols, on_bar)

    for t in rvol_trackers.values():
        t.finalize()
    logger.info('RVOL baselines ready: %d symbols', len(rvol_trackers))

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

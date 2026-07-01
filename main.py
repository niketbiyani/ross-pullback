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
from datetime import date, datetime, timedelta

from dhanhq import DhanContext

from config import Config
from indicators import IndicatorSet
from strategy_engine import StrategyEngine, BarRecord, Alert
from universe import build_universe
from dhan_feed import bootstrap, LiveFeed
from alert_manager import AlertManager, VolumeAlertManager
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

    ctx           = DhanContext(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)
    alert_mgr     = AlertManager()
    vol_alert_mgr = VolumeAlertManager()

    # ── state stores (created lazily as new symbol-TF combos appear) ─────────
    ind_sets: dict[tuple, IndicatorSet]   = {}
    engines:  dict[tuple, StrategyEngine] = {}

    is_live = False   # suppress alerts during bootstrap

    # Only run strategy engine on the most recent 4 calendar days (~2 trading days).
    # Earlier bars still flow through IndicatorSet for warmup — just skip episode detection.
    _cutoff_date      = date.today() - timedelta(days=4)
    _strategy_cutoff  = int(datetime(_cutoff_date.year, _cutoff_date.month, _cutoff_date.day).timestamp())

    # Live intraday volume tracking — updated in on_bar, read in on_alert
    today_volumes: dict[str, float] = {}   # symbol -> cumulative shares traded today
    avg_volumes:   dict[str, float] = {}   # symbol -> avg daily volume from bhavcopy

    def on_alert(alert: Alert):
        if is_live:
            today_vol = today_volumes.get(alert.symbol, 0.0)
            avg_vol   = avg_volumes.get(alert.symbol, 0.0)
            alert.today_volume = today_vol
            alert.rel_volume   = (today_vol / avg_vol) if avg_vol > 0 else 0.0
            alert_mgr.add(alert)

    def on_bar(symbol: str, tf: int, bar):
        """
        Called for every closed bar (dict or Bar obj, historical or live).
        Creates state lazily, updates indicators, runs strategy engine.
        Also accumulates today's intraday volume and drives RVOL tracking.
        """
        b      = bar if isinstance(bar, dict) else bar.__dict__
        bar_ts = b.get('ts', b.get('timestamp', 0))

        if tf == 1:
            vol = b.get('volume', 0.0)
            # Track today's cumulative volume (avoid double-counting resampled TFs)
            if date.fromtimestamp(bar_ts) == date.today():
                today_volumes[symbol] = today_volumes.get(symbol, 0.0) + vol
            # RVOL: feed historical bars during bootstrap, score during live session
            tracker = rvol_trackers.get(symbol)
            if tracker is not None:
                if not is_live:
                    tracker.add_historical(bar_ts, vol)
                else:
                    spike = tracker.score(b)
                    if spike is not None and spike.rvol >= Config.RVOL_SPIKE_THRESHOLD:
                        vol_alert_mgr.add(spike)

        key = (symbol, tf)
        if key not in ind_sets:
            ind_sets[key] = IndicatorSet()
            engines[key]  = StrategyEngine(symbol, tf, on_alert)

        vals = ind_sets[key].update(bar)
        if vals is None:
            return   # indicators still warming up

        # Old bars: indicator warmup only — skip expensive episode detection.
        # Last 4 calendar days (~2 trading days) still run the full strategy engine.
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

    # ── dashboard (start immediately so nginx never gets 502) ─────────────────
    app = create_app(alert_mgr, vol_alert_mgr)
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

    # ── build universe ────────────────────────────────────────────────────────
    logger.info('Building universe (turnover filter >= ₹%.0fL)...',
                Config.TURNOVER_THRESHOLD / 1e5)
    symbols = build_universe(ctx)
    if not symbols:
        logger.error('Empty universe — aborting')
        sys.exit(1)
    logger.info('Universe: %d symbols', len(symbols))

    # Populate avg_volumes for relative-volume calculations on live alerts
    avg_volumes.update({s['symbol']: s.get('avg_daily_volume', 0.0) for s in symbols})

    # RVOL trackers — one per symbol, fed during bootstrap, scored during live
    rvol_trackers: dict[str, RvolTracker] = {s['symbol']: RvolTracker(s['symbol']) for s in symbols}

    # ── historical bootstrap ──────────────────────────────────────────────────
    bootstrap(ctx, symbols, on_bar)

    # Finalize RVOL trackers: compute per-minute averages from bootstrap history
    for t in rvol_trackers.values():
        t.finalize()
    logger.info('RVOL trackers ready: %d symbols', len(rvol_trackers))

    # Switch to live mode — now alerts are forwarded
    is_live = True
    logger.info('Live mode active — alerts are now forwarded to dashboard')

    # ── live bar feed (polls intraday_minute_data every 15 s) ─────────────────
    feed = LiveFeed(ctx, symbols, on_bar)
    feed.start()

    # ── heartbeat ─────────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(300)
            live_engines = len([k for k in engines if k[1] == 1])
            logger.info('Heartbeat — engines: %d (1m)  alerts today: %d',
                        live_engines, len(alert_mgr.get_all()))
    except KeyboardInterrupt:
        logger.info('Shutting down...')
        feed.stop()


if __name__ == '__main__':
    main()

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

from dhanhq import DhanContext

from config import Config
from indicators import IndicatorSet
from strategy_engine import StrategyEngine, BarRecord, Alert
from universe import build_universe
from dhan_feed import bootstrap, LiveFeed
from alert_manager import AlertManager
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

    # ── state stores (created lazily as new symbol-TF combos appear) ─────────
    ind_sets: dict[tuple, IndicatorSet]   = {}
    engines:  dict[tuple, StrategyEngine] = {}

    is_live = False   # suppress alerts during bootstrap

    def on_alert(alert: Alert):
        if not is_live:
            return
        # Only forward alerts whose bar closed within the last 3 bar-lengths.
        # Filters out historical bars seeded on the first live poll cycle.
        age_seconds = int(time.time()) - alert.ts
        if age_seconds > alert.tf * 3 * 60:
            return
        alert_mgr.add(alert)

    def on_bar(symbol: str, tf: int, bar):
        """
        Called for every closed bar (dict or Bar obj, historical or live).
        Creates state lazily, updates indicators, runs strategy engine.
        """
        key = (symbol, tf)
        if key not in ind_sets:
            ind_sets[key] = IndicatorSet()
            engines[key]  = StrategyEngine(symbol, tf, on_alert)

        vals = ind_sets[key].update(bar)
        if vals is None:
            return   # indicators still warming up

        b = bar if isinstance(bar, dict) else bar.__dict__  # handle both
        rec = BarRecord(
            ts=b.get('ts', b.get('timestamp', 0)),
            open=b.get('open', 0.0), high=b.get('high', 0.0),
            low=b.get('low', 0.0),   close=b.get('close', 0.0),
            volume=b.get('volume', 0.0),
            macd=vals['macd'], signal=vals['signal'],
            ema50=vals['ema50'], rsi=vals['rsi'],
        )
        engines[key].update(rec)

    # ── dashboard (start immediately so nginx never gets 502) ─────────────────
    app = create_app(alert_mgr)
    logger.info('Dashboard → http://%s:%d', Config.DASHBOARD_HOST, Config.DASHBOARD_PORT)

    flask_thread = threading.Thread(
        target=lambda: app.run(
            host=Config.DASHBOARD_HOST, port=Config.DASHBOARD_PORT,
            threaded=True, use_reloader=False,
        ),
        daemon=True, name='Dashboard'
    )
    flask_thread.start()
    time.sleep(1)  # give Flask a moment to bind the port

    # ── build universe ────────────────────────────────────────────────────────
    logger.info('Building universe (volume filter ≥ %d shares)...', Config.VOLUME_THRESHOLD)
    symbols = build_universe(ctx)
    if not symbols:
        logger.error('Empty universe — aborting')
        sys.exit(1)
    logger.info('Universe: %d symbols', len(symbols))

    # ── historical bootstrap ──────────────────────────────────────────────────
    bootstrap(ctx, symbols, on_bar)

    # Switch to live mode — now alerts are forwarded
    is_live = True
    logger.info('Live mode active — alerts are now forwarded to dashboard')

    # ── live bar feed (polls intraday_minute_data every 60 s) ─────────────────
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

"""
Historical bootstrap (20 days of 1-min bars per symbol) and
live WebSocket tick feed using Dhan MarketFeed.
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Callable

from dhanhq import DhanContext, dhanhq, MarketFeed

from config import Config

logger = logging.getLogger(__name__)

# Dhan MarketFeed constants for NSE equity
_NSE_EQ  = 1    # exchange segment integer
_QUOTE   = 17   # feed type: LTP + OHLCV


# ── historical bootstrap ──────────────────────────────────────────────────────

def _trading_days(n: int) -> list[str]:
    """Last n weekdays (Mon-Fri) ending yesterday, oldest first."""
    days, d = [], date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(days))


def _resample(bars_1m: list[dict], tf: int) -> list[dict]:
    """Aggregate a list of 1-min bar dicts into tf-min bar dicts."""
    groups: dict[int, dict] = {}
    for b in bars_1m:
        gts = (b['ts'] // (tf * 60)) * (tf * 60)
        if gts not in groups:
            groups[gts] = {**b, 'ts': gts}
        else:
            g = groups[gts]
            g['high']    = max(g['high'], b['high'])
            g['low']     = min(g['low'],  b['low'])
            g['close']   = b['close']
            g['volume'] += b['volume']
    return [groups[k] for k in sorted(groups)]


def _fetch_history(client: dhanhq, sec: dict, n_days: int) -> list[dict]:
    """Fetch n_days of 1-min bars for one security."""
    bars = []
    for day in _trading_days(n_days):
        try:
            resp = client.intraday_minute_data(
                security_id=sec["security_id"],
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
                from_date=day,
                to_date=day,
            )
            if not isinstance(resp, dict) or not isinstance(resp.get("data"), dict):
                continue
            data = resp["data"]
            tss  = data.get("timestamp", [])
            ops  = data.get("open",   [])
            his  = data.get("high",   [])
            los  = data.get("low",    [])
            cls  = data.get("close",  [])
            vls  = data.get("volume", [])
            for i, ts in enumerate(tss):
                bars.append({
                    'ts':     int(ts),
                    'open':   float(ops[i]),
                    'high':   float(his[i]),
                    'low':    float(los[i]),
                    'close':  float(cls[i]),
                    'volume': float(vls[i]) if i < len(vls) else 0.0,
                })
        except Exception as e:
            logger.debug("[%s] history %s: %s", sec["symbol"], day, e)
        time.sleep(0.08)
    bars.sort(key=lambda x: x['ts'])
    return bars


def bootstrap(dhan_context: DhanContext,
              symbols: list[dict],
              on_bar: Callable[[str, int, dict], None]):
    """
    Fetch 20 days of 1-min data for every symbol, derive 3/5/15-min bars,
    and call on_bar(symbol, tf, bar_dict) for each bar in chronological order.
    This seeds all indicator sets so they are warm when live trading starts.
    """
    logger.info("Bootstrapping %d symbols × %d days...",
                len(symbols), Config.HISTORY_DAYS)
    client = dhanhq(dhan_context)

    def _one(sec: dict):
        bars_1m = _fetch_history(client, sec, Config.HISTORY_DAYS)
        name    = sec["symbol"]
        for tf in Config.TIMEFRAMES:
            bars = bars_1m if tf == 1 else _resample(bars_1m, tf)
            for b in bars:
                on_bar(name, tf, b)
        return name, len(bars_1m)

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futs = {ex.submit(_one, s): s for s in symbols}
        done = 0
        for fut in as_completed(futs):
            name, n = fut.result()
            done += 1
            if done % 25 == 0 or done == len(symbols):
                logger.info("  Bootstrap: %d / %d symbols", done, len(symbols))

    logger.info("Bootstrap complete.")


# ── live WebSocket feed ───────────────────────────────────────────────────────

class LiveFeed:
    """
    Connects to Dhan MarketFeed WebSocket and calls
    on_tick(symbol_name, ltp, day_volume, unix_ts) on every tick.
    Auto-reconnects on disconnect.
    """

    def __init__(self, dhan_context: DhanContext,
                 symbols: list[dict],
                 on_tick: Callable[[str, float, float, int], None]):
        self._ctx       = dhan_context
        self._symbols   = symbols
        self._on_tick   = on_tick
        self._id_map    = {s["security_id"]: s["symbol"] for s in symbols}
        self._feed: MarketFeed | None = None
        self._thread: threading.Thread | None = None

    def _handle(self, msg):
        try:
            if not isinstance(msg, dict):
                return
            mtype = msg.get("type", "")
            if "Quote" not in mtype and "Ticker" not in mtype:
                return
            sid  = str(msg.get("security_id", ""))
            name = self._id_map.get(sid)
            if not name:
                return
            ltp  = float(msg.get("LTP") or msg.get("last_price") or 0)
            vol  = float(msg.get("volume") or msg.get("day_volume") or 0)
            ltt  = msg.get("LTT") or msg.get("last_trade_time")
            if ltt is None:
                ts = int(time.time())
            elif isinstance(ltt, (int, float)):
                ts = int(ltt) if ltt > 1_000_000_000 else int(ltt / 1000)
            else:
                import datetime as _dt
                ts = int(ltt.timestamp()) if isinstance(ltt, _dt.datetime) else int(time.time())
            if ltp > 0:
                self._on_tick(name, ltp, vol, ts)
        except Exception as e:
            logger.debug("Tick parse: %s", e)

    def start(self):
        instruments = [(_NSE_EQ, s["security_id"], _QUOTE) for s in self._symbols]

        def _run():
            backoff = 5
            while True:
                try:
                    logger.info("MarketFeed connecting (%d instruments)...", len(instruments))
                    self._feed = MarketFeed(
                        self._ctx, instruments,
                        version="v2", on_message=self._handle
                    )
                    self._feed.run_forever()
                    backoff = 5  # reset after clean disconnect
                except Exception as e:
                    err = str(e)
                    if "429" in err:
                        backoff = min(backoff * 2, 300)
                        logger.error("MarketFeed: rate-limited (429) — retry in %ds", backoff)
                    else:
                        backoff = min(int(backoff * 1.5), 60)
                        logger.error("MarketFeed: %s — retry in %ds", e, backoff)
                    time.sleep(backoff)

        self._thread = threading.Thread(target=_run, daemon=True, name="MarketFeed")
        self._thread.start()
        logger.info("Live feed started.")

    def stop(self):
        if self._feed:
            try:
                self._feed.close_connection()
            except Exception:
                pass

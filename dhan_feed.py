"""
Historical bootstrap (20 days of 1-min bars per symbol) and
live polling via intraday_minute_data (today's bars, every 60 s).
Uses the same endpoint as bootstrap — no live WebSocket subscription needed.
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Callable

from dhanhq import DhanContext, dhanhq

from config import Config

logger = logging.getLogger(__name__)

_LIVE_POLL_INTERVAL = 60   # seconds between live poll cycles


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


# ── live intraday polling feed ────────────────────────────────────────────────

class LiveFeed:
    """
    Polls intraday_minute_data for today every 60 s and calls
    on_bar(symbol, tf, bar_dict) for each new closed bar across 1/3/5/15-min.
    Uses the same endpoint as bootstrap — no live data subscription required.
    """

    def __init__(self, dhan_context: DhanContext,
                 symbols: list[dict],
                 on_bar: Callable[[str, int, dict], None]):
        self._client  = dhanhq(dhan_context)
        self._symbols = symbols
        self._on_bar  = on_bar
        self._last_ts: dict[tuple, int] = {}   # (symbol, tf) -> last processed ts
        self._running = False
        self._thread: threading.Thread | None = None

    def _fetch_today(self, sec: dict) -> list[dict]:
        today = date.today().isoformat()
        try:
            resp = self._client.intraday_minute_data(
                security_id=sec["security_id"],
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
                from_date=today,
                to_date=today,
            )
            if not isinstance(resp, dict) or not isinstance(resp.get("data"), dict):
                return []
            data = resp["data"]
            tss = data.get("timestamp", [])
            ops = data.get("open",   [])
            his = data.get("high",   [])
            los = data.get("low",    [])
            cls = data.get("close",  [])
            vls = data.get("volume", [])
            bars = []
            for i, ts in enumerate(tss):
                bars.append({
                    'ts':     int(ts),
                    'open':   float(ops[i]),
                    'high':   float(his[i]),
                    'low':    float(los[i]),
                    'close':  float(cls[i]),
                    'volume': float(vls[i]) if i < len(vls) else 0.0,
                })
            bars.sort(key=lambda x: x['ts'])
            return bars
        except Exception as e:
            logger.debug("Live fetch %s: %s", sec["symbol"], e)
            return []

    def _process_symbol(self, sec: dict):
        name    = sec["symbol"]
        bars_1m = self._fetch_today(sec)
        if not bars_1m:
            return
        # Drop the last bar — it may still be forming
        if len(bars_1m) > 1:
            bars_1m = bars_1m[:-1]

        for tf in [1, 3, 5, 15]:
            bars    = bars_1m if tf == 1 else _resample(bars_1m, tf)
            last_ts = self._last_ts.get((name, tf), 0)
            new_bars = [b for b in bars if b['ts'] > last_ts]
            for b in new_bars:
                self._on_bar(name, tf, b)
            if new_bars:
                self._last_ts[(name, tf)] = new_bars[-1]['ts']

    def _poll_all(self):
        logged = False
        for sec in self._symbols:
            if not self._running:
                break
            self._process_symbol(sec)
            if not logged:
                logger.info("Live poll cycle running (%d symbols)...", len(self._symbols))
                logged = True
            time.sleep(0.08)   # 12.5 req/s, well under 20 req/s limit

    def start(self):
        self._running = True

        def _run():
            logger.info("Live feed started (intraday_minute_data polling every %ds, %d symbols).",
                        _LIVE_POLL_INTERVAL, len(self._symbols))
            while self._running:
                t0 = time.time()
                self._poll_all()
                elapsed = time.time() - t0
                logger.info("Live poll cycle done in %.1fs.", elapsed)
                wait = max(0.0, _LIVE_POLL_INTERVAL - elapsed)
                if self._running and wait > 0:
                    time.sleep(wait)

        self._thread = threading.Thread(target=_run, daemon=True, name="LiveFeed")
        self._thread.start()

    def stop(self):
        self._running = False

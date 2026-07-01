"""
Historical bootstrap (20 days of 1-min bars per symbol) and
live REST-polling tick feed using Dhan quote_data API.
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

_POLL_INTERVAL = 5   # seconds between REST polls
_CHUNK_SIZE    = 50  # securities per quote_data call (safe batch limit)


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


# ── live REST-polling feed ────────────────────────────────────────────────────

class LiveFeed:
    """
    Polls Dhan ticker_data REST API every 5 s for all symbols and calls
    on_tick(symbol_name, ltp, day_volume, unix_ts) for each quote.
    Avoids WebSocket connection-rate limits entirely.
    """

    def __init__(self, dhan_context: DhanContext,
                 symbols: list[dict],
                 on_tick: Callable[[str, float, float, int], None]):
        self._client           = dhanhq(dhan_context)
        self._symbols          = symbols
        self._on_tick          = on_tick
        self._id_map           = {s["security_id"]: s["symbol"] for s in symbols}
        self._running          = False
        self._thread: threading.Thread | None = None
        self._first_poll_logged = False

    def _parse_quote(self, sid: str, q) -> tuple[float, float]:
        """Return (ltp, day_volume) from a quote object (dict or nested)."""
        if isinstance(q, dict):
            ltp = float(q.get("last_price") or q.get("LTP") or
                        q.get("lastTradedPrice") or q.get("ltp") or 0)
            vol = float(q.get("volume") or q.get("day_volume") or
                        q.get("totalVolume") or q.get("tot_buy_quan") or 0)
            return ltp, vol
        return 0.0, 0.0

    def _poll_chunk(self, chunk: list[str], ts: int):
        """Fetch LTP for one chunk of security IDs and fire on_tick."""
        try:
            resp = self._client.ticker_data(securities={"NSE_EQ": chunk})
            if not self._first_poll_logged:
                logger.info("First poll response: %s", str(resp)[:600])
                self._first_poll_logged = True
            if not isinstance(resp, dict) or resp.get("status") != "success":
                logger.info("Poll non-success: %s", str(resp)[:300])
                return
            data = resp.get("data", {})
            # Format A: {"NSE_EQ": {"<sid>": {"LTP": ..., ...}, ...}}
            if isinstance(data, dict):
                segment = data.get("NSE_EQ", {})
                if isinstance(segment, dict):
                    for sid, q in segment.items():
                        name = self._id_map.get(str(sid))
                        if not name:
                            continue
                        ltp, vol = self._parse_quote(sid, q)
                        if ltp > 0:
                            self._on_tick(name, ltp, vol, ts)
                    return
            # Format B: [{"security_id": "...", "last_price": ..., ...}]
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    sid  = str(item.get("security_id") or item.get("securityId") or "")
                    name = self._id_map.get(sid)
                    if not name:
                        continue
                    ltp, vol = self._parse_quote(sid, item)
                    if ltp > 0:
                        self._on_tick(name, ltp, vol, ts)
        except Exception as e:
            logger.error("Poll chunk error: %s", e)

    def _poll(self):
        sec_ids = list(self._id_map.keys())
        ts = int(time.time())
        for i in range(0, len(sec_ids), _CHUNK_SIZE):
            self._poll_chunk(sec_ids[i:i + _CHUNK_SIZE], ts)
            if i + _CHUNK_SIZE < len(sec_ids):
                time.sleep(0.1)  # small gap between chunks

    def start(self):
        self._running = True

        def _run():
            logger.info("Live feed started (REST polling every %ds, %d symbols).",
                        _POLL_INTERVAL, len(self._symbols))
            while self._running:
                t0 = time.time()
                self._poll()
                elapsed = time.time() - t0
                wait = max(0.0, _POLL_INTERVAL - elapsed)
                if wait:
                    time.sleep(wait)

        self._thread = threading.Thread(target=_run, daemon=True, name="LiveFeed")
        self._thread.start()

    def stop(self):
        self._running = False

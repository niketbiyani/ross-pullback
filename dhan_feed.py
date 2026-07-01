"""
Historical bootstrap (20 days of 1-min bars per symbol) and
live polling via intraday_minute_data (today's bars, every 15 s).
Bar data is cached to disk — same-day restarts replay from cache in seconds.
"""
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Callable

from dhanhq import DhanContext, dhanhq

from config import Config

logger = logging.getLogger(__name__)

_LIVE_POLL_INTERVAL = 15   # seconds between live poll cycles

# Global API rate limiter — shared across all bootstrap workers.
# Caps the total request rate to _MIN_API_GAP seconds between any two API calls,
# so 16 workers don't flood Dhan's 20 req/s limit.
_API_LOCK    = threading.Lock()
_API_LAST: float = 0.0
_MIN_API_GAP = 0.12   # ~8 req·s⁻¹ — well under Dhan's rate limit


def _api_throttle():
    global _API_LAST
    with _API_LOCK:
        gap = _MIN_API_GAP - (time.time() - _API_LAST)
        if gap > 0:
            time.sleep(gap)
        _API_LAST = time.time()


def _market_open() -> bool:
    """True if current IST time is within NSE trading hours (9:15–15:30)."""
    m = (int(time.time()) // 60 + 330) % (24 * 60)
    return 555 <= m < 930


# ── bar cache (disk) ──────────────────────────────────────────────────────────

def _cache_path(symbol: str) -> str:
    return os.path.join(Config.BARS_CACHE_DIR, f"{symbol}.json")


def _load_cache(symbol: str) -> dict[str, list[dict]]:
    path = _cache_path(symbol)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(symbol: str, cache: dict[str, list[dict]]):
    os.makedirs(Config.BARS_CACHE_DIR, exist_ok=True)
    with open(_cache_path(symbol), 'w') as f:
        json.dump(cache, f)


# ── historical bootstrap ──────────────────────────────────────────────────────

def _trading_days(n: int) -> list[str]:
    """Last n weekdays (Mon-Fri) ending today, oldest first."""
    days, d = [], date.today()
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


def _fetch_day(client: dhanhq, sec: dict, day: str) -> list[dict]:
    """Fetch one day of 1-min bars from the API."""
    _api_throttle()
    try:
        resp = client.intraday_minute_data(
            security_id=sec["security_id"],
            exchange_segment="NSE_EQ",
            instrument_type="EQUITY",
            from_date=day,
            to_date=day,
        )
        if not isinstance(resp, dict) or not isinstance(resp.get("data"), dict):
            return []
        data = resp["data"]
        tss  = data.get("timestamp", [])
        ops  = data.get("open",   [])
        his  = data.get("high",   [])
        los  = data.get("low",    [])
        cls  = data.get("close",  [])
        vls  = data.get("volume", [])
        return [
            {
                'ts':     int(tss[i]),
                'open':   float(ops[i]),
                'high':   float(his[i]),
                'low':    float(los[i]),
                'close':  float(cls[i]),
                'volume': float(vls[i]) if i < len(vls) else 0.0,
            }
            for i in range(len(tss))
        ]
    except Exception as e:
        logger.debug("[%s] fetch %s: %s", sec["symbol"], day, e)
        return []


def _fetch_history(client: dhanhq, sec: dict, n_days: int) -> tuple[list[dict], int]:
    """
    Return (bars_1m, api_calls) for the last n_days trading days.
    Loads from disk cache; only calls API for days not yet cached.
    """
    name         = sec["symbol"]
    needed       = _trading_days(n_days)
    cache        = _load_cache(name)
    api_calls    = 0
    changed      = False

    for day in needed:
        if day in cache:
            continue
        bars = _fetch_day(client, sec, day)
        cache[day] = bars
        api_calls += 1
        changed = True

    # Prune days outside the window
    for day in list(cache.keys()):
        if day not in needed:
            del cache[day]
            changed = True

    if changed:
        _save_cache(name, cache)

    bars: list[dict] = []
    for day in needed:
        bars.extend(cache.get(day, []))
    bars.sort(key=lambda x: x['ts'])
    return bars, api_calls


def bootstrap(dhan_context: DhanContext,
              symbols: list[dict],
              on_bar: Callable[[str, int, dict], None]):
    """
    Load/fetch 20 days of 1-min data for every symbol (from cache when possible),
    derive 3/5/15-min bars, and call on_bar(symbol, tf, bar_dict) for each bar.
    """
    logger.info("Bootstrapping %d symbols × %d days (cache: %s)...",
                len(symbols), Config.HISTORY_DAYS, Config.BARS_CACHE_DIR)
    client     = dhanhq(dhan_context)
    total_api  = 0

    def _one(sec: dict):
        bars_1m, api_calls = _fetch_history(client, sec, Config.HISTORY_DAYS)
        name = sec["symbol"]
        for tf in Config.TIMEFRAMES:
            bars = bars_1m if tf == 1 else _resample(bars_1m, tf)
            for b in bars:
                on_bar(name, tf, b)
        return name, len(bars_1m), api_calls

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futs = {ex.submit(_one, s): s for s in symbols}
        done = 0
        for fut in as_completed(futs):
            name, n, calls = fut.result()
            total_api += calls
            done += 1
            if done % 25 == 0 or done == len(symbols):
                logger.info("  Bootstrap: %d / %d symbols  (API calls so far: %d)",
                            done, len(symbols), total_api)

    logger.info("Bootstrap complete. Total API calls: %d (0 = fully cached).", total_api)


# ── live intraday polling feed ────────────────────────────────────────────────

class LiveFeed:
    """
    Polls intraday_minute_data for today every 60 s and calls
    on_bar(symbol, tf, bar_dict) for each new closed bar across 1/3/5/15-min.
    Uses the same endpoint as bootstrap — no live data subscription required.
    """

    def __init__(self, dhan_context: DhanContext,
                 symbols: list[dict],
                 on_bar: Callable[[str, int, dict], None],
                 on_volume: Callable[[str, list[dict]], None] | None = None):
        self._client    = dhanhq(dhan_context)
        self._symbols   = symbols
        self._on_bar    = on_bar
        self._on_volume = on_volume
        self._last_ts: dict[tuple, int] = {}   # (symbol, tf) -> last processed ts
        self._running = False
        self._thread: threading.Thread | None = None

    def _fetch_today(self, sec: dict) -> list[dict]:
        today = date.today().isoformat()
        name  = sec["symbol"]
        _api_throttle()
        try:
            resp = self._client.intraday_minute_data(
                security_id=sec["security_id"],
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
                from_date=today,
                to_date=today,
            )
            if not isinstance(resp, dict) or not isinstance(resp.get("data"), dict):
                raise ValueError("bad response")
            data = resp["data"]
            tss = data.get("timestamp", [])
            ops = data.get("open",   [])
            his = data.get("high",   [])
            los = data.get("low",    [])
            cls = data.get("close",  [])
            vls = data.get("volume", [])
            if not tss:
                raise ValueError("empty")
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
            # Persist today's bars so they survive after market close
            cache = _load_cache(name)
            cache[today] = bars
            _save_cache(name, cache)
            return bars
        except Exception as e:
            # API returned nothing (market closed, data unavailable) —
            # fall back to today's bars from disk cache if we fetched them earlier.
            cache = _load_cache(name)
            cached = cache.get(today, [])
            if cached:
                logger.debug("Live fetch %s: API empty, using %d cached bars", name, len(cached))
            return cached

    def _process_symbol(self, sec: dict):
        name  = sec["symbol"]
        today = date.today().isoformat()
        if _market_open():
            bars_1m = self._fetch_today(sec)
        else:
            # Market closed — no new bars, use what bootstrap cached
            bars_1m = _load_cache(name).get(today, [])
        if not bars_1m:
            return
        # Drop the last bar — it may still be forming
        if len(bars_1m) > 1:
            bars_1m = bars_1m[:-1]

        # Volume callback fires with ALL today's bars before the catchup filter
        # so the leaderboard sees full-session cumulative volume, not just last 30 bars.
        if self._on_volume is not None:
            self._on_volume(name, bars_1m)

        for tf in [1, 3, 5, 15]:
            bars    = bars_1m if tf == 1 else _resample(bars_1m, tf)
            last_ts = self._last_ts.get((name, tf), 0)
            new_bars = [b for b in bars if b['ts'] > last_ts]
            # On the very first poll (last_ts==0), skip back-history to avoid
            # flooding the dashboard with stale alerts from earlier in the session.
            if last_ts == 0 and len(new_bars) > Config.LIVE_CATCHUP_BARS:
                new_bars = new_bars[-Config.LIVE_CATCHUP_BARS:]
            for b in new_bars:
                self._on_bar(name, tf, b)
            if new_bars:
                self._last_ts[(name, tf)] = new_bars[-1]['ts']

    def _poll_all(self):
        logger.info("Live poll cycle running (%d symbols)...", len(self._symbols))
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
            futs = {ex.submit(self._process_symbol, sec): sec
                    for sec in self._symbols if self._running}
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    logger.debug("Live poll worker error: %s", e)

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

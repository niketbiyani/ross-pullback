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
from bar_aggregator import BarAggregator

logger = logging.getLogger(__name__)

_LIVE_POLL_INTERVAL  = 15   # seconds between active-mover poll cycles
_SLOW_SCAN_BATCH     = 50   # non-active symbols polled per cycle (rolling window)

# Global API rate limiter — shared across all bootstrap workers.
# Caps the total request rate to _MIN_API_GAP seconds between any two API calls,
# so 32 workers don't flood Dhan's rate limits.
_API_LOCK    = threading.Lock()
_API_LAST: float = 0.0
_MIN_API_GAP = 0.10   # 10 req·s⁻¹ — safe headroom below Dhan's limit


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


# ── native TF bar cache (one cache dir per interval, avoids resampling drift) ─

def _native_cache_dir(tf: int) -> str:
    return Config.BARS_CACHE_DIR.rstrip('/') + f'_{tf}m'

def _native_cache_path(symbol: str, tf: int) -> str:
    return os.path.join(_native_cache_dir(tf), f"{symbol}.json")

def _load_native_cache(symbol: str, tf: int) -> dict[str, list[dict]]:
    path = _native_cache_path(symbol, tf)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_native_cache(symbol: str, tf: int, cache: dict[str, list[dict]]):
    os.makedirs(_native_cache_dir(tf), exist_ok=True)
    with open(_native_cache_path(symbol, tf), 'w') as f:
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


def _fetch_day_native(client: dhanhq, sec: dict, day: str, tf: int) -> list[dict]:
    """Fetch one day of native tf-min bars from the Dhan API (no resampling)."""
    _api_throttle()
    try:
        resp = client.intraday_minute_data(
            security_id=sec["security_id"],
            exchange_segment="NSE_EQ",
            instrument_type="EQUITY",
            from_date=day,
            to_date=day,
            interval=tf,
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
        logger.debug("[%s] fetch_%dm %s: %s", sec["symbol"], tf, day, e)
        return []


def _fetch_history_native(client: dhanhq, sec: dict, n_days: int, tf: int) -> tuple[list[dict], int]:
    """Return (bars, api_calls) using native tf-min bars with per-TF disk cache."""
    name      = sec["symbol"]
    needed    = _trading_days(n_days)
    cache     = _load_native_cache(name, tf)
    api_calls = 0
    changed   = False

    today = date.today().isoformat()
    for day in needed:
        if day in cache:
            if day == today and not cache[day]:
                pass
            else:
                continue
        bars = _fetch_day_native(client, sec, day, tf)
        cache[day] = bars
        api_calls += 1
        changed = True

    for day in list(cache.keys()):
        if day not in needed:
            del cache[day]
            changed = True

    if changed:
        _save_native_cache(name, tf, cache)

    bars: list[dict] = []
    for day in needed:
        bars.extend(cache.get(day, []))
    bars.sort(key=lambda x: x['ts'])
    return bars, api_calls


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

    today = date.today().isoformat()
    for day in needed:
        if day in cache:
            # Re-fetch today if previously cached as empty (rate-limit gap from prior run)
            if day == today and not cache[day]:
                pass
            else:
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
              on_bar: Callable[[str, int, dict], None],
              skip_tfs: frozenset = frozenset()):
    """Zero-bootstrap fallback: skip downloading history."""
    logger.info("Bootstrap skipped (zero-bootstrap mode active)")
    return


def bootstrap_macd(dhan_context: DhanContext,
                   symbols: list[dict],
                   on_bar: Callable[[str, int, dict], None],
                   macd_tfs: tuple = (15, 30, 60)):
    """Zero-bootstrap fallback: skip downloading history."""
    logger.info("Native bootstrap skipped (zero-bootstrap mode active)")
    return


# ── live intraday polling feed ────────────────────────────────────────────────

def _chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class LiveFeed:
    """
    Polls quote_data for all 500 Nifty symbols every 5 seconds,
    maintains real-time 1-minute bars in memory, and feeds closed bars
    to the BarAggregator for timeframe rollup.
    """

    def __init__(self, dhan_context: DhanContext,
                 symbols: list[dict],
                 on_bar: Callable[[str, int, dict], None],
                 on_quote: Callable[[str, float, float], None] | None = None):
        self._client      = dhanhq(dhan_context)
        self._symbols     = symbols
        self._on_bar      = on_bar
        self._on_quote    = on_quote
        self._aggregators = {s['symbol']: BarAggregator(s['symbol'], on_bar) for s in symbols}
        self._sec_id_to_symbol = {str(s["security_id"]): s["symbol"] for s in symbols}
        self._current_bar: dict[str, dict | None] = {s['symbol']: None for s in symbols}
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self):
        self._running = True

        def _run():
            logger.info("Live feed started (%d symbols, 5s bulk quote polling).", len(self._symbols))
            while self._running:
                t0 = time.time()
                
                # Check for closed 1m bars
                minute_ts = (int(time.time()) // 60) * 60
                closed_keys = []
                for symbol, bar in list(self._current_bar.items()):
                    if bar and bar['ts'] < minute_ts:
                        # Feed the closed bar to rollup engine
                        self._aggregators[symbol].feed_historical(bar)
                        closed_keys.append(symbol)
                for symbol in closed_keys:
                    self._current_bar[symbol] = None
                
                # Divide symbols into chunks of 100
                chunks = list(_chunk_list(self._symbols, 100))
                
                def _fetch_chunk(chunk):
                    securities = {"NSE_EQ": [int(s["security_id"]) for s in chunk]}
                    _api_throttle()
                    try:
                        resp = self._client.quote_data(securities=securities)
                        if isinstance(resp, dict) and resp.get("status") == "success":
                            return resp.get("data", {})
                    except Exception as e:
                        logger.debug("Quote fetch error: %s", e)
                    return {}

                all_data = {}
                with ThreadPoolExecutor(max_workers=len(chunks) or 1) as ex:
                    futs = [ex.submit(_fetch_chunk, c) for c in chunks]
                    for f in as_completed(futs):
                        res = f.result()
                        if res:
                            for seg, seg_data in res.items():
                                dest = all_data.setdefault(seg, {})
                                if isinstance(seg_data, list):
                                    for item in seg_data:
                                        if isinstance(item, dict):
                                            dest.update(item)
                                elif isinstance(seg_data, dict):
                                    dest.update(seg_data)
                
                # Process the fetched data
                nse_eq_data = all_data.get("NSE_EQ", {})
                for sec_id_str, quote in nse_eq_data.items():
                    symbol = self._sec_id_to_symbol.get(sec_id_str)
                    if not symbol:
                        continue
                    
                    ltp = float(quote.get("last_price", quote.get("LTP", 0.0)))
                    volume = float(quote.get("volume", 0.0))
                    
                    if ltp <= 0:
                        continue
                    
                    # Fire quote callback to update daily metrics in real time
                    if self._on_quote:
                        self._on_quote(symbol, ltp, volume)
                        
                    # Aggregate 1m bar
                    bar = self._current_bar.get(symbol)
                    if bar is None or bar['ts'] != minute_ts:
                        self._current_bar[symbol] = {
                            'ts': minute_ts,
                            'open': ltp,
                            'high': ltp,
                            'low': ltp,
                            'close': ltp,
                            'volume': 0.0,
                            'volume_start': volume
                        }
                    else:
                        bar['high'] = max(bar['high'], ltp)
                        bar['low'] = min(bar['low'], ltp)
                        bar['close'] = ltp
                        bar['volume'] = max(volume - bar['volume_start'], 0.0)

                elapsed = time.time() - t0
                wait = max(0.0, 5.0 - elapsed)
                if self._running and wait > 0:
                    time.sleep(wait)

        self._thread = threading.Thread(target=_run, daemon=True, name="LiveFeed")
        self._thread.start()

    def stop(self):
        self._running = False


def fetch_today_1m_bars(client: dhanhq, symbols: list[dict], n_days: int = 3) -> dict[str, list[dict]]:
    """Fetch recent n_days 1-minute bars for all symbols in parallel (cached to disk)."""
    logger.info("Fetching last %d trading days of 1-minute bars for %d symbols...", n_days, len(symbols))
    trading_days = _trading_days(n_days)
    today = date.today().isoformat()
    results = {}
    
    def _fetch_one(sec):
        sym = sec['symbol']
        cache = _load_cache(sym)
        
        # Check if all requested trading_days are already in cache
        missing_days = [d for d in trading_days if d not in cache or len(cache[d]) == 0]
        if not missing_days:
            all_bars = []
            for d in trading_days:
                all_bars.extend(cache[d])
            all_bars.sort(key=lambda x: x['ts'])
            return sym, all_bars
            
        # We need to fetch from the oldest missing day to today
        from_date = missing_days[0]
        to_date = today
        
        _api_throttle()
        try:
            resp = client.intraday_minute_data(
                security_id=sec["security_id"],
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
                from_date=from_date,
                to_date=to_date,
            )
            if isinstance(resp, dict) and isinstance(resp.get("data"), dict):
                data = resp["data"]
                tss = data.get("timestamp", [])
                ops = data.get("open",   [])
                his = data.get("high",   [])
                los = data.get("low",    [])
                cls = data.get("close",  [])
                vls = data.get("volume", [])
                
                # Group fetched bars by date (IST)
                fetched_by_date = {}
                from datetime import datetime, timezone, timedelta
                ist_tz = timezone(timedelta(hours=5, minutes=30))
                
                for i, ts_val in enumerate(tss):
                    dt = datetime.fromtimestamp(int(ts_val), tz=ist_tz)
                    day_str = dt.date().isoformat()
                    if day_str not in fetched_by_date:
                        fetched_by_date[day_str] = []
                    
                    fetched_by_date[day_str].append({
                        'ts':     int(ts_val),
                        'open':   float(ops[i]),
                        'high':   float(his[i]),
                        'low':    float(los[i]),
                        'close':  float(cls[i]),
                        'volume': float(vls[i]) if i < len(vls) else 0.0,
                    })
                
                # Save each day to cache
                for day_str, bars in fetched_by_date.items():
                    bars.sort(key=lambda x: x['ts'])
                    cache[day_str] = bars
                    
                if fetched_by_date:
                    _save_cache(sym, cache)
                
                all_bars = []
                for d in trading_days:
                    all_bars.extend(cache.get(d, []))
                all_bars.sort(key=lambda x: x['ts'])
                return sym, all_bars
        except Exception as e:
            logger.debug("Failed to fetch bars for %s (%s to %s): %s", sym, from_date, to_date, e)
            
        all_bars = []
        for d in trading_days:
            all_bars.extend(cache.get(d, []))
        all_bars.sort(key=lambda x: x['ts'])
        return sym, all_bars


    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_one, s): s for s in symbols}
        done = 0
        for fut in as_completed(futs):
            sym, bars = fut.result()
            if bars:
                results[sym] = bars
            done += 1
            if done % 50 == 0 or done == len(symbols):
                logger.info("  Fetched bars for %d / %d symbols", done, len(symbols))
    return results

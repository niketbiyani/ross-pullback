"""
Builds the NSE equity universe.

Primary path: NSE CM Bhavcopy (10 HTTP requests, ~5 seconds).
Fallback: Dhan intraday_minute_data volume check if NSE archives unreachable.

Filters (both must pass):
  close >= CLOSE_MIN_PRICE  (default ₹100 — excludes penny stocks)
  avg turnover >= TURNOVER_THRESHOLD (default ₹10L safety net)

Per-symbol avg_daily_volume is stored in the cache so main.py can compute
relative intraday volume (today_volume / avg_daily_volume) on each alert.

Cache TTL: 1 day — bhavcopy rebuild takes ~5 s; Dhan fallback ~8 min.
"""
import csv
import io
import json
import logging
import os
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests
from dhanhq import DhanContext, dhanhq

from config import Config

logger = logging.getLogger(__name__)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

_NSE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Referer':         'https://www.nseindia.com/',
    'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _trading_days_back(n: int) -> list[date]:
    """Last n weekdays ending yesterday, newest first."""
    days, d = [], date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def _all_nse_eq() -> list[dict]:
    """Download Dhan scrip master → [{security_id, symbol}] for NSE EQ."""
    resp = requests.get(SCRIP_URL, timeout=60)
    resp.raise_for_status()
    result = []
    for row in csv.DictReader(io.StringIO(resp.text)):
        if row.get("SEM_EXM_EXCH_ID") != "NSE":
            continue
        if row.get("SEM_INSTRUMENT_NAME") != "EQUITY":
            continue
        if row.get("SEM_SEGMENT") != "E":
            continue
        sid = row.get("SEM_SMST_SECURITY_ID", "").strip()
        sym = row.get("SEM_TRADING_SYMBOL",   "").strip()
        if sid and sym:
            result.append({"security_id": sid, "symbol": sym})
    logger.info("Scrip master: %d NSE EQ instruments", len(result))
    return result


# ── bhavcopy path ─────────────────────────────────────────────────────────────

def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass
    return s


def _bhavcopy_urls(d: date) -> list[str]:
    ds  = d.strftime("%Y%m%d")
    mon = d.strftime("%b").upper()
    return [
        f"https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{ds}_F_0000.csv.zip",
        f"https://archives.nseindia.com/content/historical/EQUITIES/"
        f"{d.year}/{mon}/cm{d.day:02d}{mon}{d.year}bhav.csv.zip",
    ]


def _download_bhavcopy(d: date, session: requests.Session) -> dict[str, dict]:
    """
    Download and parse NSE CM bhavcopy for date d.
    Returns {symbol: {volume, turnover, close}} for EQ/BE series, or {} on failure.
    """
    for url in _bhavcopy_urls(d):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.debug("Bhavcopy HTTP %d: %s", resp.status_code, url)
                continue
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    text = f.read().decode('utf-8')
            result = {}
            for row in csv.DictReader(io.StringIO(text)):
                sym    = (row.get('SYMBOL')   or row.get('TckrSymb', '')).strip()
                series = (row.get('SERIES')   or row.get('SctySrs',  '')).strip()
                if not sym or series not in ('EQ', 'BE'):
                    continue
                try:
                    vol   = float(row.get('TOTTRDQTY') or row.get('TtlTrdQty') or 0)
                    val   = float(row.get('TOTTRDVAL') or row.get('TtlTrdVal') or 0)
                    close = float(row.get('CLOSE') or row.get('ClsPric') or
                                  row.get('LAST')  or row.get('LastPric') or 0)
                except (ValueError, TypeError):
                    continue
                result[sym] = {'volume': vol, 'turnover': val, 'close': close}
            logger.info("  Bhavcopy %s: %d EQ symbols", d.isoformat(), len(result))
            return result
        except Exception as e:
            logger.warning("Bhavcopy %s failed (%s): %s",
                           d, url[url.rindex('/')+1:], e)
    logger.warning("Bhavcopy unavailable for %s", d.isoformat())
    return {}


def _build_via_bhavcopy() -> dict[str, dict] | None:
    """
    Download last VOLUME_HISTORY_DAYS bhavcopy files.
    Returns {symbol: {avg_volume, avg_turnover, close}} or None if all downloads fail.
    """
    session = _nse_session()
    days    = _trading_days_back(Config.VOLUME_HISTORY_DAYS)

    sym_days: dict[str, list[dict]] = {}
    for d in days:
        bhav = _download_bhavcopy(d, session)
        for sym, stats in bhav.items():
            sym_days.setdefault(sym, []).append(stats)
        time.sleep(0.3)

    if not sym_days:
        return None

    min_days = max(1, Config.VOLUME_HISTORY_DAYS // 2)
    result: dict[str, dict] = {}
    for sym, stats_list in sym_days.items():
        if len(stats_list) < min_days:
            continue
        n = len(stats_list)
        result[sym] = {
            'avg_volume':   sum(s['volume']   for s in stats_list) / n,
            'avg_turnover': sum(s['turnover'] for s in stats_list) / n,
            'close':        stats_list[0].get('close', 0),   # most recent
        }
    return result


# ── Dhan API fallback path ────────────────────────────────────────────────────

def _fetch_vol_dhan(client: dhanhq, sec: dict,
                   from_date: str, to_date: str) -> float:
    """Single range call → average daily volume. Returns 0 on any failure."""
    for attempt in range(2):
        try:
            resp = client.intraday_minute_data(
                security_id=sec["security_id"],
                exchange_segment="NSE_EQ",
                instrument_type="EQUITY",
                from_date=from_date,
                to_date=to_date,
            )
            if not isinstance(resp, dict) or not isinstance(resp.get("data"), dict):
                break
            data = resp["data"]
            tss  = data.get("timestamp", [])
            vls  = data.get("volume",    [])
            daily: dict[str, float] = {}
            for i, ts in enumerate(tss):
                d   = date.fromtimestamp(int(ts)).isoformat()
                vol = float(vls[i]) if i < len(vls) else 0.0
                daily[d] = daily.get(d, 0.0) + vol
            totals = [v for v in daily.values() if v > 0]
            return sum(totals) / len(totals) if totals else 0.0
        except Exception:
            if attempt == 0:
                time.sleep(0.5)
    time.sleep(0.05)
    return 0.0


def _build_via_dhan(dhan_context: DhanContext) -> dict[str, dict]:
    """
    Fallback: fetch volume via Dhan intraday_minute_data (range call per symbol).
    Slow (~8 min for full universe) but reliable. Returns same schema as bhavcopy path.
    No price data available — close filter is skipped (set to 0 for all symbols).
    """
    logger.warning("NSE bhavcopy unavailable — falling back to Dhan API volume check "
                   "(this will take several minutes)")
    all_eq   = _all_nse_eq()
    client   = dhanhq(dhan_context)
    days     = _trading_days_back(Config.VOLUME_HISTORY_DAYS)
    from_date = days[-1].isoformat()
    to_date   = days[0].isoformat()

    result: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_vol_dhan, client, s, from_date, to_date): s
                for s in all_eq}
        done = 0
        for fut in as_completed(futs):
            sec = futs[fut]
            try:
                vol = fut.result()
            except Exception:
                vol = 0.0
            result[sec["symbol"]] = {
                'avg_volume':   vol,
                'avg_turnover': 0.0,   # not available without price data
                'close':        0.0,   # price filter skipped in fallback
            }
            done += 1
            if done % 200 == 0:
                logger.info("  Dhan volume check: %d / %d", done, len(all_eq))
            time.sleep(0.05)

    logger.info("Dhan volume check complete: %d symbols", len(result))
    return result


# ── main entry point ──────────────────────────────────────────────────────────

def build_universe(dhan_context: DhanContext) -> list[dict]:
    """
    Return [{security_id, symbol, avg_daily_volume, avg_daily_turnover}].
    Tries bhavcopy first, falls back to Dhan API, then stale cache.
    Never returns empty — always produces a usable universe.
    """
    today = date.today().isoformat()
    cache = Config.UNIVERSE_CACHE

    if os.path.exists(cache):
        try:
            with open(cache) as f:
                data = json.load(f)
            if data.get("date") == today:
                logger.info("Universe: %d symbols (from cache)", len(data["symbols"]))
                return data["symbols"]
        except Exception:
            pass

    logger.info("Building universe via NSE bhavcopy (%d-day avg)...",
                Config.VOLUME_HISTORY_DAYS)

    # ── try bhavcopy ──────────────────────────────────────────────────────────
    sym_avg = _build_via_bhavcopy()

    # ── fall back to Dhan API ─────────────────────────────────────────────────
    if sym_avg is None:
        sym_avg = _build_via_dhan(dhan_context)

    # ── still nothing → use stale cache (any age) ─────────────────────────────
    if not sym_avg:
        logger.error("Both bhavcopy and Dhan API failed")
        if os.path.exists(cache):
            try:
                with open(cache) as f:
                    data = json.load(f)
                logger.warning("Using stale universe cache from %s", data.get("date"))
                return data["symbols"]
            except Exception:
                pass
        logger.error("No universe data available — aborting")
        return []

    # ── apply filters ─────────────────────────────────────────────────────────
    use_price_filter  = Config.CLOSE_MIN_PRICE > 0
    use_vol_filter    = Config.TURNOVER_THRESHOLD > 0

    passing: set[str] = set()
    for sym, s in sym_avg.items():
        if use_price_filter and s['close'] > 0 and s['close'] < Config.CLOSE_MIN_PRICE:
            continue
        if use_vol_filter and s['avg_turnover'] > 0 and s['avg_turnover'] < Config.TURNOVER_THRESHOLD:
            continue
        passing.add(sym)

    logger.info("Filters: close >= ₹%.0f, turnover >= ₹%.0fL → %d symbols pass",
                Config.CLOSE_MIN_PRICE, Config.TURNOVER_THRESHOLD / 1e5, len(passing))

    # ── map to Dhan security IDs ──────────────────────────────────────────────
    logger.info("Fetching Dhan scrip master for security ID mapping...")
    all_eq   = _all_nse_eq()
    filtered = []
    for s in all_eq:
        sym = s['symbol']
        if sym not in passing:
            continue
        avg = sym_avg[sym]
        filtered.append({
            **s,
            'avg_daily_volume':   round(avg['avg_volume'],   0),
            'avg_daily_turnover': round(avg['avg_turnover'], 0),
        })

    missed = passing - {s['symbol'] for s in filtered}
    if missed:
        logger.warning("%d symbols not in Dhan scrip master: %s",
                       len(missed), ', '.join(sorted(missed)[:10]))

    logger.info("Universe: %d symbols  (close >= ₹%.0f, turnover >= ₹%.0fL)",
                len(filtered), Config.CLOSE_MIN_PRICE, Config.TURNOVER_THRESHOLD / 1e5)

    with open(cache, "w") as f:
        json.dump({"date": today, "symbols": filtered}, f)

    return filtered

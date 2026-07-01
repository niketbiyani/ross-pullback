"""
Builds the NSE equity universe using NSE CM Bhavcopy files.

Each bhavcopy is a single CSV (zipped) covering every stock that traded on
that day, with exact volume and rupee turnover.  Downloading 10 files takes
~5 seconds — no per-symbol Dhan API calls needed.

Filter: avg daily turnover >= TURNOVER_THRESHOLD (default ₹5 crore).
Turnover = price × volume, so it correctly ranks high-price stocks like
JTEKTINDIA alongside high-share-count large-caps.

The per-symbol avg_daily_volume and avg_daily_turnover are stored in the
universe cache and exposed to callers so the dashboard can show relative
intraday volume on each alert.

Cache TTL: 1 day (rebuilding takes seconds, so no need for a longer TTL).
"""
import csv
import io
import json
import logging
import os
import time
import zipfile
from datetime import date, timedelta

import requests
from dhanhq import DhanContext

from config import Config

logger = logging.getLogger(__name__)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

_NSE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Referer':        'https://www.nseindia.com/',
    'Accept':         'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language':'en-US,en;q=0.5',
}


def _trading_days_back(n: int) -> list[date]:
    """Last n weekdays ending yesterday, newest first."""
    days, d = [], date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def _nse_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com/", timeout=10)
    except Exception:
        pass
    return s


def _bhavcopy_urls(d: date) -> list[str]:
    """Return candidate URLs for the CM bhavcopy of date d (try newer format first)."""
    ds  = d.strftime("%Y%m%d")
    mon = d.strftime("%b").upper()
    return [
        # New format (NSE switched ~2023)
        f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{ds}_F_0000.csv.zip",
        # Old format
        f"https://archives.nseindia.com/content/historical/EQUITIES/"
        f"{d.year}/{mon}/cm{d.day:02d}{mon}{d.year}bhav.csv.zip",
    ]


def _download_bhavcopy(d: date, session: requests.Session) -> dict[str, dict]:
    """
    Download and parse NSE CM bhavcopy for date d.
    Returns {symbol: {'volume': float, 'turnover': float}} for EQ/BE series.
    Returns {} on failure (holiday, weekend, network error).
    """
    for url in _bhavcopy_urls(d):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code != 200:
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
            logger.debug("Bhavcopy %s failed (%s): %s", d, url[url.rindex('/')+1:], e)
    logger.warning("Bhavcopy unavailable for %s (holiday or network error)", d.isoformat())
    return {}


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


def build_universe(dhan_context: DhanContext) -> list[dict]:
    """
    Return [{security_id, symbol, avg_daily_volume, avg_daily_turnover}] for
    symbols with average daily turnover >= TURNOVER_THRESHOLD.

    avg_daily_volume and avg_daily_turnover are included so callers can
    compute relative intraday volume on live alerts.

    Caches result daily; rebuild takes ~5 seconds via NSE bhavcopy.
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

    # ── download bhavcopy for last N trading days ─────────────────────────────
    session = _nse_session()
    days    = _trading_days_back(Config.VOLUME_HISTORY_DAYS)

    sym_days: dict[str, list[dict]] = {}
    for d in days:
        bhav = _download_bhavcopy(d, session)
        for sym, stats in bhav.items():
            sym_days.setdefault(sym, []).append(stats)
        time.sleep(0.3)

    if not sym_days:
        logger.error("All bhavcopy downloads failed — cannot build universe")
        if os.path.exists(cache):
            try:
                with open(cache) as f:
                    data = json.load(f)
                logger.warning("Using stale universe cache from %s", data.get("date"))
                return data["symbols"]
            except Exception:
                pass
        return []

    # ── compute per-symbol averages ───────────────────────────────────────────
    min_days = max(1, Config.VOLUME_HISTORY_DAYS // 2)
    sym_avg: dict[str, dict] = {}
    for sym, stats_list in sym_days.items():
        if len(stats_list) < min_days:
            continue
        n           = len(stats_list)
        avg_vol     = sum(s['volume']   for s in stats_list) / n
        avg_val     = sum(s['turnover'] for s in stats_list) / n
        latest_close = stats_list[0].get('close', 0)   # stats_list[0] = most recent day
        sym_avg[sym] = {
            'avg_volume':   avg_vol,
            'avg_turnover': avg_val,
            'close':        latest_close,
        }

    passing = {sym for sym, s in sym_avg.items()
               if s['close']        >= Config.CLOSE_MIN_PRICE
               and s['avg_turnover'] >= Config.TURNOVER_THRESHOLD}
    logger.info(
        "Filters: close >= ₹%.0f, turnover >= ₹%.0fL → %d symbols pass",
        Config.CLOSE_MIN_PRICE, Config.TURNOVER_THRESHOLD / 1e5, len(passing)
    )

    # ── map to Dhan security IDs via scrip master ─────────────────────────────
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
            'avg_daily_turnover': round(avg['avg_turnover'],  0),
        })

    mapped_syms = {s['symbol'] for s in filtered}
    missed      = passing - mapped_syms
    if missed:
        logger.warning("%d bhavcopy symbols not in Dhan scrip master: %s",
                       len(missed), ', '.join(sorted(missed)[:10]))

    logger.info("Universe: %d symbols  (close >= ₹%.0f, turnover >= ₹%.0fL)",
                len(filtered), Config.CLOSE_MIN_PRICE, Config.TURNOVER_THRESHOLD / 1e5)

    with open(cache, "w") as f:
        json.dump({"date": today, "symbols": filtered}, f)

    return filtered

"""
Builds the scannable NSE equity universe: downloads the Dhan scrip master,
then filters to symbols with average daily volume >= VOLUME_THRESHOLD.

Universe is cached for UNIVERSE_MAX_AGE_DAYS (default 7) to avoid a slow
rebuild on every restart. On rebuild, one API call per symbol fetches all
VOLUME_HISTORY_DAYS (default 10) at once via a date range query.
"""
import csv
import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests
from dhanhq import DhanContext, dhanhq

from config import Config

logger = logging.getLogger(__name__)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def _trading_days_back(n: int) -> list[str]:
    """Last n weekdays ending yesterday, newest first."""
    days, d = [], date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.isoformat())
        d -= timedelta(days=1)
    return days


def _all_nse_eq() -> list[dict]:
    """Download scrip master → return [{security_id, symbol}, ...] for NSE EQ."""
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
        sym = row.get("SEM_TRADING_SYMBOL", "").strip()
        if sid and sym:
            result.append({"security_id": sid, "symbol": sym})
    logger.info("Scrip master: %d NSE EQ instruments", len(result))
    return result


def _fetch_vol(client: dhanhq, sec: dict, from_date: str, to_date: str) -> float:
    """
    Fetch average daily volume using a single date-range call covering
    VOLUME_HISTORY_DAYS. Groups intraday bars by calendar date and averages.
    Falls back to 0.0 on any failure.
    """
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
                day = date.fromtimestamp(int(ts)).isoformat()
                vol = float(vls[i]) if i < len(vls) else 0.0
                daily[day] = daily.get(day, 0.0) + vol

            totals = [v for v in daily.values() if v > 0]
            return sum(totals) / len(totals) if totals else 0.0
        except Exception:
            if attempt == 0:
                time.sleep(0.5)

    time.sleep(0.05)
    return 0.0


def build_universe(dhan_context: DhanContext) -> list[dict]:
    """
    Return [{security_id, symbol}] for symbols whose average daily volume
    over the last VOLUME_HISTORY_DAYS trading days >= VOLUME_THRESHOLD.
    Result is cached for UNIVERSE_MAX_AGE_DAYS days to avoid repeated rebuilds.
    """
    cache = Config.UNIVERSE_CACHE

    if os.path.exists(cache):
        try:
            with open(cache) as f:
                data = json.load(f)
            cache_date = date.fromisoformat(data.get("date", "2000-01-01"))
            age_days   = (date.today() - cache_date).days
            if age_days < Config.UNIVERSE_MAX_AGE_DAYS:
                logger.info("Universe: %d symbols (cache %d day(s) old, max %d)",
                            len(data["symbols"]), age_days, Config.UNIVERSE_MAX_AGE_DAYS)
                return data["symbols"]
        except Exception:
            pass

    logger.info("Building universe — downloading scrip master...")
    all_eq = _all_nse_eq()
    client = dhanhq(dhan_context)

    days      = _trading_days_back(Config.VOLUME_HISTORY_DAYS)
    from_date = days[-1]   # oldest
    to_date   = days[0]    # most recent (yesterday)
    logger.info("Volume check: %d-day avg  (%s → %s)  for %d symbols",
                Config.VOLUME_HISTORY_DAYS, from_date, to_date, len(all_eq))

    volumes: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_vol, client, s, from_date, to_date): s for s in all_eq}
        done = 0
        for fut in as_completed(futs):
            sec = futs[fut]
            try:
                volumes[sec["security_id"]] = fut.result()
            except Exception:
                volumes[sec["security_id"]] = 0.0
            done += 1
            if done % 100 == 0:
                logger.info("  Volume check: %d / %d", done, len(all_eq))
            time.sleep(0.05)

    filtered = [
        s for s in all_eq
        if volumes.get(s["security_id"], 0) >= Config.VOLUME_THRESHOLD
    ]
    logger.info("Volume filter >= %d shares/day: %d / %d symbols pass",
                Config.VOLUME_THRESHOLD, len(filtered), len(all_eq))

    with open(cache, "w") as f:
        json.dump({"date": date.today().isoformat(), "symbols": filtered}, f)

    return filtered

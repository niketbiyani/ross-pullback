"""
Builds the scannable NSE equity universe: downloads the Dhan scrip master,
then filters to symbols with average daily volume >= VOLUME_THRESHOLD.
Result is cached daily to avoid slow restarts.
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


def _last_trading_day() -> str:
    """Most recent weekday before today."""
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


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


def _fetch_vol(client: dhanhq, sec: dict) -> float:
    """Fetch average daily volume over the last 3 trading days."""
    days   = [_last_trading_day()]
    d      = date.fromisoformat(days[0])
    while len(days) < 3:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d.isoformat())

    totals = []
    for day in days:
        for attempt in range(2):
            try:
                resp = client.intraday_minute_data(
                    security_id=sec["security_id"],
                    exchange_segment="NSE_EQ",
                    instrument_type="EQUITY",
                    from_date=day,
                    to_date=day,
                )
                if isinstance(resp, dict) and isinstance(resp.get("data"), dict):
                    vols = resp["data"].get("volume", [])
                    vol  = sum(float(v) for v in vols if v)
                    if vol > 0:
                        totals.append(vol)
                break
            except Exception:
                if attempt == 0:
                    time.sleep(0.5)
        time.sleep(0.05)

    return sum(totals) / len(totals) if totals else 0.0


def build_universe(dhan_context: DhanContext) -> list[dict]:
    """
    Return [{security_id, symbol}] for symbols with yesterday's volume
    >= Config.VOLUME_THRESHOLD. Caches result daily.
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

    logger.info("Building universe — downloading scrip master...")
    all_eq = _all_nse_eq()
    client  = dhanhq(dhan_context)

    volumes: dict[str, float] = {}
    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_vol, client, s): s for s in all_eq}
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
    logger.info("Volume filter >=%-d shares: %d / %d symbols pass",
                Config.VOLUME_THRESHOLD, len(filtered), len(all_eq))

    with open(cache, "w") as f:
        json.dump({"date": today, "symbols": filtered}, f)

    return filtered

"""
F&O universe: hardcoded NSE F&O symbol list mapped to Dhan security IDs.
Downloads the Dhan scrip master once per day and caches the result.
Symbols that aren't found in the NSE EQ segment (indices, delisted, etc.)
are logged and skipped silently.
"""
import csv
import io
import json
import logging
import os
from datetime import date

import requests

from fno_config import FnoConfig

logger = logging.getLogger(__name__)

SCRIP_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# NSE F&O universe — hardcoded list provided by user.
# Indices (NIFTY, BANKNIFTY, CNXMIDCAP) are excluded at mapping time
# because they're not in the NSE EQ segment.
FNO_SYMBOLS: set[str] = {
    "HCLTECH", "MUTHOOTFIN", "AUROPHARMA", "KAYNES", "PERSISTENT",
    "LODHA", "NATIONALUM", "UPL", "COFORGE", "MPHASIS", "LUPIN",
    "SUNPHARMA", "OBEROIRLTY", "LTF", "MANAPPURAM", "APOLLOHOSP",
    "LTM", "PNBHOUSING", "TORNTPHARM", "DRREDDY", "ABCAPITAL",
    "SHREECEM", "ANGELONE", "ETERNAL", "AMBUJACEM", "WIPRO",
    "HINDZINC", "OFSS", "TCS", "ALKEM", "NHPC", "360ONE",
    "ZYDUSLIFE", "BIOCON", "BEL", "VEDL", "TATASTEEL", "BOSCHLTD",
    "BAJAJFINSV", "UNOMINDA", "DIVISLAB", "NUVAMA", "LAURUSLABS",
    "JINDALSTEL", "BAJFINANCE", "BAJAJHLDNG", "MANKIND", "PHOENIXLTD",
    "HINDALCO", "TRENT", "KFINTECH", "NAUKRI", "DLF", "BDL",
    "GODREJPROP", "DALBHARAT", "INDHOTEL", "GLENMARK", "UNITDSPR",
    "ULTRACEMCO", "VBL", "SWIGGY", "INFY", "COCHINSHIP", "RBLBANK",
    "BHARTIARTL", "TECHM", "MAXHEALTH", "MOTHERSON", "ONGC",
    "HYUNDAI", "COALINDIA", "GRASIM", "OIL", "ICICIBANK", "HDFCBANK",
    "YESBANK", "EICHERMOT", "HAL", "ASHOKLEY", "JSWSTEEL", "AMBER",
    "SHRIRAMFIN", "KALYANKJIL", "CAMS", "BANKINDIA", "MARUTI",
    "SBICARD", "KPITTECH", "LICHSGFIN", "CHOLAFIN", "ICICIGI",
    "EXIDEIND", "INDUSTOWER", "POWERGRID", "BHARATFORG", "SRF",
    "PETRONET", "MAZDOCK", "BRITANNIA", "HDFCAMC", "IEX",
    "GMRAIRPORT", "NMDC", "CDSL", "PGEL", "VMM", "CIPLA",
    "TATAELXSI", "PRESTIGE", "ITC", "NYKAA", "INDUSINDBK",
    "TATAPOWER", "VOLTAS", "NAM-INDIA", "JUBLFOOD", "CONCOR",
    "MOTILALOFS", "CANBK", "NBCC", "NESTLEIND", "SAIL", "DELHIVERY",
    "HEROMOTOCO", "ASIANPAINT", "FORTIS", "LICI", "PIDILITIND",
    "TVSMOTOR", "PATANJALI", "JIOFIN", "AUBANK", "FEDERALBNK",
    "APLAPOLLO", "RELIANCE", "SBILIFE", "TMPV", "PAGEIND",
    "HINDUNILVR", "GODREJCP", "NTPC", "KEI", "GODFRYPHLP",
    "IREDA", "IRFC", "SUPREMEIND", "BLUESTARCO", "ABB", "PIIND",
    "GAIL", "DIXON", "RVNL", "IOC", "TITAN", "HINDPETRO",
    "BAJAJ-AUTO", "ASTRAL", "INOXWIND", "RECLTD", "LT", "IDFCFIRSTB",
    "POLYCAB", "SONACOMS", "INDIGO", "HDFCLIFE", "IDEA", "HAVELLS",
    "RADICO", "BSE", "CROMPTON", "SOLARINDS", "SBIN", "PFC",
    "KOTAKBANK", "TATACONSUM", "BPCL", "AXISBANK", "M&M", "MFSL",
    "COLPAL", "ADANIPORTS", "DABUR", "ICICIPRULI", "ADANIENT",
    "ADANIENSOL", "PAYTM", "WAAREEENER", "ADANIPOWER", "SUZLON",
    "JSWENERGY", "CUMMINSIND", "PREMIERENE", "ADANIGREEN", "SIEMENS",
    "PNB", "BANDHANBNK", "MARICO", "INDIANB", "MCX", "FORCEMOT",
    "BHEL", "BANKBARODA", "TIINDIA", "DMART", "POLICYBZR", "CGPOWER",
    "UNIONBANK", "GVT&D", "POWERINDIA",
    # Indices — these will NOT map to NSE EQ and will be skipped
    "NIFTY", "BANKNIFTY", "CNXMIDCAP",
}


def build_fno_universe() -> list[dict]:
    """
    Return [{security_id, symbol}] for all F&O symbols found in the
    Dhan NSE EQ scrip master. Cached for one calendar day.
    Symbols not found (indices, delisted names, etc.) are logged and skipped.
    """
    cache_file = FnoConfig.UNIVERSE_CACHE
    today      = date.today().isoformat()

    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                data = json.load(f)
            if data.get("date") == today:
                logger.info("FNO universe: %d symbols (cache)", len(data["symbols"]))
                return data["symbols"]
        except Exception:
            pass

    logger.info("Downloading Dhan scrip master to map FNO symbols...")
    try:
        resp = requests.get(SCRIP_URL, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.error("Failed to download scrip master: %s", e)
        # Try stale cache before giving up
        if os.path.exists(cache_file):
            try:
                with open(cache_file) as f:
                    data = json.load(f)
                logger.warning("Using stale FNO universe cache from %s", data.get("date"))
                return data["symbols"]
            except Exception:
                pass
        return []

    result: list[dict] = []
    for row in csv.DictReader(io.StringIO(resp.text)):
        if row.get("SEM_EXM_EXCH_ID")      != "NSE":      continue
        if row.get("SEM_INSTRUMENT_NAME")   != "EQUITY":   continue
        if row.get("SEM_SEGMENT")           != "E":        continue
        sym = row.get("SEM_TRADING_SYMBOL",      "").strip()
        sid = row.get("SEM_SMST_SECURITY_ID",    "").strip()
        if sym in FNO_SYMBOLS and sid:
            result.append({"security_id": sid, "symbol": sym})

    found   = {s["symbol"] for s in result}
    missing = FNO_SYMBOLS - found - {"NIFTY", "BANKNIFTY", "CNXMIDCAP"}
    if missing:
        logger.warning(
            "FNO symbols not found in Dhan NSE EQ master (%d): %s",
            len(missing), ", ".join(sorted(missing)),
        )

    logger.info("FNO universe: %d / %d symbols mapped", len(result), len(FNO_SYMBOLS) - 3)

    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True) if os.path.dirname(cache_file) else None
        with open(cache_file, "w") as f:
            json.dump({"date": today, "symbols": result}, f)
    except Exception as e:
        logger.warning("Failed to save FNO universe cache: %s", e)

    return result

"""
Configuration for the Ross Cameron micro-pullback scanner.
Borrows Dhan credentials from risk-management .env if no local .env exists.
"""
import os
from dotenv import load_dotenv

_here    = os.path.dirname(os.path.abspath(__file__))
_local   = os.path.join(_here, '.env')
_rm_env  = os.path.join(_here, '..', 'Risk-Management', '.env')

# Load base credentials from Risk-Management first if present
if os.path.exists(_rm_env):
    load_dotenv(_rm_env)
    print("[config] Loaded base credentials from ../Risk-Management/.env")

# Override with local settings if present
if os.path.exists(_local):
    load_dotenv(_local, override=True)


class Config:
    # Dhan API
    DHAN_CLIENT_ID: str   = os.getenv("DHAN_CLIENT_ID", "")
    DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "")
    DHAN_PIN: str          = os.getenv("DHAN_PIN", "")
    DHAN_TOTP_SECRET: str  = os.getenv("DHAN_TOTP_SECRET", "")

    # Scanner
    HISTORY_DAYS: int          = int(os.getenv("HISTORY_DAYS", "15"))
    TIMEFRAMES: tuple          = (1, 3, 5, 15)
    MAX_WORKERS: int           = int(os.getenv("MAX_WORKERS", "32"))

    # Universe filter.  Two gates — both must pass:
    #   CLOSE_MIN_PRICE: exclude stocks below this price (penny stock filter). ₹100 default.
    #   TURNOVER_THRESHOLD: avg daily rupee turnover safety net (catches completely
    #     illiquid names even above ₹100).  Set very low so the dashboard volume
    #     filter is the real quality gate during the day.
    CLOSE_MIN_PRICE: float     = float(os.getenv("CLOSE_MIN_PRICE", "100"))
    TURNOVER_THRESHOLD: float  = float(os.getenv("TURNOVER_THRESHOLD", "1000000"))   # ₹10L
    VOLUME_HISTORY_DAYS: int   = int(os.getenv("VOLUME_HISTORY_DAYS", "10"))

    # Mover gate — only symbols crossing BOTH thresholds get a MACD engine
    MOVER_MIN_PCT:    float = float(os.getenv("MOVER_MIN_PCT",    "2.0"))     # % intraday move
    MOVER_MIN_VOLUME: float = float(os.getenv("MOVER_MIN_VOLUME", "1000000")) # cumulative shares

    # Strategy parameters
    EPISODE_MIN_BARS: int  = 8
    SL_MIN_PCT: float      = 0.0015   # 0.15%
    SL_MAX_PCT: float      = 0.030    # 3.0%

    # Live feed: on first poll, replay at most this many recent bars per symbol/TF.
    # Only applies when bootstrap_last_ts is not seeded (e.g., bare restart with no
    # bootstrap state).  Set high so a full session (375 bars) is always covered.
    LIVE_CATCHUP_BARS: int      = int(os.getenv("LIVE_CATCHUP_BARS", "500"))

    # RVOL spike detection
    RVOL_SPIKE_THRESHOLD: float = float(os.getenv("RVOL_SPIKE_THRESHOLD", "2.0"))

    # Dashboard
    DASHBOARD_PORT: int    = int(os.getenv("SCANNER_PORT", "5050"))
    DASHBOARD_HOST: str    = os.getenv("SCANNER_HOST", "0.0.0.0")

    # Cache
    UNIVERSE_CACHE: str    = os.path.join(_here, "universe_cache.json")
    BARS_CACHE_DIR: str    = os.path.join(_here, "bars_cache")

    @classmethod
    def validate(cls) -> list[str]:
        errors = []
        if not cls.DHAN_CLIENT_ID:
            errors.append("DHAN_CLIENT_ID missing")
        if not cls.DHAN_ACCESS_TOKEN:
            errors.append("DHAN_ACCESS_TOKEN missing")
        return errors

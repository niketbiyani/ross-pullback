"""
Configuration for the Ross Cameron micro-pullback scanner.
Borrows Dhan credentials from risk-management .env if no local .env exists.
"""
import os
from dotenv import load_dotenv

_here    = os.path.dirname(os.path.abspath(__file__))
_local   = os.path.join(_here, '.env')
_rm_env  = os.path.join(_here, '..', 'risk-management', '.env')

if os.path.exists(_local):
    load_dotenv(_local)
elif os.path.exists(_rm_env):
    load_dotenv(_rm_env)
    print("[config] Using credentials from ../risk-management/.env")


class Config:
    # Dhan API
    DHAN_CLIENT_ID: str   = os.getenv("DHAN_CLIENT_ID", "")
    DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "")
    DHAN_PIN: str          = os.getenv("DHAN_PIN", "")
    DHAN_TOTP_SECRET: str  = os.getenv("DHAN_TOTP_SECRET", "")

    # Scanner
    VOLUME_THRESHOLD: int  = int(os.getenv("VOLUME_THRESHOLD", "500000"))
    HISTORY_DAYS: int      = int(os.getenv("HISTORY_DAYS", "20"))
    TIMEFRAMES: tuple      = (1, 3, 5, 15)
    MAX_WORKERS: int       = int(os.getenv("MAX_WORKERS", "1"))

    # Strategy parameters
    EPISODE_MIN_BARS: int  = 8
    SL_MIN_PCT: float      = 0.0015   # 0.15%
    SL_MAX_PCT: float      = 0.030    # 3.0%

    # Dashboard
    DASHBOARD_PORT: int    = int(os.getenv("SCANNER_PORT", "5050"))
    DASHBOARD_HOST: str    = os.getenv("SCANNER_HOST", "0.0.0.0")

    # Cache
    UNIVERSE_CACHE: str    = os.path.join(_here, "universe_cache.json")

    @classmethod
    def validate(cls) -> list[str]:
        errors = []
        if not cls.DHAN_CLIENT_ID:
            errors.append("DHAN_CLIENT_ID missing")
        if not cls.DHAN_ACCESS_TOKEN:
            errors.append("DHAN_ACCESS_TOKEN missing")
        return errors

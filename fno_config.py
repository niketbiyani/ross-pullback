"""Configuration for the FNO micro-pullback scanner."""
import os
from dotenv import load_dotenv

_here  = os.path.dirname(os.path.abspath(__file__))
_local = os.path.join(_here, '.env')
_rm    = os.path.join(_here, '..', 'Risk-Management', '.env')

if os.path.exists(_local):
    load_dotenv(_local)
elif os.path.exists(_rm):
    load_dotenv(_rm)


class FnoConfig:
    # Dhan API (shared credentials with main scanner)
    DHAN_CLIENT_ID:    str = os.getenv("DHAN_CLIENT_ID",    "")
    DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "")

    # Data window (~1 calendar month of trading days)
    HISTORY_DAYS:      int   = 22
    TIMEFRAMES:        tuple = (1, 15, 30, 60)   # 1-min for resampling + momentum; 15/30/60 for MACD
    MAX_WORKERS:       int   = int(os.getenv("FNO_WORKERS", "16"))
    LIVE_CATCHUP_BARS: int   = 500

    # Strategy — must match config.Config so strategy_engine.py behaves identically
    EPISODE_MIN_BARS: int   = 8
    SL_MIN_PCT:       float = 0.0015
    SL_MAX_PCT:       float = 0.030

    # How many past trading days to show signals for
    STRATEGY_LOOKBACK_DAYS: int = 7

    # Momentum threshold (1-min bars)
    MOVER_MIN_PCT: float = 3.0

    # Dashboard
    DASHBOARD_PORT: int = int(os.getenv("FNO_SCANNER_PORT", "5051"))
    DASHBOARD_HOST: str = os.getenv("FNO_SCANNER_HOST",     "0.0.0.0")

    # Cache — separate directories so FNO and main scanner don't interfere
    UNIVERSE_CACHE: str = os.path.join(_here, "fno_universe_cache.json")
    BARS_CACHE_DIR: str = os.path.join(_here, "bars_cache_fno")

    @classmethod
    def validate(cls) -> list[str]:
        errors = []
        if not cls.DHAN_CLIENT_ID:
            errors.append("DHAN_CLIENT_ID not set")
        if not cls.DHAN_ACCESS_TOKEN:
            errors.append("DHAN_ACCESS_TOKEN not set")
        return errors

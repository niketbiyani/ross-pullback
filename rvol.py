"""
Per-symbol volume spike detector using Z-score.

During bootstrap, collects ALL 1m bar volumes across every history day
into a single flat pool (no grouping by time of day).

After bootstrap, finalize() computes one mean and one standard deviation
across the entire pool — roughly 375 bars/day × 5 days = ~1875 samples.

During the live session, score() computes:

    z = (bar_volume − mean) / std

A bar is flagged as a spike when z exceeds the configured threshold
(default 2.0 ≈ 97.5th percentile of the stock's own volume distribution).

This makes no assumption about what time of day it is — a bar is simply
unusual if it is far above the stock's typical per-bar volume over the
last N days, regardless of when in the session it occurs.

Buyer/seller split mirrors TradingView's "Volume Buyers vs Sellers":
    buyer_vol  = volume × (close − low)  / (high − low)
    seller_vol = volume × (high − close) / (high − low)

Not thread-safe per instance — callers must ensure serial access per symbol.
"""
import math
from dataclasses import dataclass


def _in_market_hours(ts: int) -> bool:
    """Return True if UTC timestamp falls within NSE trading hours (IST 9:15–15:30)."""
    m = (ts // 60 + 330) % (24 * 60)
    return 555 <= m < 930


def _time_ist(ts: int) -> str:
    m = (ts // 60 + 330) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


@dataclass
class VolumeSpike:
    symbol:    str
    ts:        int
    time_ist:  str
    close:     float
    volume:    float     # this bar's volume
    mean_vol:  float     # mean of all historical 1m bars (5-day pool)
    std_vol:   float     # std dev of that pool
    z_score:   float     # (volume − mean) / std
    buyer_vol:  float
    seller_vol: float
    buyer_pct:  float    # 0.0–1.0


class RvolTracker:
    """One instance per symbol. Lifecycle: add_historical* → finalize → score*."""

    __slots__ = ('symbol', '_vols', '_mean', '_std', '_ready')

    def __init__(self, symbol: str):
        self.symbol  = symbol
        self._vols:  list[float] = []   # flat pool of all historical 1m volumes
        self._mean   = 0.0
        self._std    = 1.0
        self._ready  = False

    # ── bootstrap phase ───────────────────────────────────────────────────────

    def add_historical(self, ts: int, volume: float) -> None:
        """Accumulate one 1m bar volume (market hours only)."""
        if self._ready or volume <= 0:
            return
        if _in_market_hours(ts):
            self._vols.append(volume)

    def finalize(self) -> None:
        """Compute mean and std across the full 5-day pool; release raw data."""
        n = len(self._vols)
        if n >= 2:
            mean = sum(self._vols) / n
            variance = sum((v - mean) ** 2 for v in self._vols) / (n - 1)
            self._mean = mean
            self._std  = max(math.sqrt(variance), 1.0)
        self._vols.clear()
        self._ready = True

    # ── live phase ────────────────────────────────────────────────────────────

    def score(self, bar: dict) -> VolumeSpike | None:
        """Score one live 1m bar. Returns VolumeSpike or None."""
        if not self._ready or self._mean == 0.0:
            return None
        ts = bar.get('ts', bar.get('timestamp', 0))
        if not _in_market_hours(ts):
            return None

        vol    = bar.get('volume', 0.0)
        high   = bar.get('high',   0.0)
        low    = bar.get('low',    0.0)
        close  = bar.get('close',  0.0)
        rng    = max(high - low, 1e-6)

        buyer_vol  = vol * max(close - low,  0.0) / rng
        seller_vol = vol * max(high - close, 0.0) / rng

        return VolumeSpike(
            symbol     = self.symbol,
            ts         = ts,
            time_ist   = _time_ist(ts),
            close      = close,
            volume     = vol,
            mean_vol   = self._mean,
            std_vol    = self._std,
            z_score    = (vol - self._mean) / self._std,
            buyer_vol  = buyer_vol,
            seller_vol = seller_vol,
            buyer_pct  = buyer_vol / max(vol, 1e-6),
        )

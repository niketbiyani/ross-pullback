"""
Per-symbol RVOL (Relative Volume) tracker.

During bootstrap, accumulates historical 1m bar volumes grouped by IST
minute-of-day (e.g. all 9:15 bars across 5 days, all 9:16 bars, ...).
After bootstrap, finalize() computes per-minute averages.
During the live session, score() returns an RVOL score and buyer/seller
breakdown for any 1m bar.

Buyer/seller split mirrors TradingView's "Volume Buyers vs Sellers":
  buyer_vol  = volume × (close − low)  / (high − low)
  seller_vol = volume × (high − close) / (high − low)

Not thread-safe per instance — callers must ensure serial access per symbol.
(Bootstrap workers each own exactly one symbol, so this is guaranteed.)
"""
from dataclasses import dataclass


def _minute_ist(ts: int) -> int | None:
    """UTC Unix timestamp → IST minute-of-day. Returns None outside NSE hours."""
    m = (ts // 60 + 330) % (24 * 60)   # IST minutes since midnight
    return m if 555 <= m < 930 else None   # 9:15 ≤ m < 15:30


@dataclass
class VolumeSpike:
    symbol:     str
    ts:         int
    time_ist:   str
    close:      float
    volume:     float
    avg_volume: float
    rvol:       float        # volume / historical avg for this minute
    buyer_vol:  float        # estimated shares bought
    seller_vol: float        # estimated shares sold
    buyer_pct:  float        # buyer_vol / volume  (0.0–1.0)


class RvolTracker:
    """One instance per symbol. Lifecycle: add_historical* → finalize → score*."""

    __slots__ = ('symbol', '_hist', '_avg', '_ready')

    def __init__(self, symbol: str):
        self.symbol  = symbol
        self._hist:  dict[int, list[float]] = {}   # minute_ist → [daily vols]
        self._avg:   dict[int, float]       = {}   # minute_ist → mean vol
        self._ready  = False

    # ── bootstrap phase ───────────────────────────────────────────────────────

    def add_historical(self, ts: int, volume: float) -> None:
        if self._ready or volume <= 0:
            return
        m = _minute_ist(ts)
        if m is not None:
            self._hist.setdefault(m, []).append(volume)

    def finalize(self) -> None:
        """Compute per-minute averages and release raw history."""
        for m, vols in self._hist.items():
            self._avg[m] = sum(vols) / len(vols)
        self._hist.clear()
        self._ready = True

    # ── live phase ────────────────────────────────────────────────────────────

    def score(self, bar: dict) -> VolumeSpike | None:
        """Score one live 1m bar. Returns VolumeSpike or None (outside hours / no baseline)."""
        if not self._ready:
            return None
        ts  = bar.get('ts', bar.get('timestamp', 0))
        m   = _minute_ist(ts)
        if m is None:
            return None
        avg = self._avg.get(m, 0.0)
        if avg == 0.0:
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
            time_ist   = f"{m // 60:02d}:{m % 60:02d}",
            close      = close,
            volume     = vol,
            avg_volume = avg,
            rvol       = vol / avg,
            buyer_vol  = buyer_vol,
            seller_vol = seller_vol,
            buyer_pct  = buyer_vol / max(vol, 1e-6),
        )

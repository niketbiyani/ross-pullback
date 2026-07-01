"""
Per-symbol historical volume and price-range baseline tracker.

During bootstrap, collects 1m bar data bucketed by intraday minute slot
(slot 0 = 09:15, slot 1 = 09:16, …, slot 374 = 15:29 IST) and builds:

  per_slot_avg     — historical average volume for each minute slot
  per_slot_rng_avg — historical average bar range % for each minute slot
                     range % = (high - low) / open × 100

bar_rvol(ts, vol)            → vol spike vs historical slot avg
bar_price_rvol(ts, h, l, o) → price range spike vs historical slot avg

A 90× bar_rvol at 13:09 means that bar traded 90× its normal volume.
A 15× bar_price_rvol at 13:09 means that bar's price swing was 15×
its normal range at that time of day — pure price action signal.

Not thread-safe per instance — callers must ensure serial access per symbol.
"""

_SESSION_START = 555   # 09:15 IST in minutes-from-midnight
_SESSION_END   = 930   # 15:30 IST in minutes-from-midnight


def _minute_slot(ts: int) -> int | None:
    """Return 0-indexed minute slot (0=09:15 … 374=15:29), or None if outside session."""
    m = (ts // 60 + 330) % (24 * 60)
    if _SESSION_START <= m < _SESSION_END:
        return m - _SESSION_START
    return None


class RvolTracker:
    """One instance per symbol. Lifecycle: add_historical* → finalize → bar_rvol/bar_price_rvol."""

    __slots__ = (
        'symbol',
        '_per_slot', '_per_slot_rng',
        'per_slot_avg', 'per_slot_rng_avg',
        'hist_mean', 'hist_rng_mean',
        '_ready',
    )

    def __init__(self, symbol: str):
        self.symbol            = symbol
        self._per_slot:        dict[int, list[float]] = {}
        self._per_slot_rng:    dict[int, list[float]] = {}
        self.per_slot_avg:     dict[int, float]       = {}
        self.per_slot_rng_avg: dict[int, float]       = {}
        self.hist_mean         = 0.0
        self.hist_rng_mean     = 0.0
        self._ready            = False

    def add_historical(self, ts: int, volume: float,
                       high: float = 0.0, low: float = 0.0,
                       open_price: float = 0.0) -> None:
        if self._ready or volume <= 0:
            return
        sl = _minute_slot(ts)
        if sl is None:
            return
        self._per_slot.setdefault(sl, []).append(volume)
        if open_price > 0 and high > low:
            rng_pct = (high - low) / open_price * 100
            self._per_slot_rng.setdefault(sl, []).append(rng_pct)

    def finalize(self) -> None:
        all_vols: list[float] = []
        for sl, vols in self._per_slot.items():
            avg = sum(vols) / len(vols)
            self.per_slot_avg[sl] = avg
            all_vols.extend(vols)
        if all_vols:
            self.hist_mean = sum(all_vols) / len(all_vols)
        self._per_slot.clear()

        all_rngs: list[float] = []
        for sl, rngs in self._per_slot_rng.items():
            avg = sum(rngs) / len(rngs)
            self.per_slot_rng_avg[sl] = avg
            all_rngs.extend(rngs)
        if all_rngs:
            self.hist_rng_mean = sum(all_rngs) / len(all_rngs)
        self._per_slot_rng.clear()

        self._ready = True

    def bar_rvol(self, ts: int, volume: float) -> float:
        """Volume of this bar ÷ historical avg for the same minute slot."""
        if volume <= 0:
            return 0.0
        sl  = _minute_slot(ts)
        avg = (self.per_slot_avg.get(sl, self.hist_mean)
               if sl is not None else self.hist_mean)
        return volume / avg if avg > 0 else 0.0

    def bar_price_rvol(self, ts: int, high: float, low: float,
                       open_price: float) -> float:
        """Bar range % ÷ historical avg range % for the same minute slot."""
        if open_price <= 0 or high <= low:
            return 0.0
        rng_pct = (high - low) / open_price * 100
        sl  = _minute_slot(ts)
        avg = (self.per_slot_rng_avg.get(sl, self.hist_rng_mean)
               if sl is not None else self.hist_rng_mean)
        return rng_pct / avg if avg > 0 else 0.0

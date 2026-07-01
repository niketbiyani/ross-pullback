"""
Per-symbol historical volume baseline tracker.

During bootstrap, collects 1m bar volumes bucketed by intraday minute slot
(slot 0 = 09:15, slot 1 = 09:16, …, slot 374 = 15:29 IST).
After bootstrap, finalize() computes per-slot historical averages.

bar_rvol(ts, volume) compares a live bar's volume against the historical
average for the same minute slot — Ross Cameron's "volume spike" approach.
A 90× bar at 13:09 shows up as 90×, regardless of how quiet the morning was.

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
    """One instance per symbol. Lifecycle: add_historical* → finalize → bar_rvol()."""

    __slots__ = ('symbol', '_per_slot', 'per_slot_avg', 'hist_mean', '_ready')

    def __init__(self, symbol: str):
        self.symbol        = symbol
        self._per_slot:    dict[int, list[float]] = {}
        self.per_slot_avg: dict[int, float]       = {}
        self.hist_mean     = 0.0
        self._ready        = False

    def add_historical(self, ts: int, volume: float) -> None:
        if self._ready or volume <= 0:
            return
        sl = _minute_slot(ts)
        if sl is not None:
            self._per_slot.setdefault(sl, []).append(volume)

    def finalize(self) -> None:
        all_vols: list[float] = []
        for sl, vols in self._per_slot.items():
            avg = sum(vols) / len(vols)
            self.per_slot_avg[sl] = avg
            all_vols.extend(vols)
        if all_vols:
            self.hist_mean = sum(all_vols) / len(all_vols)
        self._per_slot.clear()
        self._ready = True

    def bar_rvol(self, ts: int, volume: float) -> float:
        """Return volume / historical_avg_for_this_minute_slot (0 if no baseline)."""
        if volume <= 0:
            return 0.0
        sl  = _minute_slot(ts)
        avg = (self.per_slot_avg.get(sl, self.hist_mean)
               if sl is not None else self.hist_mean)
        return volume / avg if avg > 0 else 0.0

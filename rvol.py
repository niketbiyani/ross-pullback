"""
Per-symbol historical volume baseline tracker.

During bootstrap, collects ALL 1m bar volumes within market hours across
every history day into a flat pool. After bootstrap, finalize() computes
the mean bar volume across that pool (~375 bars/day × N days).

This single historical mean is used by the dashboard leaderboard to rank
stocks by how their recent activity compares to their own norm.

Not thread-safe per instance — callers must ensure serial access per symbol.
"""


def _in_market_hours(ts: int) -> bool:
    """Return True if UTC timestamp falls within NSE trading hours (IST 9:15–15:30)."""
    m = (ts // 60 + 330) % (24 * 60)
    return 555 <= m < 930


class RvolTracker:
    """One instance per symbol. Lifecycle: add_historical* → finalize → hist_mean."""

    __slots__ = ('symbol', '_vols', 'hist_mean', '_ready')

    def __init__(self, symbol: str):
        self.symbol    = symbol
        self._vols:    list[float] = []
        self.hist_mean = 0.0
        self._ready    = False

    def add_historical(self, ts: int, volume: float) -> None:
        if self._ready or volume <= 0:
            return
        if _in_market_hours(ts):
            self._vols.append(volume)

    def finalize(self) -> None:
        if self._vols:
            self.hist_mean = sum(self._vols) / len(self._vols)
        self._vols.clear()
        self._ready = True

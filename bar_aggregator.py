"""
Aggregates ticks into 1-min OHLCV bars, then rolls up to 3/5/15-min.
on_bar(symbol, tf_minutes, bar_dict) is called each time a bar closes.
"""
from typing import Callable

OnBarCB = Callable[[str, int, dict], None]  # symbol, tf_minutes, bar_dict


class BarAggregator:
    """
    One instance per symbol.
    Feed ticks via on_tick(); closed bars are sent to the on_bar callback
    for all four timeframes (1, 3, 5, 15).
    """
    def __init__(self, symbol: str, on_bar: OnBarCB):
        self.symbol   = symbol
        self.on_bar   = on_bar
        self._bars:    dict[int, dict] = {}   # tf → current in-progress bar dict
        self._prev_vol: float = 0.0

    def on_tick(self, ltp: float, day_volume: float, ts: int):
        """
        ltp        : last traded price
        day_volume : cumulative day volume from Dhan feed (used to derive vol delta)
        ts         : unix timestamp in seconds
        """
        vol_delta       = max(day_volume - self._prev_vol, 0.0)
        self._prev_vol  = day_volume
        minute_ts       = (ts // 60) * 60
        closed_1min     = self._update_tf(1, minute_ts, ltp, ltp, ltp, ltp, vol_delta)
        if closed_1min:
            for tf in (3, 5, 15):
                self._roll_up(tf, closed_1min)

    def feed_historical(self, bar: dict):
        """
        Feed a complete historical 1-min bar (dict with ts/open/high/low/close/volume).
        Derives and fires higher-TF bars automatically.
        """
        closed_1min = self._update_tf(
            1, bar['ts'],
            bar['open'], bar['high'], bar['low'], bar['close'], bar['volume']
        )
        if closed_1min:
            for tf in (3, 5, 15):
                self._roll_up(tf, closed_1min)

    # ── internals ─────────────────────────────────────────────────────────────

    def _update_tf(self, tf: int, aligned_ts: int,
                   o: float, h: float, l: float, c: float, v: float) -> dict | None:
        bar_ts  = (aligned_ts // (tf * 60)) * (tf * 60)
        cur     = self._bars.get(tf)
        closed  = None

        if cur and cur['ts'] != bar_ts:
            closed = dict(cur)
            self.on_bar(self.symbol, tf, closed)
            cur = None

        if cur is None:
            self._bars[tf] = {'ts': bar_ts, 'open': o, 'high': h,
                              'low': l, 'close': c, 'volume': v}
        else:
            cur['high']    = max(cur['high'], h)
            cur['low']     = min(cur['low'], l)
            cur['close']   = c
            cur['volume'] += v

        return closed

    def _roll_up(self, tf: int, bar_1min: dict):
        self._update_tf(
            tf, bar_1min['ts'],
            bar_1min['open'], bar_1min['high'],
            bar_1min['low'], bar_1min['close'], bar_1min['volume']
        )

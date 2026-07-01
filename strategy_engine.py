"""
MACD micro-pullback strategy engine — incremental, one bar at a time.
Mirrors the batch logic in analysis_v5.py but suitable for live scanning.

For each (symbol, timeframe) instance, call update(bar_record) on every
closed bar. Detected entries are delivered via the on_alert callback.
"""
import logging
from dataclasses import dataclass
from typing import Callable

import numpy as np

from config import Config

logger = logging.getLogger(__name__)


@dataclass
class BarRecord:
    ts:     int
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float
    macd:   float
    signal: float
    ema50:  float
    rsi:    float


@dataclass
class Alert:
    symbol:        str
    tf:            int
    direction:     str       # 'SHORT' or 'LONG'
    wave_num:      int
    entry_price:   float
    sl_level:      float
    sl_distance:   float
    sl_pct:        float
    swing_level:   float
    rsi_at_entry:  float
    ema_clear:     bool      # V1: no EMA touch from ep_start → entry
    rsi_extreme:   bool      # RSI ≤30 (SHORT) or ≥70 (LONG) from ep_start → entry
    ep_len_so_far: int
    ts:            int


AlertCB = Callable[[Alert], None]


class StrategyEngine:
    """
    State machine for one (symbol, timeframe).
    Detects DOWN/UP episodes → wave crosses → entries.
    """

    def __init__(self, symbol: str, tf: int, on_alert: AlertCB):
        self.symbol   = symbol
        self.tf       = tf
        self.on_alert = on_alert

        self._bars: list[BarRecord] = []   # all bars ever seen
        self._reset_episode()

    # ── public ────────────────────────────────────────────────────────────────

    def update(self, bar: BarRecord):
        self._bars.append(bar)
        idx = len(self._bars) - 1

        both_neg = bar.macd < 0 and bar.signal < 0
        both_pos = bar.macd > 0 and bar.signal > 0

        if self._ep_start is None:
            if both_neg:
                self._begin_episode(idx, 'DOWN')
            elif both_pos:
                self._begin_episode(idx, 'UP')
            return

        # Check if still in episode
        in_ep = both_neg if self._ep_dir == 'DOWN' else both_pos
        if not in_ep:
            self._reset_episode()
            if both_neg:
                self._begin_episode(idx, 'DOWN')
            elif both_pos:
                self._begin_episode(idx, 'UP')
            return

        rel = idx - self._ep_start   # relative index within episode

        # Detect any cross at bars[rel-1]→bars[rel]
        if rel >= 1:
            self._detect_cross(rel)

        # Check pending crosses against this bar
        ep_len = rel + 1
        if ep_len >= Config.EPISODE_MIN_BARS:
            self._check_entries(rel, bar)

    # ── episode management ────────────────────────────────────────────────────

    def _begin_episode(self, idx: int, direction: str):
        self._ep_start = idx
        self._ep_dir   = direction
        self._wave_num = 0
        self._prev_entry_price: float | None = None
        self._all_crosses: list[tuple[int, str]] = []
        self._pending:    list[dict]             = []
        self._w1_passed_ema: bool | None  = None
        self._ep_v2_gate:    bool | None  = None
        self._ep_v2_rsi:     bool | None  = None
        self._w1_entry_rel:  int  | None  = None

    def _reset_episode(self):
        self._ep_start = None
        self._ep_dir   = None
        self._pending  = []

    # ── cross detection ───────────────────────────────────────────────────────

    def _detect_cross(self, rel: int):
        ep  = self._ep_bars()
        if rel >= len(ep):
            return
        prev, cur = ep[rel - 1], ep[rel]

        if prev.macd < prev.signal and cur.macd > cur.signal:
            self._all_crosses.append((rel, 'up'))
            if self._ep_dir == 'DOWN':
                self._on_wave_cross(rel)
        elif prev.macd > prev.signal and cur.macd < cur.signal:
            self._all_crosses.append((rel, 'down'))
            if self._ep_dir == 'UP':
                self._on_wave_cross(rel)

    def _on_wave_cross(self, rel: int):
        ep        = self._ep_bars()
        direction = self._ep_dir
        cur       = ep[rel]

        # Both lines must still be on the correct side
        if direction == 'DOWN' and (cur.macd >= 0 or cur.signal >= 0):
            return
        if direction == 'UP'   and (cur.macd <= 0 or cur.signal <= 0):
            return

        # Need minimum episode length
        if rel + 1 < Config.EPISODE_MIN_BARS:
            return

        # Swing level: extreme price from episode start → cross (inclusive)
        if direction == 'DOWN':
            swing = min(b.low  for b in ep[:rel + 1])
        else:
            swing = max(b.high for b in ep[:rel + 1])

        # Duplicate guard: skip if prev entry already broke this swing
        if self._prev_entry_price is not None:
            if direction == 'DOWN' and swing >= self._prev_entry_price:
                return
            if direction == 'UP'   and swing <= self._prev_entry_price:
                return

        # SL anchor: most recent reverse cross before this wave cross
        rev_dir   = 'down' if direction == 'DOWN' else 'up'
        sl_anchor = None
        for c2, d2 in reversed(self._all_crosses):
            if c2 < rel and d2 == rev_dir:
                sl_anchor = c2
                break
        if sl_anchor is None:
            macds = [b.macd for b in ep[:rel + 1]]
            sl_anchor = (int(np.argmin(macds)) if direction == 'DOWN'
                         else int(np.argmax(macds)))

        self._pending.append({
            'rel':         rel,
            'swing':       swing,
            'sl_anchor':   sl_anchor,
        })

    # ── entry detection ───────────────────────────────────────────────────────

    def _check_entries(self, cur_rel: int, bar: BarRecord):
        ep        = self._ep_bars()
        direction = self._ep_dir
        fired     = []

        for pc in self._pending:
            if cur_rel <= pc['rel']:        # entry must be AFTER cross
                continue

            swing     = pc['swing']
            sl_anchor = pc['sl_anchor']

            # Entry condition
            if direction == 'DOWN':
                if bar.low >= swing:
                    continue
                entry_price = bar.low
            else:
                if bar.high <= swing:
                    continue
                entry_price = bar.high

            # SL level
            if direction == 'DOWN':
                sl_level    = max(b.high for b in ep[sl_anchor:cur_rel + 1])
                sl_distance = sl_level - entry_price
            else:
                sl_level    = min(b.low  for b in ep[sl_anchor:cur_rel + 1])
                sl_distance = entry_price - sl_level

            sl_pct = sl_distance / entry_price if entry_price > 0 else 0.0
            if sl_pct < Config.SL_MIN_PCT or sl_pct > Config.SL_MAX_PCT:
                self._prev_entry_price = entry_price
                fired.append(pc)
                continue

            # EMA clear V1: no touch from episode start → entry
            if direction == 'DOWN':
                ema_clear = not any(b.high >= b.ema50 for b in ep[:cur_rel + 1])
                rsi_extreme = min(b.rsi for b in ep[:cur_rel + 1]) <= 30
            else:
                ema_clear = not any(b.low <= b.ema50 for b in ep[:cur_rel + 1])
                rsi_extreme = max(b.rsi for b in ep[:cur_rel + 1]) >= 70

            # Wave counting
            self._wave_num += 1
            wave_num = self._wave_num

            if wave_num == 1:
                self._w1_passed_ema = ema_clear
                self._ep_v2_gate    = None
                self._ep_v2_rsi     = None
                self._w1_entry_rel  = cur_rel
            elif wave_num == 2 and not self._w1_passed_ema:
                w1r = self._w1_entry_rel or 0
                if direction == 'DOWN':
                    fresh_touch = any(b.high >= b.ema50 for b in ep[w1r:cur_rel + 1])
                    fresh_rsi   = min(b.rsi for b in ep[w1r:cur_rel + 1]) <= 30
                else:
                    fresh_touch = any(b.low <= b.ema50 for b in ep[w1r:cur_rel + 1])
                    fresh_rsi   = max(b.rsi for b in ep[w1r:cur_rel + 1]) >= 70
                self._ep_v2_gate = not fresh_touch
                self._ep_v2_rsi  = fresh_rsi

            self._prev_entry_price = entry_price
            fired.append(pc)

            alert = Alert(
                symbol=self.symbol, tf=self.tf,
                direction='SHORT' if direction == 'DOWN' else 'LONG',
                wave_num=wave_num,
                entry_price=entry_price, sl_level=sl_level,
                sl_distance=sl_distance, sl_pct=sl_pct,
                swing_level=swing,
                rsi_at_entry=bar.rsi, ema_clear=ema_clear,
                rsi_extreme=rsi_extreme,
                ep_len_so_far=cur_rel + 1, ts=bar.ts,
            )
            logger.info("[%s %dm] %s W%d @ %.2f  SL %.2f (%.2f%%)",
                        self.symbol, self.tf, alert.direction,
                        wave_num, entry_price, sl_level, sl_pct * 100)
            self.on_alert(alert)

        for pc in fired:
            self._pending.remove(pc)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _ep_bars(self) -> list[BarRecord]:
        return self._bars[self._ep_start:] if self._ep_start is not None else []

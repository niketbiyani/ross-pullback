"""
MACD micro-pullback strategy engine — incremental, one bar at a time.
Mirrors the batch logic in analysis_v5.py but suitable for live scanning.

For each (symbol, timeframe) instance, call update(bar_record) on every
closed bar. Detected entries are delivered via the on_alert callback.

Filter tiers reflected in each Alert:
  V1  — ema_clear (episode-level) AND rsi_extreme both True
  V2  — ema_clear_v2 AND rsi_extreme_v2 both True (W2 fresh-window unlocked)
  raw — neither filter confirmed
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
    symbol:         str
    tf:             int
    direction:      str       # 'SHORT' or 'LONG'
    wave_num:       int
    entry_price:    float
    sl_level:       float
    sl_distance:    float
    sl_pct:         float
    swing_level:    float
    rsi_at_entry:   float
    # V1 flags (episode-level gate)
    ema_clear:      bool      # no EMA touch from ep_start → entry
    rsi_extreme:    bool      # RSI ≤30 (SHORT) or ≥70 (LONG) from ep_start → entry
    # V2 flags (W2 fresh-window gate; same as V1 for W1 and when W1 passed EMA)
    ema_clear_v2:   bool
    rsi_extreme_v2: bool
    ep_len_so_far:  int
    ts:             int
    # Populated by main.py after strategy engine fires (not by strategy engine itself)
    today_volume:   float = 0.0   # cumulative intraday volume in shares at alert time
    rel_volume:     float = 0.0   # today_volume / avg_daily_volume (1.0 = 100% of avg)
    day_range_pct:  float = 0.0   # today (high-low)/open % at alert time


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
        self._bars: list[BarRecord] = []
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

        in_ep = both_neg if self._ep_dir == 'DOWN' else both_pos
        if not in_ep:
            self._reset_episode()
            if both_neg:
                self._begin_episode(idx, 'DOWN')
            elif both_pos:
                self._begin_episode(idx, 'UP')
            return

        rel = idx - self._ep_start
        if rel >= 1:
            self._detect_cross(rel)
        if rel + 1 >= Config.EPISODE_MIN_BARS:
            self._check_entries(rel, bar)

    # ── episode management ────────────────────────────────────────────────────

    def _begin_episode(self, idx: int, direction: str):
        self._ep_start          = idx
        self._ep_dir            = direction
        self._wave_num          = 0
        self._prev_entry_price: float | None = None
        self._all_crosses:      list[tuple[int, str]] = []
        self._pending:          list[dict]             = []
        self._w1_passed_ema:    bool | None = None
        self._ep_v2_gate:       bool | None = None
        self._ep_v2_rsi:        bool | None = None
        self._w1_entry_rel:     int  | None = None
        self._prev_wave_entry_rel: int | None = None  # rel of last fired entry (RSI window start)

    def _reset_episode(self):
        self._ep_start = None
        self._ep_dir   = None
        self._pending  = []

    # ── cross detection ───────────────────────────────────────────────────────

    def _detect_cross(self, rel: int):
        ep = self._ep_bars()
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

        if direction == 'DOWN' and (cur.macd >= 0 or cur.signal >= 0):
            return
        if direction == 'UP'   and (cur.macd <= 0 or cur.signal <= 0):
            return
        if rel + 1 < Config.EPISODE_MIN_BARS:
            return

        swing = (min(b.low  for b in ep[:rel + 1]) if direction == 'DOWN'
                 else max(b.high for b in ep[:rel + 1]))

        if self._prev_entry_price is not None:
            if direction == 'DOWN' and swing >= self._prev_entry_price:
                return
            if direction == 'UP'   and swing <= self._prev_entry_price:
                return

        ep_seg = ep[:rel + 1]
        if direction == 'DOWN':
            sl_anchor = int(np.argmin([b.low  for b in ep_seg]))
        else:
            sl_anchor = int(np.argmax([b.high for b in ep_seg]))

        self._pending.append({'rel': rel, 'swing': swing, 'sl_anchor': sl_anchor})

    # ── entry detection ───────────────────────────────────────────────────────

    def _check_entries(self, cur_rel: int, bar: BarRecord):
        ep        = self._ep_bars()
        direction = self._ep_dir
        fired     = []

        for pc in self._pending:
            if cur_rel <= pc['rel']:
                continue

            swing, sl_anchor = pc['swing'], pc['sl_anchor']

            if direction == 'DOWN':
                if bar.low >= swing:
                    continue
                entry_price = bar.low
                sl_level    = max(b.high for b in ep[sl_anchor:cur_rel + 1])
                sl_distance = sl_level - entry_price
            else:
                if bar.high <= swing:
                    continue
                entry_price = bar.high
                sl_level    = min(b.low  for b in ep[sl_anchor:cur_rel + 1])
                sl_distance = entry_price - sl_level

            sl_pct = sl_distance / entry_price if entry_price > 0 else 0.0
            if sl_pct < Config.SL_MIN_PCT or sl_pct > Config.SL_MAX_PCT:
                self._prev_entry_price = entry_price
                fired.append(pc)
                continue

            # ── V1 flags ─────────────────────────────────────────────────────
            # EMA clear: episode-wide — no candle touches EMA50 from ep start to entry
            # RSI extreme: wave-specific — must be fresh for each wave
            #   W1: RSI window is ep_start → W1 cross (pre-pullback momentum)
            #   W2+: RSI window is prev_entry → current cross (fresh push each wave)
            cross_rel  = pc['rel']
            wave_start = self._prev_wave_entry_rel if self._prev_wave_entry_rel is not None else 0
            if direction == 'DOWN':
                ema_clear   = not any(b.high >= b.ema50 for b in ep[:cur_rel + 1])
                rsi_extreme = min(b.rsi for b in ep[wave_start:cross_rel + 1]) <= 30
            else:
                ema_clear   = not any(b.low <= b.ema50 for b in ep[:cur_rel + 1])
                rsi_extreme = max(b.rsi for b in ep[wave_start:cross_rel + 1]) >= 70

            # ── wave counting ──────────────────────────────────────────────
            self._wave_num += 1
            wave_num = self._wave_num

            # ── V2 flags (fresh-window for W2 when W1 failed EMA) ─────────────
            if wave_num == 1:
                ema_clear_v2   = ema_clear
                rsi_extreme_v2 = rsi_extreme
                self._w1_passed_ema = ema_clear
                self._ep_v2_gate    = None
                self._ep_v2_rsi     = None
                self._w1_entry_rel  = cur_rel

            elif wave_num == 2 and not self._w1_passed_ema:
                # W1 failed EMA: give W2 a fresh window (W1_entry → W2_entry)
                w1r = self._w1_entry_rel or 0
                if direction == 'DOWN':
                    fresh_touch = any(b.high >= b.ema50 for b in ep[w1r:cur_rel + 1])
                    fresh_rsi   = min(b.rsi for b in ep[w1r:cur_rel + 1]) <= 30
                else:
                    fresh_touch = any(b.low <= b.ema50 for b in ep[w1r:cur_rel + 1])
                    fresh_rsi   = max(b.rsi for b in ep[w1r:cur_rel + 1]) >= 70
                ema_clear_v2   = not fresh_touch
                rsi_extreme_v2 = fresh_rsi
                self._ep_v2_gate = ema_clear_v2
                self._ep_v2_rsi  = rsi_extreme_v2

            elif not self._w1_passed_ema:
                # W3+ in W1-failed episode: inherit W2's gate
                ema_clear_v2   = bool(self._ep_v2_gate) if self._ep_v2_gate is not None else False
                rsi_extreme_v2 = bool(self._ep_v2_rsi)  if self._ep_v2_rsi  is not None else False

            else:
                # W1 passed EMA: V2 = V1 for all subsequent waves
                ema_clear_v2   = ema_clear
                rsi_extreme_v2 = rsi_extreme

            self._prev_entry_price    = entry_price
            self._prev_wave_entry_rel = cur_rel
            fired.append(pc)

            alert = Alert(
                symbol=self.symbol, tf=self.tf,
                direction='SHORT' if direction == 'DOWN' else 'LONG',
                wave_num=wave_num,
                entry_price=entry_price, sl_level=sl_level,
                sl_distance=sl_distance, sl_pct=sl_pct,
                swing_level=swing, rsi_at_entry=bar.rsi,
                ema_clear=ema_clear,       rsi_extreme=rsi_extreme,
                ema_clear_v2=ema_clear_v2, rsi_extreme_v2=rsi_extreme_v2,
                ep_len_so_far=cur_rel + 1, ts=bar.ts,
            )
            logger.info("[%s %dm] %s W%d @ %.2f  SL %.2f (%.2f%%)  tier=%s",
                        self.symbol, self.tf, alert.direction, wave_num,
                        entry_price, sl_level, sl_pct * 100,
                        'V1' if (ema_clear and rsi_extreme) else
                        'V2' if (ema_clear_v2 and rsi_extreme_v2) else 'raw')
            self.on_alert(alert)

        for pc in fired:
            self._pending.remove(pc)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _ep_bars(self) -> list[BarRecord]:
        return self._bars[self._ep_start:] if self._ep_start is not None else []

"""
Deduplicates alerts and distributes them to SSE subscribers.
"""
import threading
from collections import deque
from dataclasses import asdict

from strategy_engine import Alert
from rvol import VolumeSpike


def _ts_to_ist(ts: int) -> str:
    m = (ts // 60 + 330) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


class AlertManager:
    def __init__(self, max_history: int = 1000):
        self._lock     = threading.Lock()
        self._history: deque[dict] = deque(maxlen=max_history)
        self._seen:    set[str]    = set()
        self._queues:  list[deque] = []

    def add(self, alert: Alert):
        key = (f"{alert.symbol}:{alert.tf}:{alert.direction}"
               f":W{alert.wave_num}:{alert.entry_price:.2f}")
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
            d = asdict(alert)
            d['time_ist']   = _ts_to_ist(alert.ts)
            d['sl_pct_str'] = f"{alert.sl_pct * 100:.2f}%"
            self._history.append(d)
            for q in self._queues:
                q.append(d)

    def get_all(self) -> list[dict]:
        with self._lock:
            return list(reversed(self._history))

    def subscribe(self) -> deque:
        q: deque = deque()
        with self._lock:
            self._queues.append(q)
        return q

    def unsubscribe(self, q: deque):
        with self._lock:
            try:
                self._queues.remove(q)
            except ValueError:
                pass


class VolumeAlertManager:
    """Collects VolumeSpike events and fans them out to SSE subscribers."""

    def __init__(self, max_history: int = 500):
        self._lock    = threading.Lock()
        self._history: deque[dict] = deque(maxlen=max_history)
        self._seen:   set[str]     = set()
        self._queues: list[deque]  = []

    def add(self, spike: VolumeSpike) -> None:
        key = f"{spike.symbol}:{spike.ts}"
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
            d = {
                'symbol':     spike.symbol,
                'ts':         spike.ts,
                'time_ist':   spike.time_ist,
                'close':      spike.close,
                'volume':     spike.volume,
                'avg_volume': spike.avg_volume,
                'rvol':       round(spike.rvol, 2),
                'buyer_vol':  spike.buyer_vol,
                'seller_vol': spike.seller_vol,
                'buyer_pct':  round(spike.buyer_pct, 3),
                'seller_pct': round(1.0 - spike.buyer_pct, 3),
            }
            self._history.append(d)
            for q in self._queues:
                q.append(d)

    def get_all(self) -> list[dict]:
        with self._lock:
            return list(reversed(self._history))

    def subscribe(self) -> deque:
        q: deque = deque()
        with self._lock:
            self._queues.append(q)
        return q

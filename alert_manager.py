"""
Deduplicates alerts and distributes them to SSE subscribers.
"""
import threading
from collections import deque
from dataclasses import asdict

from strategy_engine import Alert


def _ts_to_ist(ts: int) -> str:
    m = (ts // 60 + 330) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


def _ts_to_date(ts: int) -> str:
    from datetime import datetime, timezone, timedelta
    ist = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%d-%b")


class AlertManager:
    def __init__(self, max_history: int = 5000):
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
            d['date_ist']   = _ts_to_date(alert.ts)
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

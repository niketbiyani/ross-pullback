"""
Deduplicates alerts and distributes them to SSE subscribers.
Persists today's alerts to a JSONL file so history survives restarts.
"""
import json
import os
import threading
from collections import deque
from dataclasses import asdict
from datetime import datetime, timezone, timedelta

from strategy_engine import Alert


def _ist_date() -> str:
    """Current date in IST (YYYY-MM-DD)."""
    ist = datetime.now(tz=timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%Y-%m-%d")


def _ts_to_ist(ts: int) -> str:
    m = (ts // 60 + 330) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


def _ts_to_date(ts: int) -> str:
    ist = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=5, minutes=30)))
    return ist.strftime("%d-%b")


class AlertManager:
    def __init__(self, max_history: int = 5000, persist_dir: str | None = None):
        self._lock     = threading.Lock()
        self._history: deque[dict] = deque(maxlen=max_history)
        self._seen:    set[str]    = set()
        self._queues:  list[deque] = []
        self._persist_file: str | None = None

        if persist_dir:
            date_str = _ist_date()
            self._persist_file = os.path.join(persist_dir, f"alerts_{date_str}.jsonl")
            self._load_today()
            self._cleanup_old(persist_dir)

    def _load_today(self):
        if not self._persist_file or not os.path.exists(self._persist_file):
            return
        loaded = 0
        try:
            with open(self._persist_file) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        key = d.get('_key', '')
                        if key and key in self._seen:
                            continue
                        if key:
                            self._seen.add(key)
                        self._history.append(d)
                        loaded += 1
                    except Exception:
                        pass
        except Exception:
            pass
        if loaded:
            import logging
            logging.getLogger(__name__).info(
                'Restored %d alerts from disk (%s)', loaded, self._persist_file)

    def _cleanup_old(self, persist_dir: str):
        if not self._persist_file:
            return
        current = os.path.basename(self._persist_file)
        try:
            for fn in os.listdir(persist_dir):
                if fn.startswith('alerts_') and fn.endswith('.jsonl') and fn != current:
                    try:
                        os.remove(os.path.join(persist_dir, fn))
                    except OSError:
                        pass
        except Exception:
            pass

    def _append_to_disk(self, d: dict):
        if not self._persist_file:
            return
        try:
            with open(self._persist_file, 'a') as f:
                f.write(json.dumps(d) + '\n')
        except Exception:
            pass

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
            d['_key']       = key
            self._history.append(d)
            for q in self._queues:
                q.append(d)
        self._append_to_disk(d)

    def add_event(self, d: dict):
        """Add a raw dict event (e.g. momentum signal) without an Alert object."""
        key = d.get('_key', '')
        with self._lock:
            if key and key in self._seen:
                return
            if key:
                self._seen.add(key)
            self._history.append(d)
            for q in self._queues:
                q.append(d)
        self._append_to_disk(d)

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

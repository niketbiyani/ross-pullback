"""
Deduplicates alerts and distributes them to SSE subscribers.
Persists today's alerts to a JSONL file so history survives restarts.
Retains the last 7 trading days of alert files and loads them all on startup.
"""
import json
import os
import threading
from collections import deque
from dataclasses import asdict
from datetime import date, datetime, timezone, timedelta

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


def _keep_dates(n: int = 7) -> set[str]:
    """Return ISO date strings for the last n trading days (weekdays only)."""
    result: set[str] = set()
    d = date.today()
    while len(result) < n:
        if d.weekday() < 5:
            result.add(d.isoformat())
        d -= timedelta(days=1)
    return result


class AlertManager:
    def __init__(self, max_history: int = 5000, persist_dir: str | None = None):
        self._lock     = threading.Lock()
        self._history: deque[dict] = deque(maxlen=max_history)
        self._seen:    set[str]    = set()
        self._queues:  list[deque] = []
        self._persist_dir:  str | None = persist_dir
        self._persist_file: str | None = None

        if persist_dir:
            date_str = _ist_date()
            self._persist_file = os.path.join(persist_dir, f"alerts_{date_str}.jsonl")
            self._load_recent()
            self._cleanup_old(persist_dir)

    def _load_recent(self):
        """Load the last 7 trading days of alerts from disk."""
        if not self._persist_dir:
            return
        keep = _keep_dates(7)
        loaded = 0
        for day_str in sorted(keep):          # chronological so newest is last → reversed() gives newest first
            day_file = os.path.join(self._persist_dir, f"alerts_{day_str}.jsonl")
            if not os.path.exists(day_file):
                continue
            try:
                with open(day_file) as f:
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
                'Restored %d alerts from disk (%d days)', loaded, len(keep))

    def _cleanup_old(self, persist_dir: str):
        keep_files = {f"alerts_{d}.jsonl" for d in _keep_dates(7)}
        try:
            for fn in os.listdir(persist_dir):
                if fn.startswith('alerts_') and fn.endswith('.jsonl') and fn not in keep_files:
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

    def add_historical(self, alert: Alert, day_str: str):
        """Add a backfilled alert for a past trading day. Does not push to live SSE queues."""
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
            # Historical alerts are not pushed to live SSE queues
        if self._persist_dir:
            day_file = os.path.join(self._persist_dir, f"alerts_{day_str}.jsonl")
            try:
                with open(day_file, 'a') as f:
                    f.write(json.dumps(d) + '\n')
            except Exception:
                pass

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

import time
import logging
import threading
from datetime import datetime, date, timezone, timedelta
from collections import deque
from tradingview_screener import Query, Column
from config import Config

logger = logging.getLogger(__name__)

# Helpers for IST conversion
def _ist_time(ts: float) -> str:
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    dt = datetime.fromtimestamp(ts, tz=ist_tz)
    return dt.strftime("%H:%M:%S")

def _ts_to_date(ts: float) -> str:
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    dt = datetime.fromtimestamp(ts, tz=ist_tz)
    return dt.strftime("%d-%b")

def _ts_to_date_iso(ts: float) -> str:
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    dt = datetime.fromtimestamp(ts, tz=ist_tz)
    return dt.date().isoformat()

class TVScanner:
    def __init__(self, alert_mgr):
        self.alert_mgr = alert_mgr
        self.running = False
        self.thread = None
        
        # State tracking per symbol and timeframe
        # symbol -> tf -> deque of dicts ({'open', 'high', 'low', 'close', 'ts'})
        self.bar_history = {}
        # symbol -> tf -> current active forming bar dict
        self.current_bars = {}
        # (symbol, tf) -> last rapid alert state dictionary
        self.last_rapid_alerts = {}
        
        # Globally tracked fields for APIs
        self.peak_momentum = {}   # symbol -> {'pct', 'ts', 'window', 'tf'}
        self.leaderboard_data = [] # List of dicts for compute_leaderboard
        self.active_symbols = set()
        
        # Live status
        self.is_live = False

    def start(self):
        self.running = True
        self.is_live = True
        self.thread = threading.Thread(target=self._run, daemon=True, name="TVScannerLoop")
        self.thread.start()
        logger.info("TVScanner polling loop started.")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        logger.info("TVScanner polling loop stopped.")

    def _run(self):
        # We query the screener every 1.5 seconds
        poll_interval = 1.5
        
        while self.running:
            t0 = time.time()
            try:
                self._poll_once()
            except Exception as e:
                logger.error("Error during TradingView screen poll: %s", e, exc_info=True)
            
            elapsed = time.time() - t0
            sleep_time = max(0.1, poll_interval - elapsed)
            time.sleep(sleep_time)

    def _poll_once(self):
        # Build TradingView Query
        q = (Query()
             .set_markets('india')
             .select(
                 'name',
                 'close',
                 'open',
                 'high',
                 'low',
                 'volume',
                 'change',
                 'relative_volume_10d_calc',
                 'RSI|1',
                 'RSI|5',
                 'RSI|15',
                 'MACD.macd|1',
                 'MACD.macd|5',
                 'MACD.macd|15',
                 'MACD.signal|1',
                 'MACD.signal|5',
                 'MACD.signal|15',
                 'EMA20|1',
                 'EMA20|5',
                 'EMA20|15',
                 'EMA50|1',
                 'EMA50|5',
                 'EMA50|15'
             )
             .where(
                 Column('exchange') == 'NSE',
                 Column('volume') > Config.MOVER_MIN_VOLUME,
                 Column('close') > Config.CLOSE_MIN_PRICE
             ))
        
        count, df = q.get_scanner_data()
        if df.empty:
            return

        now_ts = int(time.time())
        leaderboard_rows = []

        # Process each row
        for _, row_data in df.iterrows():
            symbol = row_data.get('name')
            if not symbol:
                continue

            close = float(row_data.get('close', 0.0) or 0.0)
            open_d = float(row_data.get('open', 0.0) or 0.0)
            high_d = float(row_data.get('high', 0.0) or 0.0)
            low_d = float(row_data.get('low', 0.0) or 0.0)
            volume = float(row_data.get('volume', 0.0) or 0.0)
            change = float(row_data.get('change', 0.0) or 0.0)
            rvol = float(row_data.get('relative_volume_10d_calc', 0.0) or 0.0)

            if close <= 0:
                continue

            day_range_pct = 0.0
            if open_d > 0:
                day_range_pct = round((high_d - low_d) / open_d * 100, 2)

            self.active_symbols.add(symbol)

            # Build leaderboard row
            leaderboard_rows.append({
                'symbol': symbol,
                'bar_rvol': round(rvol, 1),
                'overnight_chg': change,
                'rs_coverage': 0.0,
                'rs_elapsed': 0,
                'day_range_pct': day_range_pct,
                'cb_ratio': 0.0,
                'cb_pct': 0.0,
                'cb_time': '',
                'buyer_pct': 0.5,
                'today_volume': volume
            })

            # Check timeframes 1m, 5m, 15m
            for tf in (1, 5, 15):
                # Calculate bar start timestamp
                tf_seconds = tf * 60
                bar_ts = (now_ts // tf_seconds) * tf_seconds

                # Manage in-memory bar aggregation
                if symbol not in self.bar_history:
                    self.bar_history[symbol] = {}
                if tf not in self.bar_history[symbol]:
                    self.bar_history[symbol][tf] = deque(maxlen=5)

                if symbol not in self.current_bars:
                    self.current_bars[symbol] = {}

                # Detect new bar boundary
                current_bar = self.current_bars[symbol].get(tf)
                if current_bar is None or current_bar['ts'] != bar_ts:
                    if current_bar is not None:
                        # Append closed bar to history
                        self.bar_history[symbol][tf].append(current_bar)
                    # Initialize new bar
                    current_bar = {
                        'ts': bar_ts,
                        'open': close,
                        'high': close,
                        'low': close,
                        'close': close
                    }
                    self.current_bars[symbol][tf] = current_bar
                else:
                    # Update active forming bar
                    current_bar['high'] = max(current_bar['high'], close)
                    current_bar['low'] = min(current_bar['low'], close)
                    current_bar['close'] = close

                # Combine history + current bar to get full list of up to 5 bars
                hist = list(self.bar_history[symbol][tf]) + [current_bar]

                # Calculate best move pct over last N bars
                best_pct = 0.0
                best_win = 1
                for n in range(1, len(hist) + 1):
                    win = hist[-n:]
                    if len(win) > 0:
                        low_idx = min(range(len(win)), key=lambda idx: win[idx]['low'])
                        high_idx = max(range(len(win)), key=lambda idx: win[idx]['high'])
                        if low_idx <= high_idx and high_idx == len(win) - 1:
                            ref_p = win[low_idx]['low'] if win[low_idx]['low'] > 0 else win[0]['open']
                            if ref_p > 0:
                                move = (win[high_idx]['high'] - win[low_idx]['low']) / ref_p * 100
                                if move > best_pct:
                                    best_pct = move
                                    best_win = n

                # Update peak momentum
                if symbol not in self.peak_momentum:
                    self.peak_momentum[symbol] = {'pct': 0.0, 'ts': bar_ts, 'window': 1, 'tf': tf}
                if best_pct > self.peak_momentum[symbol]['pct']:
                    self.peak_momentum[symbol] = {
                        'ts': bar_ts,
                        'pct': round(best_pct, 2),
                        'window': best_win,
                        'tf': tf
                    }

                # ── State Machine: Spiking ──
                key = (symbol, tf)
                last_rapid = self.last_rapid_alerts.get(key)

                rsi_val = float(row_data.get(f'RSI|{tf}', 0.0) or 0.0)

                should_alert = False
                if best_pct >= Config.RAPID_MIN_PCT:
                    if last_rapid is None:
                        should_alert = True
                    else:
                        time_passed = (now_ts - last_rapid['ts']) >= 300
                        extended = (best_pct - last_rapid['pct']) >= 0.5
                        if time_passed or extended:
                            should_alert = True

                if should_alert:
                    # Create SPIKING Alert
                    breakout_ts = last_rapid['breakout_ts'] if (last_rapid and 'breakout_ts' in last_rapid) else now_ts
                    rapid_evt = {
                        'symbol':        symbol,
                        'tf':            tf,
                        'direction':     'LONG',
                        'wave_num':      0,
                        'entry_price':   close,
                        'sl_level':      current_bar['low'],
                        'sl_distance':   close - current_bar['low'],
                        'sl_pct':        round((close - current_bar['low']) / close if close > 0 else 0.0, 4),
                        'swing_level':   current_bar['open'],
                        'rsi_at_entry':  rsi_val,
                        'ema_clear':     True,
                        'rsi_extreme':   True,
                        'ema_clear_v2':  True,
                        'rsi_extreme_v2':True,
                        'ep_len_so_far': best_win,
                        'ts':            now_ts,
                        'type':          'rapid',
                        'breakout_ts':   breakout_ts,
                        'breakout_time_ist': _ist_time(breakout_ts),
                        'pct':           round(best_pct, 2),
                        'status':        'SPIKING',
                        'today_volume':  volume,
                        'rel_volume':    round(rvol, 2),
                        'day_range_pct': day_range_pct,
                        'time_ist':      _ist_time(now_ts),
                        'date_ist':      _ts_to_date(now_ts),
                        'date_iso':      _ts_to_date_iso(now_ts),
                        'sl_pct_str':    f"{((close - current_bar['low']) / close * 100):.2f}%" if close > 0 else "0.00%",
                        '_key':          f"RAPID:{symbol}:{tf}:{breakout_ts}:{best_pct:.2f}"
                    }
                    self.last_rapid_alerts[key] = {
                        'ts': now_ts,
                        'pct': best_pct,
                        'status': 'SPIKING',
                        'breakout_ts': breakout_ts,
                        'event': rapid_evt
                    }
                    self.alert_mgr.add_event(rapid_evt)

                # ── State Machine: Pause ──
                # Check for pullback red candle: close is lower than open or previous close
                prev_c = hist[-2].get('close', current_bar['open']) if len(hist) >= 2 else current_bar['open']
                if close < current_bar['open'] or close < prev_c:
                    if last_rapid is not None and last_rapid['status'] == 'SPIKING':
                        bars_since = (now_ts - last_rapid['ts']) // (60 * tf)
                        if 1 <= bars_since <= 5:
                            last_rapid['status'] = 'PAUSE'
                            last_rapid['trigger_level'] = current_bar['open']
                            
                            pause_evt = {
                                **last_rapid['event'],
                                'ts':            now_ts,
                                'time_ist':      _ist_time(now_ts),
                                'status':        'PAUSE',
                                'entry_price':   current_bar['open'],  # open of red candle
                                'sl_level':      current_bar['low'],   # low of red candle
                                'sl_distance':   current_bar['open'] - current_bar['low'],
                                'sl_pct':        round((current_bar['open'] - current_bar['low']) / current_bar['open'] if current_bar['open'] > 0 else 0.0, 4),
                                'sl_pct_str':    f"{((current_bar['open'] - current_bar['low']) / current_bar['open'] * 100):.2f}%" if current_bar['open'] > 0 else "0.00%",
                                '_key':          f"RAPID_PAUSE:{symbol}:{tf}:{now_ts}"
                            }
                            last_rapid['event'] = pause_evt
                            self.alert_mgr.add_event(pause_evt)

                # ── State Machine: Triggered ──
                if last_rapid is not None and last_rapid['status'] == 'PAUSE' and last_rapid['event'].get('ts', 0) < now_ts:
                    if close >= last_rapid.get('trigger_level', 999999.0):
                        last_rapid['status'] = 'TRIGGERED'
                        
                        triggered_evt = {
                            **last_rapid['event'],
                            'ts':            now_ts,
                            'time_ist':      _ist_time(now_ts),
                            'status':        'TRIGGERED',
                            '_key':          f"RAPID_TRIG:{symbol}:{tf}:{now_ts}"
                        }
                        last_rapid['event'] = triggered_evt
                        self.alert_mgr.add_event(triggered_evt)

        # Sort and update leaderboard data
        leaderboard_rows.sort(key=lambda x: x['bar_rvol'], reverse=True)
        self.leaderboard_data = leaderboard_rows[:50]

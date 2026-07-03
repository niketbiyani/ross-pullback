"""
FNO Micro-Pullback Scanner
===========================
MACD micro-pullback signals on 15 / 30 / 60-min bars for the NSE F&O universe.
1 month of bar data for accurate MACD warmup; signals from the past 7 trading
days shown on the dashboard calendar.  Momentum (MOM) events detected on 1-min
bars as usual.

Dashboard:  http://localhost:5051
"""
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta

from dhanhq import DhanContext

from fno_config import FnoConfig

# Swap Config in dhan_feed before any imports that pull it transitively
import dhan_feed as _df
_df.Config = FnoConfig

from dhan_feed import bootstrap, LiveFeed, _load_cache, _trading_days, _resample
from strategy_engine import StrategyEngine, BarRecord, Alert
from indicators import IndicatorSet
from alert_manager import AlertManager
from dashboard import create_app
from fno_universe import build_fno_universe

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-5s %(name)s — %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main():
    errors = FnoConfig.validate()
    if errors:
        for e in errors:
            logger.error('Config: %s', e)
        sys.exit(1)

    _here = os.path.dirname(os.path.abspath(__file__))
    ctx   = DhanContext(FnoConfig.DHAN_CLIENT_ID, FnoConfig.DHAN_ACCESS_TOKEN)

    # Separate alert directory so FNO alerts don't mix with the intraday scanner
    _alerts_dir = os.path.join(_here, 'fno_alerts')
    os.makedirs(_alerts_dir, exist_ok=True)
    alert_mgr = AlertManager(persist_dir=_alerts_dir)

    # ── state ─────────────────────────────────────────────────────────────────
    ind_sets:  dict[tuple, IndicatorSet]   = {}
    engines:   dict[tuple, StrategyEngine] = {}
    bootstrap_last_ts: dict[tuple, int]    = {}

    is_live = False

    def _trading_day() -> date:
        d = date.today()
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    _active_date = _trading_day()
    _cutoff_date = _active_date - timedelta(days=FnoConfig.STRATEGY_LOOKBACK_DAYS)
    _strategy_cutoff = int(datetime(
        _cutoff_date.year, _cutoff_date.month, _cutoff_date.day,
    ).timestamp())

    today_volumes: dict[str, float] = {}
    today_bars:    dict[str, deque] = {}
    today_highs:   dict[str, float] = {}
    today_lows:    dict[str, float] = {}
    today_opens:   dict[str, float] = {}
    prev_closes:   dict[str, float] = {}
    overnight_chg: dict[str, float] = {}

    bar_history:    dict[str, deque] = {}
    peak_momentum:  dict[str, dict]  = {}
    mom_last_fired: dict[str, int]   = {}
    moves_log:      dict[str, list]  = {}

    alerted_symbols: set[str] = set()   # any signal today (MACD or MOM)

    _bootstrap_prev_close: dict[str, float] = {}

    # ── helpers ───────────────────────────────────────────────────────────────
    def _ist_time(ts: int) -> str:
        m = (ts // 60 + 330) % (24 * 60)
        return f"{m // 60:02d}:{m % 60:02d}"

    def _ts_to_date_label(ts: int) -> str:
        from datetime import timezone, timedelta as _td
        ist = datetime.fromtimestamp(ts, tz=timezone(_td(hours=5, minutes=30)))
        return ist.strftime("%d-%b")

    # ── leaderboard ───────────────────────────────────────────────────────────
    def compute_leaderboard() -> list[dict]:
        if _active_date == date.today():
            now_ist_min = (int(time.time()) // 60 + 330) % (24 * 60)
            elapsed_min = min(max(now_ist_min - 555, 1), 375)
        else:
            elapsed_min = 375

        rows = []
        for symbol in list(alerted_symbols):
            today_vol = today_volumes.get(symbol, 0.0)
            o_chg     = overnight_chg.get(symbol, 0.0)

            open_p        = today_opens.get(symbol, 0.0)
            day_range_pct = 0.0
            if open_p > 0:
                h = today_highs.get(symbol, 0.0)
                l = today_lows.get(symbol, 0.0)
                if h > l > 0:
                    day_range_pct = round((h - l) / open_p * 100, 2)

            bars      = today_bars.get(symbol)
            buyer_pct = 0.5
            if bars:
                tv = tb = 0.0
                for bar_d in bars:
                    vol_b = bar_d.get('volume', 0.0)
                    rng   = max(bar_d.get('high', 0.0) - bar_d.get('low', 0.0), 1e-6)
                    tv   += vol_b
                    tb   += vol_b * max(bar_d.get('close', 0.0) - bar_d.get('low', 0.0), 0.0) / rng
                if tv > 0:
                    buyer_pct = tb / tv

            base = {
                'symbol':        symbol,
                'bar_rvol':      0.0,
                'overnight_chg': o_chg,
                'rs_coverage':   0.0,
                'rs_elapsed':    0,
                'day_range_pct': day_range_pct,
                'cb_ratio':      0.0,
                'cb_pct':        0.0,
                'cb_time':       '',
                'ratio':         0.0,
                'today_vol':     round(today_vol),
                'avg_daily':     0,
                'elapsed':       elapsed_min,
                'buyer_pct':     round(buyer_pct, 3),
                'seller_pct':    round(1.0 - buyer_pct, 3),
            }

            moves = moves_log.get(symbol, [])
            if not moves:
                mom_ev = peak_momentum.get(symbol)
                if mom_ev:
                    moves = [mom_ev]
                else:
                    moves = [{'ts': 0, 'pct': 0.0, 'window': 0}]

            for mv in moves:
                row = dict(base)
                row['peak_mom_pct']  = mv['pct']
                row['peak_mom_win']  = mv.get('window', 0)
                row['peak_mom_time'] = _ist_time(mv['ts']) if mv.get('ts') else ''
                rows.append(row)

        rows.sort(key=lambda x: x['overnight_chg'], reverse=True)
        return rows

    # ── alert callback ────────────────────────────────────────────────────────
    def on_alert(alert: Alert):
        if not is_live:
            return
        if date.fromtimestamp(alert.ts) < _cutoff_date:
            return
        alert.today_volume = today_volumes.get(alert.symbol, 0.0)
        alert.rel_volume   = 0.0
        alerted_symbols.add(alert.symbol)
        alert_mgr.add(alert)

    # ── bar callback ──────────────────────────────────────────────────────────
    def on_bar(symbol: str, tf: int, bar):
        nonlocal is_live
        b      = bar if isinstance(bar, dict) else bar.__dict__
        bar_ts = b.get('ts', b.get('timestamp', 0))

        if tf == 1:
            # ── 1-min: update today's state + detect momentum ─────────────
            vol    = b.get('volume', 0.0)
            high   = b.get('high',   0.0)
            low    = b.get('low',    0.0)
            open_p = b.get('open',   0.0)
            close  = b.get('close',  0.0)
            bar_date      = date.fromtimestamp(bar_ts)
            is_active_bar = (bar_date == _active_date)

            if is_active_bar:
                if is_live:
                    today_volumes[symbol] = today_volumes.get(symbol, 0.0) + vol
                if symbol not in today_bars:
                    today_bars[symbol] = deque(maxlen=10)
                today_bars[symbol].append(b)

                if symbol not in today_opens:        today_opens[symbol] = open_p
                if high > today_highs.get(symbol, 0.0): today_highs[symbol] = high
                if symbol not in today_lows or low < today_lows[symbol]:
                    today_lows[symbol] = low

                prev_c = prev_closes.get(symbol, 0.0)
                if prev_c > 0 and close > 0:
                    overnight_chg[symbol] = round((close - prev_c) / prev_c * 100, 2)

                # Momentum: best % range across 1–5 consecutive bars
                if symbol not in bar_history:
                    bar_history[symbol] = deque(maxlen=5)
                bar_history[symbol].append({'open': open_p, 'high': high, 'low': low, 'ts': bar_ts})
                hist     = list(bar_history[symbol])
                best_pct = 0.0
                best_win = 1
                for n in range(1, len(hist) + 1):
                    win = hist[-n:]
                    ref = win[0]['open']
                    if ref > 0:
                        move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref * 100
                        if move > best_pct:
                            best_pct = move
                            best_win = n
                if best_pct > peak_momentum.get(symbol, {}).get('pct', 0.0):
                    peak_momentum[symbol] = {'ts': bar_ts, 'pct': round(best_pct, 2), 'window': best_win}
                if best_pct >= FnoConfig.MOVER_MIN_PCT:
                    alerted_symbols.add(symbol)

                if is_live and best_pct >= FnoConfig.MOVER_MIN_PCT:
                    if bar_ts - mom_last_fired.get(symbol, 0) >= 300:
                        mom_last_fired[symbol] = bar_ts
                        now_ts = int(time.time())
                        logger.info('MOM %s %.2f%% bar=%s detected=%s lag=%ds',
                                    symbol, best_pct, _ist_time(bar_ts),
                                    _ist_time(now_ts), now_ts - (bar_ts + 60))
                        if symbol not in moves_log:
                            moves_log[symbol] = []
                        if not any(m['ts'] == bar_ts for m in moves_log[symbol]):
                            moves_log[symbol].append(
                                {'ts': bar_ts, 'pct': round(best_pct, 2), 'window': best_win}
                            )
            else:
                if not is_live:
                    _bootstrap_prev_close[symbol] = close
            return   # 1-min: state-only, MACD strategy runs on higher TFs only

        # ── 15 / 30 / 60-min: MACD strategy ─────────────────────────────────
        key = (symbol, tf)
        if key not in ind_sets:
            ind_sets[key] = IndicatorSet()
            engines[key]  = StrategyEngine(symbol, tf, on_alert)

        vals = ind_sets[key].update(bar)
        if vals is None:
            return

        if bar_ts < _strategy_cutoff:
            # Still track ts so live feed picks up from here
            if bar_ts > bootstrap_last_ts.get(key, 0):
                bootstrap_last_ts[key] = bar_ts
            return

        rec = BarRecord(
            ts=bar_ts,
            open=b.get('open', 0.0),   high=b.get('high', 0.0),
            low=b.get('low',  0.0),    close=b.get('close', 0.0),
            volume=b.get('volume', 0.0),
            macd=vals['macd'],   signal=vals['signal'],
            ema50=vals['ema50'], rsi=vals['rsi'],
        )
        engines[key].update(rec)
        if bar_ts > bootstrap_last_ts.get(key, 0):
            bootstrap_last_ts[key] = bar_ts

    # ── dashboard ─────────────────────────────────────────────────────────────
    _rescan_ref: dict = {}
    app = create_app(
        alert_mgr, compute_leaderboard,
        rescan_ref=_rescan_ref,
        get_peak_momentum=lambda: peak_momentum,
        js_defaults={'lbMinVol': 0, 'lbMinMom': 0},
        title='FNO Scanner',
    )
    logger.info('Dashboard → http://%s:%d', FnoConfig.DASHBOARD_HOST, FnoConfig.DASHBOARD_PORT)

    flask_thread = threading.Thread(
        target=lambda: app.run(
            host=FnoConfig.DASHBOARD_HOST, port=FnoConfig.DASHBOARD_PORT,
            threaded=True, use_reloader=False,
        ),
        daemon=True, name='FnoDashboard',
    )
    flask_thread.start()
    time.sleep(1)

    # ── universe ──────────────────────────────────────────────────────────────
    symbols = build_fno_universe()
    if not symbols:
        logger.error('FNO universe empty — check Dhan scrip master connectivity')
        sys.exit(1)
    logger.info('FNO universe: %d symbols loaded', len(symbols))

    # ── Phase 1: populate Movers from today's disk cache ─────────────────────
    def _scan_today_bars():
        today = _active_date.isoformat()
        local_last_fired: dict[str, int] = {}
        for sec in symbols:
            name = sec['symbol']
            bars = sorted(_load_cache(name).get(today, []), key=lambda x: x['ts'])
            if not bars:
                continue
            scan_events: list[dict] = []
            bh = deque(maxlen=5)
            vol_total = t_high = t_open = t_close = 0.0
            t_low = float('inf')
            for b in bars:
                ts     = b.get('ts',     0)
                open_p = b.get('open',   0.0)
                high   = b.get('high',   0.0)
                low    = b.get('low',    0.0)
                close  = b.get('close',  0.0)
                vol    = b.get('volume', 0.0)
                vol_total += vol
                if t_open == 0.0:             t_open = open_p
                if high > t_high:             t_high = high
                if low > 0 and low < t_low:   t_low  = low
                t_close = close
                bh.append({'open': open_p, 'high': high, 'low': low, 'ts': ts})
                hist     = list(bh)
                best_pct = 0.0
                best_win = 1
                for n in range(1, len(hist) + 1):
                    win = hist[-n:]
                    ref = win[0]['open']
                    if ref > 0:
                        move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref * 100
                        if move > best_pct:
                            best_pct = move
                            best_win = n
                if best_pct > peak_momentum.get(name, {}).get('pct', 0.0):
                    peak_momentum[name] = {'ts': ts, 'pct': round(best_pct, 2), 'window': best_win}
                if best_pct >= FnoConfig.MOVER_MIN_PCT:
                    alerted_symbols.add(name)
                if best_pct >= FnoConfig.MOVER_MIN_PCT and ts - local_last_fired.get(name, 0) >= 300:
                    local_last_fired[name] = ts
                    scan_events.append({'ts': ts, 'pct': round(best_pct, 2), 'window': best_win})

            # Merge: only add ts values not already in moves_log
            existing_ts = {m['ts'] for m in moves_log.get(name, [])}
            if name not in moves_log:
                moves_log[name] = []
            for ev in scan_events:
                if ev['ts'] not in existing_ts:
                    moves_log[name].append(ev)
                    existing_ts.add(ev['ts'])

            if vol_total > 0:                        today_volumes[name] = vol_total
            if t_high > 0:                           today_highs[name]   = t_high
            if t_low < float('inf') and t_low > 0:  today_lows[name]    = t_low
            if t_open > 0 and name not in today_opens: today_opens[name] = t_open
            prev_c = prev_closes.get(name, 0.0)
            if prev_c > 0 and t_close > 0:
                overnight_chg[name] = round((t_close - prev_c) / prev_c * 100, 2)

    _scan_today_bars()
    logger.info('Phase 1 complete — %d symbols with data, %d with momentum signals',
                len(today_volumes), len(alerted_symbols))

    # ── Phase 2: full bootstrap (22 days) ────────────────────────────────────
    _bootstrap_done = threading.Event()

    def _backfill_history():
        """Run MACD strategy for each of the past 7 trading days and persist alerts."""
        all_days  = _trading_days(FnoConfig.HISTORY_DAYS)
        today_str = date.today().isoformat()
        past_days = sorted(d for d in all_days if d < today_str)[-FnoConfig.STRATEGY_LOOKBACK_DAYS:]
        if not past_days:
            logger.info('Backfill: no past days in cache')
            return

        past_days_set = set(past_days)
        logger.info('Backfill: %d past days (%s → %s)', len(past_days), past_days[0], past_days[-1])

        for sec in symbols:
            name = sec['symbol']
            try:
                cache   = _load_cache(name)
                bars_1m = sorted(
                    [b for day in all_days for b in cache.get(day, [])],
                    key=lambda x: x['ts'],
                )
                if not bars_1m:
                    continue

                # MOM events on 1-min bars for past days
                bh: deque = deque(maxlen=5)
                mom_last_bf = 0
                for b in bars_1m:
                    ts    = b.get('ts', 0)
                    b_day = date.fromtimestamp(ts).isoformat()
                    bh.append({'open': b.get('open', 0.0), 'high': b.get('high', 0.0),
                               'low':  b.get('low',  0.0), 'ts': ts})
                    if b_day not in past_days_set:
                        continue
                    hist     = list(bh)
                    best_pct = 0.0
                    best_win = 1
                    for n in range(1, len(hist) + 1):
                        win = hist[-n:]
                        ref = win[0]['open']
                        if ref > 0:
                            move = (max(w['high'] for w in win) - min(w['low'] for w in win)) / ref * 100
                            if move > best_pct:
                                best_pct = move
                                best_win = n
                    if best_pct >= FnoConfig.MOVER_MIN_PCT and ts - mom_last_bf >= 300:
                        mom_last_bf = ts
                        alert_mgr.add_historical_event({
                            'alert_type': 'MOM', 'symbol': name, 'tf': 1,
                            'pct': round(best_pct, 2), 'window': best_win,
                            'ts': ts,
                            'time_ist': _ist_time(ts),
                            'date_ist': _ts_to_date_label(ts),
                            'today_volume': 0.0,
                            '_key': f"{name}:MOM:{ts}",
                        }, b_day)

                # MACD strategy on 15 / 30 / 60-min for past days
                for tf in (15, 30, 60):
                    tf_bars        = _resample(bars_1m, tf)
                    ind            = IndicatorSet()
                    alerts_by_day: dict[str, list] = {}

                    def _cb(a: Alert, _dst=alerts_by_day, _pds=past_days_set):
                        a_day = date.fromtimestamp(a.ts).isoformat()
                        if a_day in _pds:
                            _dst.setdefault(a_day, []).append(a)

                    engine = StrategyEngine(name, tf, _cb)
                    for b_dict in tf_bars:
                        b_dict = b_dict if isinstance(b_dict, dict) else b_dict.__dict__
                        vals   = ind.update(b_dict)
                        if vals is None:
                            continue
                        rec = BarRecord(
                            ts=b_dict.get('ts', 0),
                            open=b_dict.get('open', 0.0),  high=b_dict.get('high', 0.0),
                            low=b_dict.get('low',  0.0),   close=b_dict.get('close', 0.0),
                            volume=b_dict.get('volume', 0.0),
                            macd=vals['macd'],   signal=vals['signal'],
                            ema50=vals['ema50'], rsi=vals['rsi'],
                        )
                        engine.update(rec)

                    for day_str, day_alerts in alerts_by_day.items():
                        for alert in day_alerts:
                            alert_mgr.add_historical(alert, day_str)

            except Exception as e:
                logger.debug('Backfill %s: %s', name, e)

        logger.info('Backfill complete — %d total alerts', len(alert_mgr.get_all()))

    def _run_bootstrap():
        nonlocal is_live
        logger.info('Bootstrap: %d symbols × %d days (15/30/60-min MACD + 1-min state)',
                    len(symbols), FnoConfig.HISTORY_DAYS)
        try:
            bootstrap(ctx, symbols, on_bar)
        except Exception as e:
            logger.warning('Bootstrap incomplete (%s) — continuing with partial state', e)

        prev_closes.update(_bootstrap_prev_close)
        _bootstrap_prev_close.clear()
        logger.info('Prev closes ready: %d symbols', len(prev_closes))

        is_live = True
        logger.info('Phase 2 complete — MACD alerts active, starting live feed')
        _bootstrap_done.set()
        threading.Thread(target=_backfill_history, daemon=True, name='FnoBackfill').start()

    threading.Thread(target=_run_bootstrap, daemon=True, name='FnoBootstrap').start()
    logger.info('Bootstrap running in background — Movers tab live from Phase 1 cache')

    try:
        while not _bootstrap_done.is_set():
            _bootstrap_done.wait(timeout=1.0)
    except KeyboardInterrupt:
        logger.info('Shutting down during bootstrap...')
        return

    # ── rescan (on-demand via dashboard button) ───────────────────────────────
    def rescan_today() -> int:
        before = set(alerted_symbols)
        _scan_today_bars()
        n_new = len(alerted_symbols - before)
        logger.info('Rescan: %d new symbols (total: %d)', n_new, len(alerted_symbols))
        return n_new

    _rescan_ref['fn'] = rescan_today

    # ── live feed ─────────────────────────────────────────────────────────────
    # All FNO symbols are always "active" — poll all ~200 every 15 s cycle
    feed = LiveFeed(ctx, symbols, on_bar,
                    priority_fn=lambda: {s['symbol'] for s in symbols})
    feed._last_ts.update(bootstrap_last_ts)
    logger.info('Live feed seeded with %d timestamps, polling %d symbols every cycle',
                len(bootstrap_last_ts), len(symbols))
    feed.start()

    # ── heartbeat ─────────────────────────────────────────────────────────────
    try:
        while True:
            time.sleep(300)
            logger.info('Heartbeat — engines: %d  alerts: %d  leaderboard: %d',
                        len(ind_sets), len(alert_mgr.get_all()), len(alerted_symbols))
    except KeyboardInterrupt:
        logger.info('Shutting down...')
        feed.stop()


if __name__ == '__main__':
    main()

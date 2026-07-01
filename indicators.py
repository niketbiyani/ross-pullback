"""
Incremental MACD(12,26,9), EMA(50), RSI(14).
Each class maintains running state; call update() once per closed bar.
"""


class _EMA:
    def __init__(self, period: int):
        self.period = period
        self.k = 2.0 / (period + 1)
        self.value: float | None = None
        self._buf: list[float] = []

    def update(self, price: float) -> float | None:
        if self.value is None:
            self._buf.append(price)
            if len(self._buf) >= self.period:
                self.value = sum(self._buf) / len(self._buf)
                self._buf = []
        else:
            self.value = price * self.k + self.value * (1 - self.k)
        return self.value


class RSI:
    def __init__(self, period: int = 14):
        self.period = period
        self._prev: float | None = None
        self._buf_g: list[float] = []
        self._buf_l: list[float] = []
        self._avg_g: float | None = None
        self._avg_l: float | None = None
        self.value: float | None = None

    def update(self, close: float) -> float | None:
        if self._prev is None:
            self._prev = close
            return None
        change = close - self._prev
        g, l = max(change, 0.0), max(-change, 0.0)
        self._prev = close
        if self._avg_g is None:
            self._buf_g.append(g)
            self._buf_l.append(l)
            if len(self._buf_g) >= self.period:
                self._avg_g = sum(self._buf_g) / self.period
                self._avg_l = sum(self._buf_l) / self.period
                self._buf_g = []
                self._buf_l = []
        else:
            self._avg_g = (self._avg_g * (self.period - 1) + g) / self.period
            self._avg_l = (self._avg_l * (self.period - 1) + l) / self.period
        if self._avg_g is not None:
            self.value = 100.0 if self._avg_l == 0 else 100.0 - 100.0 / (1.0 + self._avg_g / self._avg_l)
        return self.value


class MACD:
    """MACD(fast, slow, signal) via EMA."""
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self._fast   = _EMA(fast)
        self._slow   = _EMA(slow)
        self._signal = _EMA(signal)
        self.macd_line: float | None   = None
        self.signal_line: float | None = None

    def update(self, close: float) -> tuple[float | None, float | None]:
        f = self._fast.update(close)
        s = self._slow.update(close)
        if f is not None and s is not None:
            self.macd_line = f - s
            self.signal_line = self._signal.update(self.macd_line)
        return self.macd_line, self.signal_line


class IndicatorSet:
    """All indicators for one (symbol, timeframe)."""
    def __init__(self):
        self.macd  = MACD(12, 26, 9)
        self.ema50 = _EMA(50)
        self.rsi   = RSI(14)

    def update(self, bar) -> dict | None:
        """
        bar: dict with keys open/high/low/close/volume/ts,
             or object with those attributes.
        Returns dict(macd, signal, ema50, rsi) once all indicators are ready.
        """
        close = bar['close'] if isinstance(bar, dict) else bar.close
        m, s  = self.macd.update(close)
        e     = self.ema50.update(close)
        r     = self.rsi.update(close)
        if None in (m, s, e, r):
            return None
        return {'macd': m, 'signal': s, 'ema50': e, 'rsi': r}

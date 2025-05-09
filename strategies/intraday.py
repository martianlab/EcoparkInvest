import numpy as np
import pandas as pd
from strategies import BaseStrategy, register

@register('opening_range')
class OpeningRangeStrategy(BaseStrategy):
    """
    Intraday Opening Range Breakout:
    – строит High/Low первых `minutes` минут сессии
    – в лонг, когда цена закрытия пробивает вверх этот High
    – в шорт, когда пробивает вниз этот Low
    """
    def __init__(self, minutes: int = 15, risk_target: float = 0.01):
        self.minutes = minutes
        self.risk = risk_target
        self.reasons = {}

    def _single(self, df: pd.DataFrame) -> pd.Series:
        # группируем по датам
        df = df.copy()
        df['date'] = df.index.date
        out = pd.Series(0.0, index=df.index)
        for d, group in df.groupby('date'):
            # первые N строк дня
            first = group.iloc[:self.minutes]
            if first.empty:
                self.reasons[str(d)] = f"no data for opening range on {d}"
                continue
            high0 = first['High'].max()
            low0  = first['Low'].min()
            day_slice = group.index
            # сигналы
            long_sig  = (df.loc[day_slice, 'Close'] > high0)
            short_sig = (df.loc[day_slice, 'Close'] < low0)
            pos = long_sig.astype(float) - short_sig.astype(float)
            out.loc[day_slice] = pos * self.risk
        return out.fillna(0)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        for sym in df.columns.get_level_values(0).unique():
            intraday = df[sym]
            result[sym] = self._single(intraday)
        return result.fillna(0)


@register('vwap_reversion')
class VWAPReversionStrategy(BaseStrategy):
    """
    VWAP Mean Reversion:
    – считает VWAP = cum(volume*price) / cum(volume)
    – сигнал, когда (Close/VWAP - 1) выходит за ±threshold
    """
    def __init__(self, threshold: float = 0.005, risk_target: float = 0.01):
        self.threshold = threshold
        self.risk = risk_target
        self.reasons = {}

    def _single(self, df: pd.DataFrame) -> pd.Series:
        price = df['Close']
        vol   = df['Volume']
        cum_pv = (price * vol).cumsum()
        cum_v  = vol.cumsum()
        vwap = cum_pv / cum_v
        dev = (price / vwap) - 1.0
        long_sig  = dev < -self.threshold
        short_sig = dev >  self.threshold
        pos = long_sig.astype(float) - short_sig.astype(float)
        return (pos * self.risk).fillna(0)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        for sym in df.columns.get_level_values(0).unique():
            intraday = df[sym]
            result[sym] = self._single(intraday)
        return result.fillna(0)


@register('intraday_ema_crossover')
class IntradayEMACrossoverStrategy(BaseStrategy):
    """
    Intraday EMA Crossover:
    – EMA(fast) пересекает EMA(slow)
    – позиция = sign(EMA_fast - EMA_slow)
    """
    def __init__(self, fast: int = 9, slow: int = 21, risk_target: float = 0.01):
        self.fast = fast
        self.slow = slow
        self.risk = risk_target
        self.reasons = {}

    def _ema(self, series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False).mean()

    def _single(self, df: pd.DataFrame) -> pd.Series:
        price = df['Close']
        ema_fast = self._ema(price, self.fast)
        ema_slow = self._ema(price, self.slow)
        diff = ema_fast - ema_slow
        pos = np.sign(diff).fillna(0)
        return (pos * self.risk)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        for sym in df.columns.get_level_values(0).unique():
            intraday = df[sym]
            result[sym] = self._single(intraday)
        return result.fillna(0)


@register('intraday_rsi')
class IntradayRSIStrategy(BaseStrategy):
    """
    Intraday RSI Oversold/Overbought:
    – считает RSI на intraday-кадрах
    – длинная позиция при выходе из зоны перепроданности
    – короткая при выходе из зоны перекупленности
    """
    def __init__(self, period: int = 14, oversold: int = 30, overbought: int = 70, risk_target: float = 0.01):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought
        self.risk = risk_target
        self.reasons = {}

    def _rsi(self, series: pd.Series) -> pd.Series:
        delta = series.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/self.period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/self.period, adjust=False).mean()
        rs = avg_gain / avg_loss
        return 100 - 100/(1 + rs)

    def _single(self, df: pd.DataFrame) -> pd.Series:
        series = df['Close']
        rsi = self._rsi(series)
        long_sig = (rsi.shift(1) < self.oversold) & (rsi > self.oversold)
        short_sig = (rsi.shift(1) > self.overbought) & (rsi < self.overbought)
        pos = long_sig.astype(float) - short_sig.astype(float)
        return (pos * self.risk).fillna(0)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        for sym in df.columns.get_level_values(0).unique():
            intraday = df[sym]
            result[sym] = self._single(intraday)
        return result.fillna(0)
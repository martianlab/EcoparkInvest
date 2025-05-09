import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
MACD Cross
Идея: вход, когда MACD линия пересекает сигнальную снизу вверх.
    классический индикатор тренда + импульса;
    хорошая альтернатива SMA crossover;
    можно усилить фильтрами по тренду/воле.
Параметры: EMA(12), EMA(26), signal = EMA(9)
"""
@register('macd_cross')
class MACDCrossStrategy(BaseStrategy):
    def __init__(self, short=12, long=26, signal=9, risk_target=0.01):
        self.s = short; self.l = long; self.sig = signal; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc, sym):
        ema_short = ohlc['Close'].ewm(span=self.s).mean()
        ema_long = ohlc['Close'].ewm(span=self.l).mean()
        macd = ema_short - ema_long
        signal_line = macd.ewm(span=self.sig).mean()
        cross = (macd > signal_line) & (macd.shift(1) <= signal_line.shift(1))
        if cross.sum() == 0:
            self.reasons[sym] = "no MACD cross"
        pos = cross.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df):
        self.reasons.clear(); result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)

import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Donchian Channel Breakout
Идея: Канал Дончиана рисуется по максимумам и минимумам цены за последние N периодов. 
Вход, когда цена пробивает максимум/минимум за N дней.
    базовая стратегия системы «Черепах»
    хороша на трендовых активах
    можно реализовать в long‑only варианте
    
Параметры: high.max(20), low.min(20)
"""

@register('donchian_breakout')
class DonchianBreakoutStrategy(BaseStrategy):
    def __init__(self, window=20, risk_target=0.01):
        self.win = window; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc, sym):
        high = ohlc['Close'].rolling(self.win).max().shift(1)
        signal = ohlc['Close'] > high
        if signal.sum() == 0:
            self.reasons[sym] = "no Donchian breakout"
        pos = signal.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df):
        self.reasons.clear(); result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)
import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Идея: покупать, если цена пробивает максимум последних N дней.
    Трендовая стратегия пробоя.
    Работает без наклона/тренда — просто на факт пробития.
    Подходит для рынков, где бывают «взрывы» волатильности.
Идеальна для расширения диапазонов и начала трендов.
"""

@register('breakout')
class BreakoutStrategy(BaseStrategy):
    def __init__(self, window=20, risk_target=0.01):
        self.win = window; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc: pd.DataFrame, sym: str) -> pd.Series:
        high = ohlc['Close'].rolling(self.win).max().shift(1)
        signal = ohlc['Close'] > high
        if signal.sum() == 0:
            self.reasons[sym] = "no breakout observed"
        pos = signal.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change(fill_method=None).rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.reasons.clear()
        result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)
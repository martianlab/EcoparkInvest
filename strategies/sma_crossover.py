import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Идея: классика — вход при пересечении быстрой и медленной SMA.
    - Покупка, если SMA20 пересекает вверх SMA100 (или любые другие окна).
    - Простая логика трендследящей системы.
    - Может запаздывать, но фильтрует фальшивые сигналы.
Базовая модель для системной торговли на всех рынках.
"""

@register('sma_crossover')
class SMACrossoverStrategy(BaseStrategy):
    def __init__(self, short_win=20, long_win=100, risk_target=0.01):
        self.s = short_win; self.l = long_win; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc: pd.DataFrame, sym: str) -> pd.Series:
        sma_s = ohlc['Close'].rolling(self.s).mean()
        sma_l = ohlc['Close'].rolling(self.l).mean()
        cross = (sma_s > sma_l) & (sma_s.shift(1) <= sma_l.shift(1))
        pos = cross.replace(0, np.nan).ffill().fillna(0)
        if pos.sum() == 0:
            self.reasons[sym] = "no crossover detected"
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.reasons.clear()
        result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)

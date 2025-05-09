import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Идея: покупать активы, цена которых выше своей долгосрочной SMA (например, 200 дней).
    Простейшая трендовая модель.
    Не реагирует на шум, просто игнорирует активы «ниже скользящей».
    Быстро адаптируется к рыночным изменениям.
Полезна для фильтрации трендовых активов.
"""

@register('momentum')
class MomentumStrategy(BaseStrategy):
    def __init__(self, window=200, risk_target=0.01):
        self.win = window; self.risk = risk_target
        self.reasons = {}  # тикер -> причина

    def _single(self, ohlc: pd.DataFrame, sym: str) -> pd.Series:
        sma = ohlc['Close'].rolling(self.win).mean()
        signal = ohlc['Close'] > sma
        if signal.sum() == 0:
            self.reasons[sym] = "price never above SMA"
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
import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Идея: когда волатильность сжалась (низкий STD), и цена пробивает максимум — вход на ускорение.
    - Смотрит, находится ли текущая вола на минимуме за N дней.
    - Проверяет пробой short‑range high (например, за 10 дней).
    - Это сигнал: «тихий актив начинает движение».
Полезно для раннего входа в импульсы.
"""

@register('volatility_contraction')
class VolatilityContractionStrategy(BaseStrategy):
    def __init__(self, window=60, risk_target=0.01):
        self.win = window; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc: pd.DataFrame, sym: str) -> pd.Series:
        vol = ohlc['Close'].pct_change().rolling(20).std()
        low_vol = vol == vol.rolling(self.win).min()
        breakout = ohlc['Close'] > ohlc['Close'].rolling(10).max().shift(1)
        signal = low_vol & breakout
        if signal.sum() == 0:
            self.reasons[sym] = "no volatility contraction breakout"
        pos = signal.replace(0, np.nan).ffill().fillna(0)
        vol_ann = vol * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.reasons.clear()
        result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)


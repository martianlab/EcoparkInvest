import numpy as np, pandas as pd
from core.indicators import ema, rolling_vol, slope
from strategies import BaseStrategy, register

"""
Идея: если цена пробивает максимум последних N дней, и объём выше среднего → это подтверждённый пробой.
    усиливает breakout;
    отсекает «пустые» движения;
    требует наличия Volume в данных.
"""

@register('volume_surge')
class VolumeSurgeStrategy(BaseStrategy):
    def __init__(self, window=20, risk_target=0.01):
        self.win = window; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc, sym):
        if 'Volume' not in ohlc.columns:
            self.reasons[sym] = "no Volume data"
            return pd.Series(index=ohlc.index, data=0.0)

        high = ohlc['Close'].rolling(self.win).max().shift(1)
        vol_avg = ohlc['Volume'].rolling(self.win).mean()
        signal = (ohlc['Close'] > high) & (ohlc['Volume'] > vol_avg)
        if signal.sum() == 0:
            self.reasons[sym] = "no volume-confirmed breakout"

        pos = signal.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df):
        self.reasons.clear(); result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)

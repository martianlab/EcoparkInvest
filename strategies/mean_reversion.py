import numpy as np, pandas as pd
from core.indicators import ema, rolling_vol, slope
from strategies import BaseStrategy, register

"""
Идея: покупать, если цена значительно ниже своей EMA (переоценка), при этом тренд — восходящий.
    - Использует z‑score между ценой и EMA.
    - Адаптируется к волатильности (режим: low, medium, high).
    - Требует подтверждения восходящего тренда (наклон положительный).
Хорошо работает в фазах коррекции к среднему.
"""

@register('mean_reversion')
class MeanReversionStrategy(BaseStrategy):
    def __init__(self, window=252, risk_target=0.01):
        self.win = window; self.risk = risk_target
        self.zmap = {'low': -0.8, 'medium': -1.0, 'high': -1.3}
        self.reasons = {}  # тикер -> причина

    def _single(self, ohlc: pd.DataFrame, sym: str) -> pd.Series:
        ema_ser = ema(ohlc['Close'], self.win)
        std = ohlc['Close'].rolling(self.win).std(ddof=0)
        z = (ohlc['Close'] - ema_ser) / std

        vol = rolling_vol(ohlc['Close'], 60)
        q_low, q_high = vol.quantile([0.33, 0.66])
        if not (q_low < q_high):
            regime = pd.Series('medium', index=vol.index)
        else:
            regime = pd.cut(vol, [-np.inf, q_low, q_high, np.inf], labels=['low', 'medium', 'high']).astype(str)
        entry_z = regime.map(self.zmap).astype(float)

        trend = ohlc['Close'].rolling(self.win).apply(slope, raw=True) > 0
        pos = (z < entry_z) & trend

        if pos.sum() == 0:
            self.reasons[sym] = "no entry condition met"

        pos = pos.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change(fill_method=None).rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.reasons.clear()
        result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)
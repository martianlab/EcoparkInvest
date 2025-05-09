import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Reversal After Drop (обратный откат)
Идея: если акция сильно упала за 5 дней (например, −5%), но сегодня отскакивает — это сигнал на вход.
    ловит короткие развороты;
    работает против тренда (контртренд);
    дополняет momentum/mean_reversion.
"""

@register('reversal_drop')
class ReversalAfterDropStrategy(BaseStrategy):
    def __init__(self, drop=-0.05, risk_target=0.01):
        self.drop = drop; self.risk = risk_target
        self.reasons = {}

    def _single(self, ohlc, sym):
        ret_5 = ohlc['Close'].pct_change(5)
        rebound = ohlc['Close'].pct_change() > 0
        signal = (ret_5 < self.drop) & rebound
        if signal.sum() == 0:
            self.reasons[sym] = "no 5-day drop+rebound"
        pos = signal.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df):
        self.reasons.clear(); result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)
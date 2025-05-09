import numpy as np, pandas as pd
from strategies import BaseStrategy, register

"""
Идея: если RSI < 30 и растёт — покупаем.
    ловит развороты в моменте;
    не требует тренда;
    фильтрует по «силе отскока».
Параметры: RSI‑14, вход если: RSI[t−1] < 30 и RSI[t] > RSI[t−1]
"""

@register('rsi_bounce')
class RSIBounceStrategy(BaseStrategy):
    def __init__(self, period=14, risk_target=0.01):
        self.period = period; self.risk = risk_target
        self.reasons = {}

    def _rsi(self, series):
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(self.period).mean()
        loss = -delta.clip(upper=0).rolling(self.period).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def _single(self, ohlc, sym):
        rsi = self._rsi(ohlc['Close'])
        signal = (rsi.shift(1) < 30) & (rsi > rsi.shift(1))
        if signal.sum() == 0:
            self.reasons[sym] = "no RSI bounce detected"
        pos = signal.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df):
        self.reasons.clear(); result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)
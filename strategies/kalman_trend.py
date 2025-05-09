import numpy as np, pandas as pd
from strategies import BaseStrategy, register
from pykalman import KalmanFilter

"""
Kalman Trend Filter
Идея: использовать Kalman Filter для сглаживания цены и выявления тренда.
    продвинутый метод;
    менее чувствителен к шуму;
    может служить как индикатор или фильтр.
"""

@register('kalman_trend')
class KalmanTrendStrategy(BaseStrategy):
    def __init__(self, risk_target=0.01):
        self.risk = risk_target
        self.reasons = {}

    def _smooth_kalman(self, prices: pd.Series) -> pd.Series:
        kf = KalmanFilter(initial_state_mean=prices.iloc[0], n_dim_obs=1)
        state_means, _ = kf.filter(prices.values)
        return pd.Series(state_means.flatten(), index=prices.index)

    def _single(self, ohlc: pd.DataFrame, sym: str) -> pd.Series:
        smoothed = self._smooth_kalman(ohlc['Close'])
        trend = smoothed.diff() > 0
        if trend.sum() == 0:
            self.reasons[sym] = "no upward Kalman trend"
        pos = trend.replace(0, np.nan).ffill().fillna(0)
        vol_ann = ohlc['Close'].pct_change().rolling(20).std() * np.sqrt(252)
        return self.risk / vol_ann.replace(0, np.nan) * pos

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.reasons.clear(); result = []
        for sym in df.columns.get_level_values(0).unique():
            col = self._single(df[sym], sym)
            result.append(col.rename(sym))
        return pd.concat(result, axis=1).fillna(0)
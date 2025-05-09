import numpy as np, pandas as pd
from strategies import BaseStrategy, register
from sklearn.linear_model import LinearRegression

def parse_strat_name(name):
    if ':' in name:
        base, args = name.split(':', 1)
        params = args.split(',')
        return base, params
    return name, []

@register('pair_reversion')
class PairReversionStrategy(BaseStrategy):
    def __init__(self, pair=('GAZP','LKOH'), z_entry=0.01, risk_target=0.01):
        self.pair = pair
        self.z = z_entry
        self.risk = risk_target
        self.reasons = {}

    def _single(self, df1, df2, sym1, sym2):
        px1 = df1['Close'].dropna().copy()
        px2 = df2['Close'].dropna().copy()
        df = pd.concat([px1, px2], axis=1, join='inner')
        df.columns = [sym1, sym2]

        # регрессия sym1 ~ β * sym2
        model = LinearRegression().fit(df[[sym2]], df[sym1])
        beta = model.coef_[0]
        spread = df[sym1] - beta * df[sym2]

        if len(df) < 50:
            self.reasons[f"{sym1}/{sym2}"] = "недостаточно общих точек (менее 50)"
            return pd.DataFrame(0, index=df1.index.union(df2.index), columns=[sym1, sym2])

        mu, sigma = spread.rolling(50).mean(), spread.rolling(50).std()
        zscore = (spread - mu) / sigma

        print(f"[DEBUG {sym1}/{sym2}] Z‑score range: {zscore.min():.2f} to {zscore.max():.2f}")

        long_signal = zscore < -self.z
        short_signal = zscore > self.z

        if not long_signal.any() and not short_signal.any():
            self.reasons[f"{sym1}/{sym2}"] = f"z‑score не превышает ±{self.z}"

        # формируем веса: лонг sym1/шорт sym2 или наоборот
        pos1 = long_signal.astype(float) - short_signal.astype(float)
        pos2 = -beta * pos1

        # нормировка по волатильности
        vol1 = px1.pct_change().rolling(20).std() * np.sqrt(252)
        vol2 = px2.pct_change().rolling(20).std() * np.sqrt(252)
        w1 = self.risk / vol1 * pos1
        w2 = self.risk / vol2 * pos2

        # финальный результат
        w = pd.DataFrame({sym1: w1, sym2: w2})
        return w.fillna(0)

    def generate(self, df):
        sym1, sym2 = self.pair
        if sym1 not in df.columns.get_level_values(0) or sym2 not in df.columns.get_level_values(0):
            self.reasons[f"{sym1}/{sym2}"] = "missing pair symbols"
            return pd.DataFrame(0, index=df.index, columns=[sym1, sym2])
        return self._single(df[sym1], df[sym2], sym1, sym2)
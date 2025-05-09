import numpy as np, pandas as pd

__all__ = ['ema','sma','rolling_vol','slope']

def ema(s: pd.Series, span: int):
    return s.ewm(span=span, adjust=False).mean()

def sma(s: pd.Series, w: int):
    return s.rolling(w).mean()

def rolling_vol(s: pd.Series, w=60):
    # explicit fill_method=None to silence FutureWarning
    return s.pct_change(fill_method=None).rolling(w).std()

def slope(arr):
    """Return linear‑regression slope for array/Series, ignoring NaNs."""
    y = np.asarray(arr, dtype=float)
    mask = ~np.isnan(y)
    if mask.sum() < 2:
        return np.nan
    y = y[mask]
    x = np.arange(len(y))
    return np.polyfit(x, y, 1)[0]
import numpy as np, pandas as pd

"""
При запуске с ключом --backtest скрипт:
    берёт последние 756 дней (≈3 года),
    симулирует входы/выходы c комиссией 0.05%,
    выводит:
        CAGR — среднегодовой рост капитала,
        Sharpe — доход на единицу риска,
        Max DD — максимальную просадку.
    Цифры нужны, чтобы сразу понять, лучше ли ансамбль «просто купить и держать».
"""

def run_backtest(price: pd.DataFrame, wts: pd.DataFrame, fee=0.0005):
    close = price.xs('Close', level=1, axis=1)
    pnl = (close.pct_change(fill_method=None).shift(-1) * wts).sum(axis=1)
    pnl -= fee * wts.diff().abs().sum(axis=1)
    equity = (1 + pnl.fillna(0)).cumprod()
    std = pnl.std(ddof=0)
    sharpe = np.nan if std == 0 or np.isnan(std) else pnl.mean() / std * np.sqrt(252)
    perf = {
        'CAGR': equity.iloc[-1] ** (252 / len(equity)) - 1,
        'Sharpe': sharpe,
        'MaxDD': (equity / equity.cummax() - 1).min(),
    }
    return equity, perf
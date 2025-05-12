import numpy as np
import pandas as pd

"""
При запуске с ключом --backtest скрипт:
    берёт последние 756 дней (≈3 года),
    симулирует входы/выходы c комиссией 0.05%,
    выводит:
        CAGR — среднегодовой рост капитала,
        Sharpe — доход на единицу риска,
        MaxDD — максимальную просадку,
        Trades — количество входов в позиции.
    Цифры нужны, чтобы сразу понять, лучше ли ансамбль «просто купить и держать».
"""

def run_backtest(price: pd.DataFrame, wts: pd.DataFrame, fee=0.0005):
    # извлекаем только цены закрытия
    close = price.xs('Close', level=1, axis=1)

    # PnL по портфелю: доходности * веса
    pnl = (close.pct_change(fill_method=None).shift(-1) * wts).sum(axis=1)
    # вычитаем комиссии за изменение веса
    pnl -= fee * wts.diff().abs().sum(axis=1)

    # кривая капитала
    equity = (1 + pnl.fillna(0)).cumprod()

    # основные метрики
    std = pnl.std(ddof=0)
    sharpe = np.nan if std == 0 or np.isnan(std) else pnl.mean() / std * np.sqrt(252)
    cagr = equity.iloc[-1] ** (252 / len(equity)) - 1
    max_dd = (equity / equity.cummax() - 1).min()

    # подсчёт сделок: входы из flat в позицию
    port_pos = wts.sum(axis=1)
    entry_mask = (port_pos != 0) & (port_pos.shift(1).fillna(0) == 0)
    n_trades = int(entry_mask.sum())

    perf = {
        'CAGR': cagr,
        'Sharpe': sharpe,
        'MaxDD': max_dd,
        'Trades': n_trades,
    }
    return equity, perf
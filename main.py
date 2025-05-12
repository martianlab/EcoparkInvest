"""
Tinkoff Ensemble Trading Project – Multi‑Asset Edition (v3.3)
============================================================

Пример запуска
--------------
```bash
# выводит все акции, доступные через Tinkoff API
python main.py --list` .

# делает backtest по всем рублевым тикерам
python main.py --auto-rub --backtest

# один тикер через API
python main.py AAPL --backtest

# три тикера через API
python main.py AAPL MSFT NVDA --backtest --strats mean_reversion momentum

# смешаем API + локальный CSV
python main.py AAPL ./data/SBER.csv

# парный трейдинг по GAZP и LKOH
python main.py GAZP LKOH --strats pair_reversion:GAZP,LKOH


# ==========================================================================
# Стратегии: типы и взаимодействие
#
# | Стратегия               | Тип             | Что делает                                                                      | Дополняет                        |
# |-------------------------|------------------|----------------------------------------------------------------------------------|----------------------------------|
# | mean_reversion          | Контртренд       | Покупка при отклонении от EMA вниз, если тренд восходящий                       | momentum, breakout               |
# | momentum                | Трендовая        | Покупка, если цена выше SMA                                                     | mean_reversion, breakout         |
# | breakout                | Импульс/тренд    | Вход при пробое максимума за N дней                                             | mean_reversion                   |
# | volatility_contraction  | Импульс          | Вход после фазы низкой волатильности и пробоя вверх                             | breakout, kalman_trend           |
# | sma_crossover           | Трендовая        | Вход при пересечении быстрой SMA и медленной                                   | mean_reversion, momentum         |
# | rsi_bounce              | Импульс/разворот | Вход при RSI < 30 и его росте (разворот от перепроданности)                    | mean_reversion, reversal_drop    |
# | macd_cross              | Трендовая        | Вход по пересечению MACD и сигнальной линии                                     | sma_crossover, kalman_trend      |
# | donchian_breakout       | Трендовая        | Вход при пробое канала Дончиана (макс за N дней)                                | breakout, momentum               |
# | reversal_drop           | Контртренд       | Вход после сильного падения и разворота (rebound)                               | rsi_bounce, mean_reversion       |
# | volume_surge            | Импульс/подтв.   | Вход при пробое high и росте объема выше среднего                               | breakout, donchian_breakout      |
# | kalman_trend            | Тренд-фильтр     | Вход при положительном наклоне сглаженного тренда (Kalman filter)              | momentum, macd_cross, breakout   |
# | pair_reversion          | Рыночно-нейтральн| Шорт переоценённой и лонг недооценённой акции при расхождении спреда            | mean_reversion, rsi_bounce       |
#
# ==========================================================================

"""

import argparse, pandas as pd
from core.data_loader import load_prices
from portfolio_manager import PortfolioManager
from core.backtester import run_backtest
from core.universe import get_share_universe, UniverseError
from core.dividend_loader import get_dividends
from itertools import combinations


def build_price_dataframe(symbols: list[str], interval: int = None, days: int = 750) -> pd.DataFrame:
    frames = []
    for sym in symbols:
        try:
            df = load_prices(sym, interval=interval, days=days)[["Open", "High", "Low", "Close", "Volume"]].copy()
        except SystemExit as e:
            print(f"[WARN] {sym}: {e}")
            continue
        if df.empty:
            print(f"[WARN] {sym}: no data, skipped")
            continue
        df.columns = pd.MultiIndex.from_product([[sym], df.columns])
        frames.append(df)
    if not frames:
        raise SystemExit("No valid symbols with data")
    return pd.concat(frames, axis=1).sort_index()


def cli():
    p = argparse.ArgumentParser("Ensemble Trading CLI – multi‑asset")
    p.add_argument("symbols", nargs="*", help="Tickers/FIGIs or CSV paths (omit if --list)")
    p.add_argument("--strats", nargs="*", default=["mean_reversion"],
                   help="Strategy names")
    p.add_argument("--backtest", action="store_true", help="Run 3‑year back‑test")
    p.add_argument("--list", action="store_true",
                   help="Show available share tickers via Tinkoff API and exit")
    p.add_argument("--refresh", action="store_true",
                   help="Force refresh of ticker list (skip cache)")
    p.add_argument("--auto-rub", action="store_true",
                   help="Use all RUB-denominated tickers from Tinkoff API")
    p.add_argument("--auto-hkd", action="store_true",
                   help = "Use all HKD-denominated tickers from Tinkoff API")
    p.add_argument("--divs", action="store_true", help="Show dividend history from MOEX ISS API")
    p.add_argument("--list-pairs", action="store_true",
                   help="List symbol pairs with >= min common data points and exit")
    p.add_argument("--min-len", type=int, default=50,
                   help="Minimum number of common data points for pairs")
    p.add_argument("--intraday", action="store_true", help="Use intraday data")
    p.add_argument("--interval", type=int, choices=[1, 5, 15, 30], default=5,
                   help="Bar interval in minutes for intraday")
    p.add_argument("--per-symbol", action="store_true",
                   help = "Run back‑test independently for every symbol and show table")

    args = p.parse_args()

    load_kwargs = {}
    if args.intraday:
        load_kwargs['interval'] = args.interval
        load_kwargs['days'] = 5

    if args.divs:
        for sym in args.symbols:
            print(f"===== {sym} dividends =====")
            try:
                df = get_dividends(sym)
                print(df[["registryclosedate", "value", "currencyid"]].to_string(index=False))
            except Exception as e:
                print(f"{sym}: {e}")

    if args.list:
        try:
            refresh = 0 if not args.refresh else -1
            uni = get_share_universe(refresh_hours=refresh)
            cols = ["ticker", "figi", "currency", "class", "name", "last"]
            print(uni[cols].to_string(index=False))
        except UniverseError as e:
            print(e)
        return

    if args.auto_rub:
        try:
            refresh = 0 if not args.refresh else -1
            uni = get_share_universe(refresh_hours=refresh)
            rub_tickers = uni.query("currency == 'rub'")['ticker'].unique().tolist()
            args.symbols = rub_tickers
            print(f"[INFO] Loaded {len(rub_tickers)} RUB‑denominated tickers")
        except UniverseError as e:
            print(e)
            return

    if args.auto_hkd:
        try:
            refresh = 0 if not args.refresh else -1
            uni = get_share_universe(refresh_hours=refresh)
            hkd_tickers = uni.query("currency == 'hkd'")['ticker'].unique().tolist()
            args.symbols = hkd_tickers
            print(f"[INFO] Loaded {len(hkd_tickers)} hkd‑denominated tickers")
        except UniverseError as e:
            print(e)
            return

    if args.list_pairs:
        if len(args.symbols) < 2:
            p.error("--list-pairs requires at least two symbols")
        price_df = build_price_dataframe(args.symbols, **load_kwargs)
        close = price_df.xs('Close', axis=1, level=1)
        print(f"Pairs with >= {args.min_len} common data points:")
        for sym1, sym2 in combinations(close.columns, 2):
            sub = close[[sym1, sym2]].dropna()
            if len(sub) >= args.min_len:
                print(f"  {sym1}/{sym2} – {len(sub)} points")
        return

    # -------------------------------------------------
    #  Индивидуальный бэктест «по каждому тикеру»
    # -------------------------------------------------
    if args.per_symbol:
        rows = []
        for sym in args.symbols:
            # 1) Загружаем только этот тикер
            try:
                df_sym = build_price_dataframe([sym], **load_kwargs)
            except SystemExit as e:
                print(f"[WARN] {sym}: {e}")
                continue

            # 2) Считаем веса стратегий
            wts_sym = PortfolioManager(args.strats).generate_weights(df_sym)

            # 3) Берём последние 756 дней
            df_bt  = df_sym.tail(365)
            w_bt   = wts_sym.loc[df_bt.index]

            # 4) Бэктест и сбор метрик
            _, perf = run_backtest(df_bt, w_bt)
            rows.append([sym,
                         f"{perf['CAGR']:.2%}",
                         f"{perf['Sharpe']:.2f}",
                         f"{perf['MaxDD']:.2%}",
                         perf['Trades']])

        # 5) Вывод таблицы
        table = pd.DataFrame(rows,
                             columns=["Ticker", "CAGR", "Sharpe", "MaxDD", "Trades"])
        print("\nPer‑symbol back‑test (1 year, 365 d):")
        print(table.to_string(index=False))
        return  # не продолжаем дальше

    if not args.symbols:
        p.error("symbols required unless --list is used")

    price_df = build_price_dataframe(args.symbols, **load_kwargs)
    pm = PortfolioManager(args.strats)
    weights = pm.generate_weights(price_df)

    print("Current portfolio weights:")
    last = weights.iloc[-1]
    for sym, wt in last.items():
        if wt != 0:
            print(f"  {sym:<6}: {wt:+.3f}")
    if last.abs().sum() == 0:
        print("  FLAT / no positions")
    for strat in pm.strategies:
        if hasattr(strat, "reasons") and strat.reasons:
            print(f"  [{strat.name}] diagnostics:")
            for sym, reason in strat.reasons.items():
                print(f"    {sym:<6} – {reason}")

    if args.backtest:
        df_bt = price_df.tail(756)
        w_bt = weights.loc[df_bt.index]
        _, perf = run_backtest(df_bt, w_bt)
        print("\nBack‑test 3 years:")
        print(f"  CAGR (среднегодовой рост капитала) : {perf['CAGR']:.2%}")
        print(f"  Sharpe (доход на единицу риска): {perf['Sharpe']:.2f}")
        print(f"  MaxDD (максимальная просадка): {perf['MaxDD']:.2%}")
        print(f"  Trades (количество сделок) : {perf['Trades']}")

if __name__ == "__main__":
    cli()
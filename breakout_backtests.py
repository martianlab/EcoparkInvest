import os
import math
import time
import random
from datetime import datetime, timedelta

import pandas as pd
from tinkoff.invest import Client, CandleInterval, InstrumentStatus
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode

# ─── ПАРАМЕТРЫ ─────────────────────────────────────────────────────────────────
INTERVAL_BT   = CandleInterval.CANDLE_INTERVAL_1_MIN
DAYS_BACK     = 30

CAPITAL_START = 50_000
COMMISSION    = 0.0004
RISK_PCT      = 0.02
TP_GRID       = [0.005, 0.01, 0.015]
SL_GRID       = [0.003, 0.005, 0.01]
DELTA_GRID    = [0.001, 0.002, 0.003]
LOOKBACK_GRID = [10, 20, 30]

# ─── ТОКЕН ────────────────────────────────────────────────────────────────────
TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")


def fetch_all_ruble_tickers(client: Client) -> list[str]:
    """
    Возвращает список тикеров всех доступных рублёвых акций на MOEX.
    Фильтрация по:
      - instrument_status = BASE
      - api_trade_available_flag = True
      - currency = "RUB"
    """
    tickers = []
    for inst in client.instruments.shares(
            instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
    ).instruments:
        if inst.api_trade_available_flag and inst.currency == "rub":
            tickers.append(inst.ticker)
    return tickers


def resolve_figi(ticker: str, client: Client) -> str | None:
    for inst in client.instruments.shares(
            instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
    ).instruments:
        if (inst.ticker.upper() == ticker.upper()
                and inst.api_trade_available_flag
                and inst.currency == "rub"):
            return inst.figi
    return None


def fetch_candles(figi: str, interval, days: int) -> pd.DataFrame:
    """Загружает минутные свечи за последние days дней, возвращает DataFrame с часовым поясом Moscow."""
    since = now() - timedelta(days=days)
    rows = []
    with Client(TOKEN_INVEST) as cl:
        def safe_gen():
            backoff, left = 1, 5
            while True:
                try:
                    yield from cl.get_all_candles(figi=figi, from_=since, interval=interval)
                    break
                except RequestError as e:
                    status, _, meta = e.args
                    if status == StatusCode.RESOURCE_EXHAUSTED:
                        time.sleep(max(int(meta.ratelimit_reset), 1) + 1)
                    elif status == StatusCode.UNAVAILABLE and left:
                        time.sleep(backoff + random.random() * 0.5)
                        backoff *= 2
                        left -= 1
                    else:
                        raise

        for c in safe_gen():
            rows.append({
                "time":  pd.to_datetime(c.time),
                "high":  c.high.units + c.high.nano  / 1e9,
                "close": c.close.units + c.close.nano / 1e9,
                "vol":   c.volume
            })

    df = pd.DataFrame(rows).set_index("time").sort_index()
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    return df.tz_convert("Europe/Moscow")


def backtest(df: pd.DataFrame, lookback: int, delta: float, tp: float, sl: float) -> tuple[float,int,int,int]:
    """Запускаем одиночный бэктест, возвращаем (PnL %, trades, wins, losses)."""
    df = df.copy()
    df["hi_lvl"]  = df["high"].rolling(lookback).max().shift(1)
    df["vol_ma"] = df["vol"].rolling(lookback).mean().shift(1)

    cap        = CAPITAL_START
    pos_qty    = 0
    entry_px   = 0.0
    entry_val  = 0.0
    trades = wins = losses = 0

    for _, row in df.iterrows():
        # выход по TP/SL
        if pos_qty:
            change = row.close / entry_px - 1
            if change >= tp or change <= -sl:
                pnl = (row.close * pos_qty - entry_val) - row.close * pos_qty * COMMISSION
                cap += pnl
                wins   += change >= tp
                losses += change <= -sl
                trades += 1
                pos_qty = 0

        # вход по пробою с объёмом
        if pos_qty == 0 and row.close > row.hi_lvl and (row.close - row.hi_lvl) / row.hi_lvl >= delta:
            if row.vol > row.vol_ma:
                risk_cash = cap * RISK_PCT
                qty = min(
                    math.floor(risk_cash / (row.close * sl)),
                    math.floor(cap / row.close)
                )
                if qty > 0:
                    entry_px  = row.close
                    entry_val = qty * row.close
                    cap      -= entry_val * COMMISSION
                    pos_qty   = qty

    pnl_pct = (cap / CAPITAL_START - 1) * 100
    return pnl_pct, trades, wins, losses


def main():
    results = []

    with Client(TOKEN_INVEST) as client:
        tickers = fetch_all_ruble_tickers(client)
        print(f"Найдено {len(tickers)} рублёвых тикеров для анализа.")

    for ticker in tickers:
        try:
            with Client(TOKEN_INVEST) as client:
                figi = resolve_figi(ticker, client)
            if not figi:
                print(f"❌ {ticker}: FIGI не найден, пропускаем.")
                continue

            df = fetch_candles(figi, INTERVAL_BT, DAYS_BACK)

            best_ret = float("-inf")
            best_params = ()
            best_trades = best_wins = best_losses = 0

            for lb in LOOKBACK_GRID:
                for d in DELTA_GRID:
                    for tp in TP_GRID:
                        for sl in SL_GRID:
                            ret, trades, wins, losses = backtest(df, lb, d, tp, sl)
                            if ret > best_ret:
                                best_ret, best_params = ret, (lb, d, tp, sl)
                                best_trades, best_wins, best_losses = trades, wins, losses

            print(f"✅ {ticker}: PnL={best_ret:.2f}%, params={best_params}, "
                  f"trades={best_trades}, wins={best_wins}, losses={best_losses}")

            results.append({
                "ticker":   ticker,
                "pnl_pct":  best_ret,
                "lookback": best_params[0],
                "delta":    best_params[1],
                "tp":       best_params[2],
                "sl":       best_params[3],
                "trades":   best_trades,
                "wins":     best_wins,
                "losses":   best_losses,
            })

            time.sleep(0.5)  # не превышаем rate‐limit

        except Exception as e:
            print(f"❌ {ticker}: ошибка {e}, пропускаем.")

    # Сохраняем результаты
    df_res = pd.DataFrame(results)
    filename = f"backtest_results_{datetime.now():%Y%m%d_%H%M%S}.csv"
    df_res.to_csv(filename, index=False)
    print(f"Готово! Результаты сохранены в {filename}")


if __name__ == "__main__":
    main()

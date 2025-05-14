"""
Этот скрипт реализует стратегию Volume Spike Reversal (VSR) — внутридневную торговую стратегию,
основанную на развороте после резкого всплеска объёма и появления характерной свечной формации
(длинный хвост, малое тело).

🧠 Суть стратегии:
• Стратегия реагирует на свечи с аномально высоким объёмом (в 2.5 раза выше среднего).
• Дополнительное условие — форма свечи: длинный нижний хвост и малое тело (покупка на разворот).
• Вход происходит на следующей минутной свече.
• Выход — по тейк-профиту (TP) или стоп-лоссу (SL), которые подбираются из сетки значений.

⚙️ Что делает скрипт:
• Загружает исторические минутные свечи за 30 дней по всем ликвидным тикерам выбранной валюты.
• Ищет точки входа по стратегии Volume Spike Reversal.
• Подбирает лучшие параметры TP/SL для каждого тикера (сеточный перебор).
• Сохраняет результат в CSV.
• При наличии Telegram-токена отправляет таблицу в Telegram.

Запуск:
  python volume_spike_reversal_backtests.py --currency rub
"""

import os
import math
import time
import random
import argparse
import requests
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
VOLUME_MULTIPLIER = 2.5

TP_GRID = [0.005, 0.01, 0.015]
SL_GRID = [0.003, 0.005, 0.01]

TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")
TG_TOKEN     = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")


def fetch_all_tickers_by_currency(client: Client, currency: str) -> list[str]:
    return [
        inst.ticker for inst in client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments
        if inst.api_trade_available_flag and inst.currency.lower() == currency.lower()
    ]


def resolve_figi(ticker: str, client: Client, currency: str) -> str | None:
    for inst in client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments:
        if inst.ticker.upper() == ticker.upper() and inst.api_trade_available_flag and inst.currency.lower() == currency.lower():
            return inst.figi
    return None


def fetch_candles(figi: str, interval, days: int) -> pd.DataFrame:
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
                "low":   c.low.units + c.low.nano / 1e9,
                "open":  c.open.units + c.open.nano / 1e9,
                "close": c.close.units + c.close.nano / 1e9,
                "vol":   c.volume
            })

    df = pd.DataFrame(rows).set_index("time").sort_index()
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    return df.tz_convert("Europe/Moscow")


def backtest_volume_spike(df: pd.DataFrame, tp: float, sl: float) -> tuple[float, int, int, int]:
    df = df.copy()
    df["vol_ma"] = df["vol"].rolling(20).mean().shift(1)

    cap = CAPITAL_START
    pos_qty = entry_px = entry_val = 0.0
    trades = wins = losses = 0

    for i in range(1, len(df)):
        r = df.iloc[i]
        prev = df.iloc[i - 1]

        tail_size = prev.close - prev.low
        body_size = abs(prev.close - prev.open)
        if prev.vol > prev.vol_ma * VOLUME_MULTIPLIER and tail_size > body_size * 1.2:
            risk_cash = cap * RISK_PCT
            qty = math.floor(risk_cash / (r.close * sl))
            if qty > 0:
                entry_px = r.close
                entry_val = qty * r.close
                cap -= entry_val * COMMISSION
                pos_qty = qty

        if pos_qty:
            change = r.close / entry_px - 1
            if change >= tp or change <= -sl:
                pnl = (r.close * pos_qty - entry_val) - r.close * pos_qty * COMMISSION
                cap += pnl
                wins += change >= tp
                losses += change <= -sl
                trades += 1
                pos_qty = 0

    pnl_pct = (cap / CAPITAL_START - 1) * 100
    return pnl_pct, trades, wins, losses


def send_file_to_telegram(filepath: str, caption: str = ""):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("❌ TG_BOT_TOKEN или TG_CHAT_ID не заданы.")
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendDocument"
    with open(filepath, "rb") as file:
        response = requests.post(url, data={"chat_id": TG_CHAT_ID, "caption": caption}, files={"document": file})
    if response.status_code == 200:
        print("📤 Результаты отправлены в Telegram.")
    else:
        print(f"❌ Ошибка при отправке в Telegram: {response.text}")


def main():
    parser = argparse.ArgumentParser(description="Backtest volume spike reversal strategy.")
    parser.add_argument("--currency", default="rub", help="Валюта тикеров (по умолчанию: rub)")
    args = parser.parse_args()
    currency = args.currency.lower()

    results = []

    with Client(TOKEN_INVEST) as client:
        tickers = fetch_all_tickers_by_currency(client, currency)
        print(f"Найдено {len(tickers)} тикеров с валютой {currency.upper()} для анализа.")

    for ticker in tickers:
        try:
            with Client(TOKEN_INVEST) as client:
                figi = resolve_figi(ticker, client, currency)
            if not figi:
                print(f"❌ {ticker}: FIGI не найден, пропускаем.")
                continue

            df = fetch_candles(figi, INTERVAL_BT, DAYS_BACK)

            best_ret = float("-inf")
            best_tp = best_sl = 0
            best_trades = best_wins = best_losses = 0

            for tp in TP_GRID:
                for sl in SL_GRID:
                    ret, trades, wins, losses = backtest_volume_spike(df, tp, sl)
                    if ret > best_ret:
                        best_ret = ret
                        best_tp = tp
                        best_sl = sl
                        best_trades = trades
                        best_wins = wins
                        best_losses = losses

            print(f"✅ {ticker}: PnL={best_ret:.2f}%, tp={best_tp}, sl={best_sl}, "
                  f"trades={best_trades}, wins={best_wins}, losses={best_losses}")

            results.append({
                "ticker":  ticker,
                "pnl_pct": best_ret,
                "tp":      best_tp,
                "sl":      best_sl,
                "trades":  best_trades,
                "wins":    best_wins,
                "losses":  best_losses,
            })

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ {ticker}: ошибка {e}, пропускаем.")

    df_res = pd.DataFrame(results)
    df_res = df_res.sort_values(by="pnl_pct", ascending=False)
    filename = f"spike_results_{currency.upper()}_{datetime.now():%Y%m%d_%H%M%S}.csv"
    df_res.to_csv(filename, index=False)
    print(f"Готово! Результаты сохранены в {filename}")
    send_file_to_telegram(filename, caption=f"📈 Volume Spike Reversal: {currency.upper()}")

if __name__ == "__main__":
    main()

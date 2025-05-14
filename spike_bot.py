import os
import math
import time
import random
import argparse
import requests
from datetime import datetime, timedelta

import pandas as pd
from tinkoff.invest import Client, CandleInterval, InstrumentStatus, OrderDirection, OrderType
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode

# ─── ПАРАМЕТРЫ ─────────────────────────────────────────────
TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")

CANDLE_INTERVAL = CandleInterval.CANDLE_INTERVAL_1_MIN
TP_GRID = [0.005, 0.01, 0.015]
SL_GRID = [0.003, 0.005, 0.01]
VOLUME_MULTIPLIER = 2.5
RISK_PCT = 0.02
COMMISSION = 0.0004
CAPITAL_START = 50_000

# ─── УТИЛИТЫ ───────────────────────────────────────────────

def _qfloat(q):
    return q.units + q.nano / 1e9

# ─── TELEGRAM ──────────────────────────────────────────────

def tg_send(msg: str, prefix: str = ""):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("❌ Telegram переменные окружения не заданы.")
        return
    prefix_tag = f"[{prefix.upper()}] " if prefix else ""
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": prefix_tag + msg,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, data=payload)
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")

# ─── ПОЛУЧЕНИЕ ДАННЫХ ──────────────────────────────────────

def resolve_figi(client, ticker, currency="rub"):
    for inst in client.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments:
        if inst.ticker.upper() == ticker.upper() and inst.api_trade_available_flag and inst.currency.lower() == currency:
            return inst.figi
    return None

def fetch_candles(figi: str, interval: CandleInterval, days: int) -> pd.DataFrame:
    since = now() - timedelta(days=days)
    rows: list[dict] = []

    def _gen():
        backoff, retries = 1, 5
        while True:
            try:
                with Client(TOKEN_INVEST) as cl:
                    yield from cl.get_all_candles(figi=figi, from_=since, interval=interval)
                break
            except RequestError as e:
                status, *_ = e.args
                if status == StatusCode.RESOURCE_EXHAUSTED:
                    reset = int(e.metadata.ratelimit_reset) if hasattr(e, "metadata") else 1
                    time.sleep(reset + 1)
                elif status == StatusCode.UNAVAILABLE and retries > 0:
                    time.sleep(backoff + random.random())
                    backoff *= 2
                    retries -= 1
                else:
                    raise

    for c in _gen():
        rows.append({
            "time": pd.to_datetime(c.time).tz_convert("Europe/Moscow"),
            "open": _qfloat(c.open),
            "high": _qfloat(c.high),
            "low": _qfloat(c.low),
            "close": _qfloat(c.close),
            "vol": c.volume,
        })
    df = pd.DataFrame(rows).set_index("time").sort_index()
    return df

def fetch_recent_candles(client, figi, interval=CANDLE_INTERVAL, lookback=2):
    to_ = now()
    from_ = to_ - timedelta(minutes=lookback)
    candles = client.market_data.get_candles(figi=figi, from_=from_, to=to_, interval=interval).candles
    return pd.DataFrame([{
        "time": pd.to_datetime(c.time).tz_convert("Europe/Moscow"),
        "open": _qfloat(c.open),
        "high": _qfloat(c.high),
        "low": _qfloat(c.low),
        "close": _qfloat(c.close),
        "vol": c.volume
    } for c in candles])

# ─── БЭКТЕСТ ────────────────────────────────────────────────

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

def find_best_tp_sl(df: pd.DataFrame, ticker: str) -> tuple[float, float]:
    best_ret = float("-inf")
    best_tp = best_sl = 0
    for tp in TP_GRID:
        for sl in SL_GRID:
            ret, *_ = backtest_volume_spike(df, tp, sl)
            if ret > best_ret:
                best_ret = ret
                best_tp = tp
                best_sl = sl
    tg_send(
        f"📊 <b>Backtest завершён</b>\nTP: <code>{best_tp}</code>, SL: <code>{best_sl}</code>\nДоходность: <b>{best_ret:.2f}%</b>",
        prefix=ticker
    )
    return best_tp, best_sl

# ─── ОРДЕРА ─────────────────────────────────────────────────

def place_order(client, figi, qty, direction):
    account_id = client.users.get_accounts().accounts[0].id
    order = client.orders.post_order(
        figi=figi,
        quantity=qty,
        direction=direction,
        account_id=account_id,
        order_type=OrderType.ORDER_TYPE_MARKET,
        order_id=str(datetime.utcnow().timestamp())
    )
    print(f"📤 Ордер: {direction.name}, qty={qty}, цена={_qfloat(order.executed_order_price)}")

# ─── ОСНОВНОЙ ЦИКЛ ─────────────────────────────────────────

def live_trade(ticker: str, live: bool, days_back: int):
    mode = 'LIVE' if live else 'СИМУЛЯЦИЯ'
    tg_send(f"🤖 <b>VSR бот запущен</b>\nРежим: <b>{mode}</b>\nДней истории: <b>{days_back}</b>", prefix=ticker)

    with Client(TOKEN_INVEST) as client:
        figi = resolve_figi(client, ticker)
        if not figi:
            tg_send("❌ FIGI не найден.", prefix=ticker)
            return

        df_hist = fetch_candles(figi, interval=CANDLE_INTERVAL, days=days_back)
        if df_hist.empty:
            tg_send("❌ История свечей пуста.", prefix=ticker)
            return

        tp, sl = find_best_tp_sl(df_hist, ticker)

        cap = CAPITAL_START
        pos_qty = 0
        entry_px = 0

        candles = df_hist.copy()
        candles["vol_ma"] = candles["vol"].rolling(20).mean()

        print("⏳ Ожидание новых свечей...")
        while True:
            time.sleep(60)
            new_candle = fetch_recent_candles(client, figi, lookback=2)
            if len(new_candle) < 2:
                continue

            prev, curr = new_candle.iloc[-2], new_candle.iloc[-1]
            vol_ma = candles["vol"].iloc[-20:].mean()
            tail_size = prev.close - prev.low
            body_size = abs(prev.close - prev.open)

            if pos_qty == 0 and prev.vol > vol_ma * VOLUME_MULTIPLIER and tail_size > body_size * 1.2:
                risk_cash = cap * RISK_PCT
                qty = math.floor(risk_cash / (curr.close * sl))
                if qty > 0:
                    entry_px = curr.close
                    pos_qty = qty
                    tg_send(
                        f"📈 <b>Вход</b>\nЦена: <code>{entry_px:.2f}</code>\nTP: <code>{tp}</code> | SL: <code>{sl}</code>\nQty: {qty}",
                        prefix=ticker
                    )
                    if live:
                        place_order(client, figi, qty, OrderDirection.ORDER_DIRECTION_BUY)

            elif pos_qty > 0:
                change = curr.close / entry_px - 1
                if change >= tp or change <= -sl:
                    pnl = (curr.close * pos_qty - entry_px * pos_qty) - curr.close * pos_qty * COMMISSION
                    cap += pnl
                    tg_send(
                        f"💰 <b>Выход</b>\nЦена: <code>{curr.close:.2f}</code>\nΔ: {change:.4f}\nPnL: {pnl:.2f}\nКапитал: {cap:.2f}",
                        prefix=ticker
                    )
                    if live:
                        place_order(client, figi, pos_qty, OrderDirection.ORDER_DIRECTION_SELL)
                    pos_qty = 0

            candles = pd.concat([candles, curr.to_frame().T]).iloc[-60:]
            candles["vol_ma"] = candles["vol"].rolling(20).mean()

# ─── ЗАПУСК ─────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Volume Spike Reversal bot")
    parser.add_argument("ticker", help="Тикер для торговли (например: SBER)")
    parser.add_argument("--live", action="store_true", help="Флаг реальной торговли")
    parser.add_argument("--days-back", type=int, default=1, help="Количество дней для бэктеста (по умолчанию: 1)")
    args = parser.parse_args()

    live_trade(args.ticker.upper(), args.live, args.days_back)

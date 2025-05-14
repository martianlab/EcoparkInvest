'''
"""
Breakout trading bot for Tinkoff Invest — стратегия объёмного пробоя (Volume Breakout).

📈 Описание стратегии:
Бот торгует по стратегии пробоя на объёмах на минутных свечах.
Цель — покупать актив, когда цена пробивает локальный максимум на повышенных объёмах,
и выходить из позиции по тейк-профиту (TP) или стоп-лоссу (SL).

🔍 Условия входа:
• Цена закрытия текущей свечи выше локального максимума за `lookback` свечей (`hi_lvl`)
• Пробой значимый: `(close - hi_lvl) / hi_lvl >= delta`
• Объём текущей свечи превышает средний за `lookback`: `vol > vol_ma`
• Нет открытой позиции

⚙️ Расчёт позиции:
• Риск на сделку: `RISK_PCT` от капитала
• Кол-во лотов подбирается так, чтобы потери по SL не превышали допустимый риск
• Покупка по рыночной цене на рассчитанное количество лотов

💰 Выход из позиции:
• TP: цена выросла на `tp`, либо
• SL: цена упала на `sl`
• Закрытие позиции, учёт комиссии, обновление капитала

🧠 Оптимизация (раз в день):
• Перед началом торгового дня подбираются лучшие параметры (`lookback`, `delta`, `tp`, `sl`)
  по историческим данным за `DAYS_BACK` дней.
• Подбор идёт по сетке параметров:
  - `lookback`: 10, 20, 30 минут
  - `delta`: 0.1%, 0.2%, 0.3%
  - `tp`: 0.5%, 1%, 1.5%
  - `sl`: 0.3%, 0.5%, 1%
• Целевая метрика: максимальный PnL

📊 Настройки по умолчанию:
• Начальный капитал: 50,000
• Риск на сделку: 2%
• Комиссия на сделку: 0.04%
• История для анализа: по умолчанию 30 дней (можно задать через аргумент)

🔄 Частота анализа:
• Каждую минуту — проверка новой закрытой свечи, обновление сигналов
• Раз в день (по Europe/Moscow) — новая оптимизация стратегии

📦 Тип торговли:
• По умолчанию: сигнальный режим (Signal-only) — бот отправляет сигналы в Telegram
• С флагом `--live`: торговля реальными рыночными ордерами через API

📬 Telegram:
• Все события (запуск, параметры, сигналы, сделки, ошибки) отправляются в Telegram
• Сообщения префиксуются тикером: *TICKER*

• **Один back‑test в день** – параметры подбираются ровно один раз на
  старте и затем при наступлении нового торгового дня (по дате
  Europe/Moscow).  Между оптимизациями генерируется только торговая
  логика по закрытым минутным свечам.
• **Signal‑mode** (только уведомления) по умолчанию, включить real‑orders
  можно через константу `LIVE_TRADING`.
• Все события (старт, параметры, сигналы, PnL, ошибки) летят в Telegram.

Переменные окружения: `TINKOFF_TOKEN, TELEGRAM_BOT_TOKEN,
TELEGRAM_CHAT_ID, BOT_TICKER (опц.), BOT_DAYS_BACK (опц.).

Запуск:
   python script.py GAZP --days-back 30 --live
'''

from __future__ import annotations

import os
import sys
import math
import time
import random
import signal
import logging
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
import argparse

import requests
import pandas as pd
from grpc import StatusCode
from tinkoff.invest import (
    Client,
    CandleInterval,
    InstrumentStatus,
    OrderDirection,
    OrderType,
    Quotation,
)
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError

# ────────────── Config ─────────────────────────────────────────────────── #
parser = argparse.ArgumentParser(description="Breakout trading bot for Tinkoff Invest")
parser.add_argument("ticker", nargs="?", default=os.getenv("BOT_TICKER", "VTBR"),
                    help="Ticker to trade (default from BOT_TICKER env or VTBR)")
parser.add_argument("--days-back", type=int, default=int(os.getenv("BOT_DAYS_BACK", "30")),
                    help="How many days of history to fetch for backtest")
parser.add_argument("--live", action="store_true", help="Enable real orders (LIVE_TRADING)")
args = parser.parse_args()

TICKER = args.ticker.upper()
DAYS_BACK = args.days_back
INTERVAL = CandleInterval.CANDLE_INTERVAL_1_MIN

CAPITAL_START = 50_000.0
COMMISSION = 0.0004
RISK_PCT = 0.02

TP_GRID = [0.005, 0.01, 0.015]
SL_GRID = [0.003, 0.005, 0.01]
DELTA_GRID = [0.001, 0.002, 0.003]
LOOKBACK_GRID = [10, 20, 30]

LIVE_TRADING = args.live

TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")
TG_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

# ────────────────────────────── Logging ──────────────────────────────────── #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("breakout_bot")

# ─────────────────────────── Telegram helper ────────────────────────────── #
def tg_send(text: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        log.debug("TG not configured: %s", text)
        return
    # Prefix message with ticker
    text = f"*{TICKER}* {text}"
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": TG_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            log.error("Telegram error %s: %s", resp.status_code, resp.text)
    except Exception as exc:
        log.exception("Telegram send failed: %s", exc)

# ─────────────────────────── Tinkoff helpers ────────────────────────────── #
def _qfloat(q: Quotation) -> float:
    return q.units + q.nano / 1e9

def resolve_figi(ticker: str) -> str | None:
    with Client(TOKEN_INVEST) as c:
        for inst in c.instruments.shares(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments:
            if inst.ticker == ticker and inst.api_trade_available_flag:
                return inst.figi
    return None

if TOKEN_INVEST is None:
    raise SystemExit("TINKOFF_TOKEN not set")
FIGI = resolve_figi(TICKER)
if not FIGI:
    raise SystemExit(f"Cannot resolve FIGI for {TICKER}")
log.info("Resolved FIGI %s for ticker %s", FIGI, TICKER)

# ──────────────────────────── Market data ───────────────────────────────── #
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

def fetch_latest_candle(figi: str) -> dict | None:
    to_time = now()
    from_time = to_time - timedelta(minutes=3)
    with Client(TOKEN_INVEST) as cl:
        cs = cl.market_data.get_candles(figi=figi, from_=from_time, to=to_time, interval=INTERVAL).candles
    if not cs:
        return None
    c = cs[-1]
    closed = pd.to_datetime(c.time).tz_convert("Europe/Moscow")
    if closed >= datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Moscow")):
        return None
    return {
        "time": closed,
        "open": _qfloat(c.open),
        "high": _qfloat(c.high),
        "low": _qfloat(c.low),
        "close": _qfloat(c.close),
        "vol": c.volume,
    }

# ────────────────────────── Strategy back-test ──────────────────────────── #
def backtest(df: pd.DataFrame, lookback: int, delta: float, tp: float, sl: float) -> tuple[float,int,int,int]:
    df = df.copy()
    df["hi_lvl"] = df["high"].rolling(lookback).max().shift(1)
    df["vol_ma"] = df["vol"].rolling(lookback).mean().shift(1)
    cap = CAPITAL_START
    pos_qty = entry_px = entry_val = 0.0
    trades = wins = losses = 0
    for r in df.itertuples():
        if pos_qty:
            change = r.close / entry_px - 1
            if change >= tp or change <= -sl:
                cap += (r.close*pos_qty - entry_val) - r.close*pos_qty*COMMISSION
                wins += change >= tp
                losses += change <= -sl
                trades += 1
                pos_qty = 0
        if not pos_qty and r.close > r.hi_lvl and (r.close - r.hi_lvl)/r.hi_lvl >= delta and r.vol > r.vol_ma:
            risk = cap * RISK_PCT
            qty = min(math.floor(risk/(r.close*sl)), math.floor(cap/r.close))
            if qty > 0:
                pos_qty = qty
                entry_px = r.close
                entry_val = qty * r.close
                cap -= entry_val * COMMISSION
    return (cap/CAPITAL_START - 1)*100, trades, wins, losses

def optimize_params(df: pd.DataFrame) -> dict:
    best = {"ret": -1e9}
    for lb in LOOKBACK_GRID:
        for d in DELTA_GRID:
            for tp in TP_GRID:
                for sl in SL_GRID:
                    ret, tr, w, l = backtest(df, lb, d, tp, sl)
                    if ret > best["ret"]:
                        best.update({"ret":ret, "lookback":lb, "delta":d, "tp":tp, "sl":sl, "trades":tr, "wins":w, "losses":l})
    return best

# ───────────────────────────── Bot class ───────────────────────────────── #
class BreakoutBot:
    def __init__(self, figi: str):
        self.figi = figi
        self.df: pd.DataFrame = pd.DataFrame()
        self.best: dict | None = None
        self.day: date | None = None
        self.capital = CAPITAL_START
        self.pos_qty = 0
        self.entry_px = self.entry_val = 0.0
        self.running = True

    def start(self):
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)
        tg_send(f"🚀 Breakout bot started (live: `{LIVE_TRADING}`)")

        self.day = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Moscow")).date()
        self._refresh_history()

        while self.running:
            try:
                self._maybe_new_day()
                candle = fetch_latest_candle(self.figi)
                if candle:
                    self._process_candle(candle)
                time.sleep(60)  # раз в минуту
            except Exception as exc:
                log.exception("Unhandled error: %s", exc)
                tg_send(f"❗️ Unhandled error: `{exc}`")
                time.sleep(10)

    def _stop(self, *_):
        self.running = False
        tg_send("⏹ Bot stopped")

    def _maybe_new_day(self):
        today = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Moscow")).date()
        if today != self.day:
            self.day = today
            self._refresh_history()

    def _refresh_history(self):
        log.info("Fetching history...")
        self.df = fetch_candles(self.figi, INTERVAL, DAYS_BACK)
        self.best = optimize_params(self.df)
        msg = (
            "🔍 Best params for last *{}* days:\n"
            "lookback = `{lookback}`, delta = `{delta}`, tp = `{tp}`, sl = `{sl}`\n"
            "PnL = `{ret:.2f}%`, trades = `{trades}` (win {wins} / loss {losses})"
        ).format(DAYS_BACK, **self.best)
        tg_send(msg)
        log.info(msg.replace("\n", " "))

    def _process_candle(self, c: dict):
        ts = c["time"]
        self.df.loc[ts] = [c[k] for k in ["open","high","low","close","vol"]]
        lb = self.best["lookback"]
        hi_lvl = self.df["high"].rolling(lb).max().shift(1).iat[-1]
        vol_ma = self.df["vol"].rolling(lb).mean().shift(1).iat[-1]
        close, vol = c["close"], c["vol"]

        # === выход из позиции ===
        if self.pos_qty:
            change = close / self.entry_px - 1
            if change >= self.best["tp"] or change <= -self.best["sl"]:
                proceeds = close * self.pos_qty          # валовая выручка
                pnl_gross = proceeds - self.entry_val    # до комиссии
                pnl_net   = pnl_gross - proceeds * COMMISSION
                self.capital += pnl_net
                res  = "✅ TP hit" if change >= self.best["tp"] else "🛑 SL hit"
                word = "прибыль" if pnl_net >= 0 else "убыток"
                tg_send(f"{res} @ `{close}` {word} `{pnl_net:.2f}` equity `{self.capital:.2f}`")  ### CHANGED ###
                self._close_position()
                return

        # === вход в позицию ===
        if (not self.pos_qty and
            close > hi_lvl and
            (close - hi_lvl)/hi_lvl >= self.best["delta"] and
            vol > vol_ma):
            risk = self.capital * RISK_PCT
            qty  = min(math.floor(risk / (close * self.best["sl"])),
                       math.floor(self.capital / close))
            if qty > 0:
                self._open_position(qty, close)

    # ---------- ОРДЕРА ----------
    def _open_position(self, qty: int, price: float):
        self.pos_qty   = qty
        self.entry_px  = price
        self.entry_val = qty * price
        cost = self.entry_val * (1 + COMMISSION)
        tg_send(f"📈 Buy {qty} @ `{price}` cost `{cost:.2f}` (live={LIVE_TRADING})")
        if LIVE_TRADING:
            self._place_market_order(qty, OrderDirection.ORDER_DIRECTION_BUY)

    def _close_position(self):
        if self.pos_qty and LIVE_TRADING:
            self._place_market_order(self.pos_qty, OrderDirection.ORDER_DIRECTION_SELL)
        self.pos_qty = 0

    def _place_market_order(self, qty: int, direction: OrderDirection):
        try:
            with Client(TOKEN_INVEST) as cl:
                order_id = f"bot-{int(time.time()*1e6)}"
                cl.orders.post_order(
                    figi=self.figi,
                    quantity=qty,
                    order_type=OrderType.ORDER_TYPE_MARKET,
                    direction=direction,
                    account_id=cl.users.get_accounts().accounts[0].id,
                    order_id=order_id,
                )
                tg_send(f"💸 Order {order_id} executed: {direction.name} {qty}")
        except Exception as exc:
            log.exception("Order failed: %s", exc)
            tg_send(f"⚠️ Order failed: `{exc}`")

# ─────────────────────────────── main ───────────────────────────────────── #
if __name__ == "__main__":
    bot = BreakoutBot(FIGI)
    bot.start()

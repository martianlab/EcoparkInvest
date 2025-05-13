"""
Если цена пробивает недавний максимум, и в этот момент повышается объём — значит, на рынке есть реальный интерес.
Это может быть началом направленного импульса, и мы стараемся «запрыгнуть» в этот момент.
"""
import os, math, time, random
from datetime import datetime, timedelta

import pandas as pd
from tinkoff.invest import Client, CandleInterval, InstrumentStatus
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode

# ─── ПАРАМЕТРЫ ─────────────────────────────────────────────────────────────────
TICKER = "VTBR"
INTERVAL_BT = CandleInterval.CANDLE_INTERVAL_1_MIN
DAYS_BACK = 60

CAPITAL_START = 50_000
COMMISSION = 0.0004
RISK_PCT = 0.02
TP_GRID = [0.005, 0.01, 0.015]
SL_GRID = [0.003, 0.005, 0.01]
DELTA_GRID = [0.001, 0.002, 0.003]
LOOKBACK_GRID = [10, 20, 30]

# ─── ТОКЕН ────────────────────────────────────────────────────────────────────
TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")


# ─── FIGI ──────────────────────────────────────────────────────────────────────
def resolve_figi(ticker: str) -> str | None:
    with Client(TOKEN_INVEST) as c:
        for s in c.instruments.shares(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
        ).instruments:
            if s.ticker.upper() == ticker.upper() and s.api_trade_available_flag:
                return s.figi
    return None


FIGI = resolve_figi(TICKER)
if not FIGI:
    raise SystemExit(f"❌ FIGI {TICKER} не найден")


# ─── ЗАБОР СВЕЧЕЙ ─────────────────────────────────────────────────────────────
def fetch_candles(figi, interval, days):
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
                        backoff *= 2;
                        left -= 1
                    else:
                        raise

        for c in safe_gen():
            rows.append({
                "time": pd.to_datetime(c.time),
                "high": c.high.units + c.high.nano / 1e9,
                "close": c.close.units + c.close.nano / 1e9,
                "vol": c.volume
            })
    df = pd.DataFrame(rows).set_index("time").sort_index()
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    return df.tz_convert("Europe/Moscow")


# ─── BACKTEST ─────────────────────────────────────────────────────────────────
def backtest(df, lookback, delta, tp, sl):
    df = df.copy()
    df["hi_lvl"] = df["high"].rolling(lookback).max().shift(1)
    df["vol_ma"] = df["vol"].rolling(lookback).mean().shift(1)

    cap, pos_qty, entry_px, entry_val = CAPITAL_START, 0, None, 0
    trades, wins, losses = 0, 0, 0
    for t, r in df.iterrows():
        # Выход по tp/sl
        if pos_qty:
            change = r.close / entry_px - 1
            if change >= tp or change <= -sl:
                cap += (r.close * pos_qty - entry_val) - r.close * pos_qty * COMMISSION
                if change >= tp:
                    wins += 1
                else:
                    losses += 1
                pos_qty = 0
                trades += 1

        # Вход по пробою
        if pos_qty == 0 and r.close > r.hi_lvl and (r.close - r.hi_lvl) / r.hi_lvl >= delta:
            if r.vol > r.vol_ma:
                risk_cash = cap * RISK_PCT
                qty = min(math.floor(risk_cash / (r.close * sl)), math.floor(cap / r.close))
                if qty > 0:
                    pos_qty = qty
                    entry_px = r.close
                    entry_val = qty * r.close
                    cap -= entry_val * COMMISSION
    return (cap / CAPITAL_START - 1) * 100, trades, wins, losses


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = fetch_candles(FIGI, INTERVAL_BT, DAYS_BACK)
    best, best_params, best_trades, best_wins, best_losses = -1e9, None, 0, 0, 0
    for lb in LOOKBACK_GRID:
        for d in DELTA_GRID:
            for tp in TP_GRID:
                for sl in SL_GRID:
                    ret, trades, wins, losses = backtest(df, lb, d, tp, sl)
                    if ret > best:
                        best = ret
                        best_params = (lb, d, tp, sl)
                        best_trades = trades
                        best_wins = wins
                        best_losses = losses

    print(f"🔍 Лучшая конфигурация:")
    print(f"  lookback={best_params[0]}, delta={best_params[1]}, tp={best_params[2]}, sl={best_params[3]}")
    print(f"  PnL: {best:.2f}% за {DAYS_BACK} дней на 1-минутных свечах")
    print(f"  Сделок: {best_trades} (успешных: {best_wins}, убыточных: {best_losses})")

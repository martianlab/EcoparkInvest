import os, sys, asyncio, math, time, random, pytz
from datetime import datetime, timedelta

import pandas as pd
from tinkoff.invest import Client, AsyncClient, CandleInterval, InstrumentStatus
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode
import telegram

# ─── НАСТРОЙКИ ────────────────────────────────────────────────────────────────
TICKER          = "SBER"
INTERVAL_BT     = CandleInterval.CANDLE_INTERVAL_30_MIN     # для back-test
INTERVAL_LIVE   = CandleInterval.CANDLE_INTERVAL_1_MIN      # для мониторинга
DAYS_BACK       = 365
REPORT_INTERVAL = 3600        # раз в секунды: отправка текущего курса

CAPITAL_START = 50_000        # ₽
COMMISSION    = 0.0004        # 0.04 %
RISK_PCT      = 0.02          # риск на сделку (2 %)

GRID_LEVEL  = [20, 30, 40, 60]
GRID_TP     = [0.012, 0.015, 0.018]
GRID_SL     = [0.004, 0.005, 0.006]
GRID_DELTA  = [0.003, 0.004]

TZ_MS            = pytz.timezone("Europe/Moscow")
SESSION_BEG      = datetime.strptime("10:00", "%H:%M").time()
SESSION_END      = datetime.strptime("18:45", "%H:%M").time()
DAILY_RECALC_AT  = datetime.strptime("09:55", "%H:%M").time()
POLL_SECONDS     = 30           # частота опроса котировки

# ─── АВТОРИЗАЦИЯ ────────────────────────────────────────────────────────────────
TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")
TG_TOKEN     = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID   = os.getenv("TG_CHAT_ID")  # строка: id или @channel

bot = telegram.Bot(token=TG_TOKEN)

# ─── ПОМОЩНИК TELEGRAM ─────────────────────────────────────────────────────────
async def tg_send(text: str):
    try:
        await bot.send_message(chat_id=TG_CHAT_ID, text=text, parse_mode="HTML")
    except Exception:
        pass

# ─── РАЗРЕШЕНИЕ FIGI ────────────────────────────────────────────────────────────
def resolve_figi(ticker: str) -> str | None:
    with Client(TOKEN_INVEST) as c:
        for inst in c.instruments.shares(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
        ).instruments:
            if inst.ticker.upper() == ticker.upper() and inst.api_trade_available_flag:
                return inst.figi
    return None

FIGI = resolve_figi(TICKER)
if FIGI is None:
    asyncio.run(tg_send(f"❌ FIGI для {TICKER} не найден"))
    raise SystemExit("FIGI не найден")

# ─── ЗАГРУЗКА СВЕЧЕЙ ─────────────────────────────────────────────────────────────
def fetch_candles(figi: str, interval, days: int) -> pd.DataFrame:
    since = now() - timedelta(days=days)
    rows = []
    with Client(TOKEN_INVEST) as cl:
        def safe_gen():
            backoff, attempts_left = 1, 5
            while True:
                try:
                    yield from cl.get_all_candles(figi=figi, from_=since, interval=interval)
                    break
                except RequestError as e:
                    status, _, meta = e.args
                    if status == StatusCode.RESOURCE_EXHAUSTED:
                        time.sleep(max(int(meta.ratelimit_reset), 1) + 1)
                    elif status == StatusCode.UNAVAILABLE and attempts_left:
                        time.sleep(backoff + random.uniform(0, .5))
                        backoff *= 2; attempts_left -= 1
                    else:
                        raise
        for c in safe_gen():
            rows.append({
                "time":  pd.to_datetime(c.time),
                "open":  c.open.units  + c.open.nano  / 1e9,
                "high":  c.high.units  + c.high.nano  / 1e9,
                "low":   c.low.units   + c.low.nano   / 1e9,
                "close": c.close.units + c.close.nano / 1e9,
                "vol":   c.volume
            })
    df = pd.DataFrame(rows).set_index("time").sort_index()
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    return df.tz_convert(TZ_MS)

# ─── УРОВНИ ДЛЯ BACKTEST ─────────────────────────────────────────────────────────
def add_levels(df: pd.DataFrame, period: int) -> pd.DataFrame:
    df["hi_lvl"] = df["high"].rolling(period).max().shift(1)
    return df

# ─── BACKTEST (без Telegram-флуда) ──────────────────────────────────────────────
def backtest(df_raw: pd.DataFrame,
             *, level_period: int, tp: float, sl: float, min_delta: float,
             hrs=(10,18)) -> float:
    df = add_levels(df_raw.copy(), level_period)
    cap, pos_qty, entry_px, entry_val = CAPITAL_START, 0, None, 0
    for t, r in df.iterrows():
        if pos_qty:
            change = (r.close / entry_px) - 1
            if change >= tp or change <= -sl:
                cap += (pos_qty * r.close - entry_val) - (pos_qty * r.close) * COMMISSION
                pos_qty = 0
        if pos_qty == 0 and hrs[0] <= t.hour < hrs[1] and r.close > r.hi_lvl and (r.close - r.hi_lvl)/r.hi_lvl >= min_delta:
            risk_cash = cap * RISK_PCT
            qty = min(math.floor(risk_cash/(r.close*sl)), math.floor(cap/r.close))
            if qty:
                pos_qty, entry_px, entry_val = qty, r.close, qty*r.close
                cap -= entry_val * COMMISSION
    return (cap / CAPITAL_START - 1) * 100

# ─── ОПТИМИЗАЦИЯ С УВЕДОМЛЕНИЕМ ─────────────────────────────────────────────────
async def choose_and_notify(df: pd.DataFrame) -> tuple[int,float,float,float]:
    best, params = float('-inf'), (0,0,0,0)
    for lp in GRID_LEVEL:
        for tp in GRID_TP:
            for sl in GRID_SL:
                for md in GRID_DELTA:
                    pnl = backtest(df, level_period=lp, tp=tp, sl=sl, min_delta=md)
                    if pnl > best:
                        best, params = pnl, (lp,tp,sl,md)
    await tg_send(
        f"🔄 Оптимизация:\nПрофит: {best:.2f}%\nlvl={params[0]}, tp={params[1]}, sl={params[2]}, Δ={params[3]}"
    )
    return params

# ─── LIVE-ТРЕЙДЕР ─────────────────────────────────────────────────────────────
class LiveTrader:
    def __init__(self, figi: str):
        self.figi = figi
        self.level_period = self.tp = self.sl = self.delta = None
        self.hi_cache = []
        self.pos_qty  = 0
        self.entry_px = 0

    def set_params(self, lvl, tp, sl, delta):
        self.level_period, self.tp, self.sl, self.delta = lvl, tp, sl, delta
        self.hi_cache.clear()
        asyncio.create_task(
            tg_send(f"🔄 Live-параметры: lvl={lvl}, tp={tp}, sl={sl}, Δ={delta}")
        )

    def feed_price(self, price: float):
        self.hi_cache.append(price)
        if len(self.hi_cache) > self.level_period:
            self.hi_cache.pop(0)
        if len(self.hi_cache) < self.level_period:
            return
        hi_lvl = max(self.hi_cache[:-1])
        if self.pos_qty:
            change = (price / self.entry_px) - 1
            if change >= self.tp or change <= -self.sl:
                asyncio.create_task(
                    tg_send(f"💰 SELL {TICKER} | Цена: {price:.2f} | P/L: {change*100:.2f}%")
                )
                self.pos_qty = 0
                return
        if self.pos_qty == 0 and price > hi_lvl and (price - hi_lvl)/hi_lvl >= self.delta:
            tp_p = price * (1 + self.tp)
            sl_p = price * (1 - self.sl)
            asyncio.create_task(
                tg_send(f"🚀 BUY {TICKER} | Цена: {price:.2f} | TP: {tp_p:.2f} | SL: {sl_p:.2f}")
            )
            self.pos_qty = 1
            self.entry_px = price

# ─── ПЕРИОДИЧЕСКИЕ ЗАДАЧИ ─────────────────────────────────────────────────────
async def sleep_until(target: datetime):
    now_utc = datetime.now(tz=pytz.UTC)
    delta   = (target - now_utc).total_seconds()
    if delta > 0:
        await asyncio.sleep(delta)

async def daily_optimizer(trader: LiveTrader):
    hist = fetch_candles(FIGI, INTERVAL_BT, DAYS_BACK)
    lvl,tp,sl,md = await choose_and_notify(hist)
    trader.set_params(lvl,tp,sl,md)
    while True:
        today = datetime.now(tz=TZ_MS).date()
        run_at = datetime.combine(today, DAILY_RECALC_AT, tzinfo=TZ_MS)
        if datetime.now(tz=TZ_MS) > run_at:
            run_at += timedelta(days=1)
        await sleep_until(run_at.astimezone(pytz.UTC))
        hist = fetch_candles(FIGI, INTERVAL_BT, DAYS_BACK)
        lvl,tp,sl,md = await choose_and_notify(hist)
        trader.set_params(lvl,tp,sl,md)

async def poll_prices(trader: LiveTrader):
    async with AsyncClient(TOKEN_INVEST) as cl:
        while True:
            now_time = datetime.now(tz=TZ_MS).time()
            if SESSION_BEG <= now_time <= SESSION_END and trader.level_period:
                try:
                    resp = await cl.market_data.get_last_prices(figi=[FIGI])
                    p = next(x for x in resp.last_prices if x.figi == FIGI)
                    price = p.price.units + p.price.nano/1e9
                    trader.feed_price(price)
                except Exception:
                    pass
            await asyncio.sleep(POLL_SECONDS)

async def periodic_report():
    # раз в REPORT_INTERVAL секунд отправляет текущий курс
    async with AsyncClient(TOKEN_INVEST) as cl:
        while True:
            try:
                resp = await cl.market_data.get_last_prices(figi=[FIGI])
                p = next(x for x in resp.last_prices if x.figi == FIGI)
                price = p.price.units + p.price.nano/1e9
                await tg_send(f"📈 Текущий курс {TICKER}: {price:.2f}")
            except Exception:
                pass
            await asyncio.sleep(REPORT_INTERVAL)

async def main():
    await tg_send(f"✅ Бот запущен для {TICKER}")
    trader = LiveTrader(FIGI)
    await asyncio.gather(
        daily_optimizer(trader),
        poll_prices(trader),
        periodic_report()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        asyncio.run(tg_send("ℹ️ Bot stopped."))

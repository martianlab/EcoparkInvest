import os, itertools, time, random, math
import pandas as pd, numpy as np
from datetime import timedelta
from tinkoff.invest import Client, CandleInterval, InstrumentStatus
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode

# ─── Пользовательские настройки ───────────────────────────────────────────────
TOKEN          = os.getenv("TINKOFF_TOKEN")

TICKERS        = ["IRAO"]              # список тикеров
INTERVAL       = CandleInterval.CANDLE_INTERVAL_30_MIN
DAYS_BACK      = 365

CAPITAL_START  = 50_000             # ₽
COMMISSION     = 0.0004                # 0.04 %
RISK_PCT       = 0.02

GRID_LEVEL     = [20, 30, 40, 60]
GRID_TP         = [0.012, 0.015, 0.018]
GRID_SL         = [0.004, 0.005]
GRID_DELTA     = [0.003, 0.004]

EQUITY_FREQ    = "1D"                  # частота записи equity‑кривой

# ─── FIGI ──────────────────────────────────────────────────────────────────────
def resolve_figi(ticker, token):
    with Client(token) as c:
        for s in c.instruments.shares(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
            ).instruments:
            if s.ticker.upper() == ticker.upper() and s.api_trade_available_flag:
                return s.figi
    return None

# ─── Устойчивый загрузчик свечей ──────────────────────────────────────────────
def safe_get_all_candles(client, **kwargs):
    backoff, attempts_left = 1, 5
    while True:
        try:
            yield from client.get_all_candles(**kwargs)
            break
        except RequestError as e:
            status, details, meta = e.args
            if status == StatusCode.RESOURCE_EXHAUSTED:
                wait = max(int(meta.ratelimit_reset), 1)
                print(f"  ⏳ RPS‑лимит, жду {wait}s …")
                time.sleep(wait + 1)
            elif status == StatusCode.UNAVAILABLE and attempts_left:
                print(f"  🔄 Тайм‑аут, retry in {backoff}s … ({attempts_left} left)")
                time.sleep(backoff + random.uniform(0, .5))
                backoff *= 2; attempts_left -= 1
            else:
                raise

def fetch_candles(token, figi):
    since = now() - timedelta(days=DAYS_BACK)
    rows  = []
    with Client(token) as cl:
        try:
            for c in safe_get_all_candles(cl, figi=figi, from_=since, interval=INTERVAL):
                rows.append({
                    "time":  pd.to_datetime(c.time),
                    "open":  c.open.units  + c.open.nano  / 1e9,
                    "high":  c.high.units  + c.high.nano  / 1e9,
                    "low":   c.low.units   + c.low.nano   / 1e9,
                    "close": c.close.units + c.close.nano / 1e9,
                    "vol":   c.volume})
        except RequestError as e:
            print("  ⚠️  Не удалось загрузить свечи:", e.args[1]); return None
    df = pd.DataFrame(rows).set_index("time").sort_index()
    if df.empty: return None
    if df.index.tz is None: df = df.tz_localize("UTC")
    return df.tz_convert("Europe/Moscow")

# ─── Технический уровень ──────────────────────────────────────────────────────
def add_levels(df, p):
    df["hi_lvl"] = df["high"].rolling(p).max().shift(1)
    return df

# ─── Back‑test с детальным логом ──────────────────────────────────────────────
def backtest(df_raw, *, level_period, tp, sl, min_delta, hrs=(10, 17)):
    df            = add_levels(df_raw.copy(), level_period)
    cap           = CAPITAL_START
    pos_qty       = 0
    entry_price   = None
    entry_value   = 0

    trades, equity = [], []

    for t, r in df.iterrows():
        # ── выход
        if pos_qty:
            change = (r.close / entry_price) - 1
            if change >= tp or change <= -sl:
                exit_val = pos_qty * r.close
                pnl      = exit_val - entry_value
                cap     += pnl
                cap     -= exit_val * COMMISSION
                trades.append({
                    "time": t, "side": "SELL",
                    "qty":  pos_qty, "price": r.close,
                    "value": exit_val, "pnl": pnl,
                    "capital_after": cap
                })
                pos_qty = 0; entry_price = None; entry_value = 0

        # ── вход
        if pos_qty == 0 and hrs[0] <= t.hour < hrs[1]:
            if r.close > r.hi_lvl and (r.close - r.hi_lvl) / r.hi_lvl >= min_delta:
                risk_money = cap * RISK_PCT
                qty_float  = risk_money / (r.close * sl)
                qty        = math.floor(min(qty_float, cap / r.close))
                if qty > 0:
                    entry_price = r.close
                    pos_qty     = qty
                    entry_value = qty * entry_price
                    cap        -= entry_value * COMMISSION
                    trades.append({
                        "time": t, "side": "BUY",
                        "qty":  qty, "price": entry_price,
                        "value": entry_value, "pnl": 0,
                        "capital_after": cap
                    })

        # ── equity‑срез
        if not equity or t.floor(EQUITY_FREQ) > equity[-1]["time"]:
            m2m = cap if pos_qty == 0 else cap + pos_qty * (r.close - entry_price)
            equity.append({"time": t.floor(EQUITY_FREQ), "equity": m2m})

    # ── финальная переоценка
    if pos_qty:
        m2m = cap + pos_qty * (df.iloc[-1].close - entry_price)
    else:
        m2m = cap
    equity.append({"time": df.index[-1].floor(EQUITY_FREQ), "equity": m2m})

    ret  = (m2m / CAPITAL_START - 1) * 100
    pnl_arr = np.array([tr["pnl"] for tr in trades if tr["side"] == "SELL"])
    win = (pnl_arr > 0).mean() * 100 if pnl_arr.size else 0
    avg = np.mean(pnl_arr / CAPITAL_START * 100) if pnl_arr.size else 0

    return {
        "return_%":    ret,
        "trades":      len(pnl_arr),
        "win_%":       win,
        "avg_trade_%": avg,
        "trades_df":   pd.DataFrame(trades),
        "equity_df":   pd.DataFrame(equity).drop_duplicates("time")
    }

# ─── Главный прогон ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    summary = []

    for tic in TICKERS:
        print(f"\n=== {tic} ===")
        figi = resolve_figi(tic, TOKEN)
        if not figi: print("  ⚠️  FIGI не найден"); continue

        candles = fetch_candles(TOKEN, figi)
        if candles is None: print("  ⚠️  Пропуск"); continue

        # ── подбор лучшей сетки
        best_metrics, best_params = None, None
        for lp, tp, sl, md in itertools.product(
                GRID_LEVEL, GRID_TP, GRID_SL, GRID_DELTA):
            res = backtest(candles, level_period=lp, tp=tp, sl=sl, min_delta=md)
            if best_metrics is None or res["return_%"] > best_metrics["return_%"]:
                best_metrics, best_params = res, (lp, tp, sl, md)

        lp, tp, sl, md = best_params
        print(f"  PnL {best_metrics['return_%']:.2f}% | trades={best_metrics['trades']} | "
              f"lvl={lp} tp={tp} sl={sl} Δ={md}")

        # ── финальный лог
        detailed = backtest(candles, level_period=lp, tp=tp, sl=sl, min_delta=md)

        detailed["trades_df"].to_csv(f"trades_{tic}.csv", index=False)
        detailed["equity_df"].to_csv(f"equity_{tic}.csv", index=False)

        print(f"    📄 Лог сделок  → trades_{tic}.csv")
        print(f"    📈 Equity‑кривая → equity_{tic}.csv")

        # ── добавляем ВСЕ столбцы в summary
        summary.append({
            "ticker": tic,
            "lvl":    lp,
            "tp":     tp,
            "sl":     sl,
            "delta":  md,
            **best_metrics
        })

    # ── экранная сводка
    df = pd.DataFrame(summary).sort_values("return_%", ascending=False)
    print("\n==== Сводная таблица ====")
    if not df.empty:
        print(df[["ticker", "return_%", "trades", "win_%",
                  "lvl", "tp", "sl", "delta"]]
              .to_string(index=False,
                         formatters={"return_%": "{:6.2f}".format,
                                     "win_%":    "{:5.1f}".format}))
    else:
        print("Нет данных – все тикеры пропущены.")
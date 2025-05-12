import os, itertools, time, random
import pandas as pd, numpy as np
from tinkoff.invest import Client, CandleInterval, InstrumentStatus
from tinkoff.invest.utils import now
from tinkoff.invest.exceptions import RequestError
from grpc import StatusCode
from datetime import timedelta

# ─── Пользовательские настройки ────────────────────────────────────────────────
TOKEN = os.getenv("TINKOFF_TOKEN")

TICKERS = [
    # ── Голубые фишки
    "SBER","GAZP","LKOH","GMKN","NVTK","ROSN","TATN","CHMF","ALRS",
    "MAGN","NLMK","MOEX","MTSS","PLZL","POLY",
    # ── Второй эшелон (ликвид)
    "AFLT","PIKK","RASP","VTBR","IRAO","HYDR","RSTI","TGKA","TRNFP",
    "SNGS","SNGSP","MGNT","FIVE","PHOR","AKRN","RUAL","BANEP","MSNG",
    "ENPG","MRKC"
]

INTERVAL      = CandleInterval.CANDLE_INTERVAL_30_MIN
DAYS_BACK     = 365
COMMISSION    = 0.0004
CAPITAL_START = 1_000_000

GRID_LEVEL, GRID_TP = [20,30,40,60], [0.008,0.010,0.012]
GRID_SL, GRID_DELTA = [0.003,0.004,0.005], [0.003,0.004]

# ─── FIGI ──────────────────────────────────────────────────────────────────────
def resolve_figi(ticker, token):
    with Client(token) as c:
        for s in c.instruments.shares(
                instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
            ).instruments:
            if s.ticker.upper()==ticker.upper() and s.api_trade_available_flag:
                return s.figi
    return None

# ─── Надёжная загрузка свечей ─────────────────────────────────────────────────
def safe_get_all_candles(client, **kwargs):
    """yield‑генератор: выдерживает лимит RPS и сетевые тайм‑ауты"""
    backoff = 1
    attempts_left = 5
    while True:
        try:
            for candle in client.get_all_candles(**kwargs):
                yield candle
            break                                 # вышли, всё ок
        except RequestError as e:
            status, details, meta = e.args
            if status == StatusCode.RESOURCE_EXHAUSTED:
                reset = int(meta.ratelimit_reset)
                wait  = max(reset, 1)
                print(f"  ⏳ Лимит RPS, жду {wait}s …")
                time.sleep(wait + 1)
            elif status == StatusCode.UNAVAILABLE and attempts_left:
                print(f"  🔄 Тайм‑аут, retry in {backoff}s … ({attempts_left} left)")
                time.sleep(backoff + random.uniform(0,0.5))
                backoff *= 2
                attempts_left -= 1
            else:
                raise

def fetch_candles(token, figi):
    since = now() - timedelta(days=DAYS_BACK)
    rows=[]
    with Client(token) as cl:
        try:
            for c in safe_get_all_candles(cl, figi=figi, from_=since, interval=INTERVAL):
                rows.append({
                    "time":  pd.to_datetime(c.time),
                    "open":  c.open.units+c.open.nano/1e9,
                    "high":  c.high.units+c.high.nano/1e9,
                    "low":   c.low.units +c.low.nano /1e9,
                    "close": c.close.units+c.close.nano/1e9,
                    "vol":   c.volume})
        except RequestError as e:
            print("  ⚠️  Не удалось загрузить свечи: ", e.args[1])
            return None
    df=pd.DataFrame(rows).set_index("time").sort_index()
    if df.empty: return None
    if df.index.tz is None: df=df.tz_localize("UTC")
    return df.tz_convert("Europe/Moscow")

# ─── Стратегия ─────────────────────────────────────────────────────────────────
def add_levels(df,p): df["hi_lvl"]=df["high"].rolling(p).max().shift(1); return df

def backtest(df_raw,*,level_period,tp,sl,min_delta,hrs=(10,17)):
    df=add_levels(df_raw.copy(),level_period)
    cap,pos,entry=CAPITAL_START,0,None; deals=[]
    for t,r in df.iterrows():
        if pos and ((r.close/entry-1)>=tp or (r.close/entry-1)<=-sl):
            pnl=(r.close/entry-1)*cap; cap+=pnl; cap-=cap*COMMISSION; deals.append(pnl); pos=0
        if pos==0 and hrs[0]<=t.hour<hrs[1]:
            if r.close>r.hi_lvl and (r.close-r.hi_lvl)/r.hi_lvl>=min_delta:
                pos,entry=1,r.close; cap-=cap*COMMISSION
    ret=(cap/CAPITAL_START-1)*100
    win=(np.array(deals)>0).mean()*100 if deals else 0
    avg=np.mean(np.array(deals)/CAPITAL_START*100) if deals else 0
    return {"return_%":ret,"trades":len(deals),"win_%":win,"avg_trade_%":avg}

# ─── Главный цикл ──────────────────────────────────────────────────────────────
if __name__=="__main__":
    summary=[]
    for tic in TICKERS:
        print(f"\n=== {tic} ===")
        figi=resolve_figi(tic,TOKEN)
        if not figi: print("  ⚠️  FIGI не найден"); continue
        candles=fetch_candles(TOKEN,figi)
        if candles is None:
            print("  ⚠️  Пропуск из‑за ошибок загрузки"); continue
        best=None
        for lp,tp,sl,md in itertools.product(GRID_LEVEL,GRID_TP,GRID_SL,GRID_DELTA):
            res=backtest(candles,level_period=lp,tp=tp,sl=sl,min_delta=md)
            res.update({"lvl":lp,"tp":tp,"sl":sl,"delta":md})
            if best is None or res["return_%"]>best["return_%"]: best=res
        print(f"  PnL {best['return_%']:.2f}% | trades={best['trades']} | "
              f"lvl={best['lvl']} tp={best['tp']} sl={best['sl']} Δ={best['delta']}")
        summary.append({"ticker":tic,**best})

    df=pd.DataFrame(summary).sort_values("return_%",ascending=False)
    print("\n==== Сводная таблица ====")
    if not df.empty:
        print(df[["ticker","return_%","trades","win_%",
                   "lvl","tp","sl","delta"]]
              .to_string(index=False,
                         formatters={"return_%":"{:6.2f}".format,
                                     "win_%":"{:5.1f}".format}))
    else:
        print("Нет данных – все тикеры пропущены.")
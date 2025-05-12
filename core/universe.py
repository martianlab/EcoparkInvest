# file: core/universe.py
from datetime import datetime, timedelta
import json, os, pathlib, pandas as pd
from io import StringIO
from tinkoff.invest import Client, InstrumentStatus

CACHE = pathlib.Path(".universe_cache.json")
TOKEN_ENV = "TINKOFF_TOKEN"

class UniverseError(RuntimeError):
    pass

# ---------- кэш ----------
def _load_cache(hours: int):
    if CACHE.exists():
        ts, txt = json.loads(CACHE.read_text())
        if datetime.utcnow() - datetime.fromisoformat(ts) < timedelta(hours=hours):
            return pd.read_json(StringIO(txt))
    return None

def _save_cache(df: pd.DataFrame):
    CACHE.write_text(json.dumps([datetime.utcnow().isoformat(), df.to_json()]))

# ---------- главный вызов ----------
def get_share_universe(refresh_hours: int = 24) -> pd.DataFrame:
    """
    Возвращает DataFrame всех акций, доступных текущему аккаунту Tinkoff.
    Добавлена колонка `last` — последняя цена (по данным MarketDataService).
    """
    cached = _load_cache(refresh_hours)
    if cached is not None:
        return cached

    token = os.getenv(TOKEN_ENV)
    if not token:
        raise UniverseError(f"Environment variable {TOKEN_ENV} is not set")

    with Client(token) as cl:
        # 1) Список всех акций
        shares = cl.instruments.shares(
            instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
        ).instruments

        # 2) Последние цены (берём за один вызов)
        figi_list = [s.figi for s in shares]
        last_prices = cl.market_data.get_last_prices(figi=figi_list).last_prices
        price_map = {
            p.figi: p.price.units + p.price.nano / 1e9
            for p in last_prices
        }

    # 3) Собираем таблицу
    df = pd.DataFrame(
        {
            "ticker":   [s.ticker for s in shares],
            "figi":     [s.figi for s in shares],
            "currency": [s.currency for s in shares],
            "class":    [s.class_code for s in shares],
            "name":     [s.name for s in shares],
            "lot":      [s.lot for s in shares],
            "last":     [price_map.get(s.figi, float('nan')) for s in shares],
        }
    )

    _save_cache(df)
    return df

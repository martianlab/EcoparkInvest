import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import pandas as pd
from tinkoff.invest import Client, CandleInterval, InstrumentStatus


__all__: List[str] = [
    'TinkoffAPIError',
    'download_daily_candles',
    'download_intraday_candles',
]

# --- Logging setup ---
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "ERROR"),
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

TOKEN_ENV = 'TINKOFF_TOKEN'


class TinkoffAPIError(RuntimeError):
    """Raised on auth failures or missing data."""
    pass


def _get_token() -> str:
    tok = os.getenv(TOKEN_ENV)
    if not tok:
        logger.error("Environment variable %s not set", TOKEN_ENV)
        raise TinkoffAPIError(f"Environment variable {TOKEN_ENV} not set")
    return tok


def _find_figi(symbol: str, cl: Client) -> Optional[str]:
    """
    Найти FIGI для заданного тикера, опираясь только на доступные акции.
    Фильтруем по тикеру, class_code и флагам доступности торговли.
    """
    # Получаем список всех акций, доступных вашему счёту
    shares = cl.instruments.shares(
        instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE
    ).instruments

    symbol_up = symbol.upper()
    for s in shares:
        if (
            s.ticker.upper() == symbol_up and
            s.class_code in ("TQBR", "SPBXM") and
            s.buy_available_flag and s.sell_available_flag
        ):
            return s.figi

    logger.error("No matching share found for symbol %s", symbol)
    return None


def download_daily_candles(symbol: str, days: int = 750) -> pd.DataFrame:
    """Return DataFrame indexed by Date with OHLCV for `symbol`."""
    token = _get_token()
    with Client(token) as cl:
        figi = symbol if len(symbol) == 12 else _find_figi(symbol, cl)
        if figi is None:
            logger.error("Cannot resolve FIGI for %s", symbol)
            raise TinkoffAPIError(f"Cannot resolve FIGI for {symbol}")

        now = datetime.now(timezone.utc)
        frm = now - timedelta(days=days)
        candles = list(
            cl.get_all_candles(
                figi=figi,
                from_=frm,
                to=now,
                interval=CandleInterval.CANDLE_INTERVAL_DAY,
            )
        )

    if not candles:
        logger.warning("No daily candles returned for %s", symbol)
        raise TinkoffAPIError(f"No candles returned for {symbol}")

    rows = []
    for c in candles:
        rows.append({
            'Date': c.time.astimezone(timezone.utc).date(),
            'Open': c.open.units + c.open.nano / 1e9,
            'High': c.high.units + c.high.nano / 1e9,
            'Low':  c.low.units  + c.low.nano  / 1e9,
            'Close':c.close.units+ c.close.nano/ 1e9,
            'Volume': c.volume,
        })
    df = pd.DataFrame(rows).set_index('Date').sort_index()
    return df


def download_intraday_candles(symbol: str,
                             days: int = 1,
                             interval: int = 5) -> pd.DataFrame:
    """
    interval – минуты (1, 5, 15, 30).
    days – сколько последних календарных дней.
    """
    token = _get_token()
    with Client(token) as cl:
        figi = symbol if len(symbol) == 12 else _find_figi(symbol, cl)
        if figi is None:
            logger.error("Cannot resolve FIGI for %s", symbol)
            raise TinkoffAPIError(f"Cannot resolve FIGI for {symbol}")

        now = datetime.now(timezone.utc)
        frm = now - timedelta(days=days)
        try:
            candles = list(cl.get_all_candles(
                figi=figi,
                from_=frm,
                to=now,
                interval={
                    1: CandleInterval.CANDLE_INTERVAL_1_MIN,
                    5: CandleInterval.CANDLE_INTERVAL_5_MIN,
                    15: CandleInterval.CANDLE_INTERVAL_15_MIN,
                    30: CandleInterval.CANDLE_INTERVAL_30_MIN,
                }[interval]
            ))
        except Exception as e:
            logger.exception("Error fetching intraday candles for %s", symbol)
            raise

    if not candles:
        logger.warning("No intraday candles returned for %s @%dmin", symbol, interval)
        raise TinkoffAPIError(f"No intraday candles returned for {symbol} @{interval}m")

    logger.info("Retrieved %d intraday candles for %s @%dmin", len(candles), symbol, interval)
    rows = []
    for c in candles:
        rows.append({
            'Date': c.time.astimezone(timezone.utc),
            'Open': c.open.units + c.open.nano / 1e9,
            'High': c.high.units + c.high.nano / 1e9,
            'Low':  c.low.units  + c.low.nano  / 1e9,
            'Close':c.close.units+ c.close.nano/ 1e9,
            'Volume': c.volume,
        })
    df = pd.DataFrame(rows).set_index('Date').sort_index()
    return df
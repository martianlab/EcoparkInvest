
import pandas as pd
from pathlib import Path
from core.tinkoff_api import download_daily_candles, download_intraday_candles, TinkoffAPIError

def load_prices(src: str, interval: int = None, days: int = 750) -> pd.DataFrame:
    path = Path(src)
    if path.exists():
        df = (pd.read_parquet(path) if path.suffix.lower() in {'.parquet', '.pq'}
              else pd.read_csv(path))
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date').sort_index()
    else:
        try:
            if interval:
                df = download_intraday_candles(src, days=days, interval=interval)
            else:
                df = download_daily_candles(src, days=days)
        except TinkoffAPIError as e:
            raise SystemExit(e)

    return df
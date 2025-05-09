import requests
import pandas as pd

import requests
import pandas as pd

def get_dividends(ticker):
    url = f'https://iss.moex.com/iss/securities/{ticker}/dividends.json'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Ошибка запроса: {response.status_code}")

    data = response.json()
    block = data.get("dividends", {})
    columns = block.get("columns")
    rows = block.get("data")

    if not columns or not rows:
        raise Exception(f"Нет дивидендных данных для {ticker}")

    df = pd.DataFrame(rows, columns=columns)
    return df
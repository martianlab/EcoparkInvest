import sqlite3
from tinkoff.invest import Client, AccountType
import os

DB_PATH = "account_state.db"
TOKEN_INVEST = os.getenv("TINKOFF_TOKEN")

def init_account_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS account_balance (
            id INTEGER PRIMARY KEY,
            capital REAL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)

def update_account_balance():
    capital = get_cash_rub()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM account_balance")
        conn.execute("INSERT INTO account_balance (capital) VALUES (?)", (capital,))
    return capital

def get_cash_rub() -> float:
    token = os.getenv("TINKOFF_TOKEN")
    if not token:
        raise RuntimeError("TINKOFF_TOKEN not set")

    with Client(token) as client:
        accounts = client.users.get_accounts().accounts
        acc = next(a for a in accounts if a.type == AccountType.ACCOUNT_TYPE_TINKOFF)

        portfolio = client.operations.get_portfolio(account_id=acc.id)
        for p in portfolio.positions:
            if p.instrument_type == "currency" and p.figi == "BBG0013HGFT4":  # RUB
                rub = p.quantity.units + p.quantity.nano / 1e9
                return rub

    return 0.0

def get_account_balance() -> float:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT capital FROM account_balance ORDER BY updated_at DESC LIMIT 1").fetchone()
    return row[0] if row else 0.0
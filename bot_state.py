import sqlite3
from pathlib import Path

DB_PATH = Path("bot_state.db")

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_probabilities (
            ticker TEXT PRIMARY KEY,
            probability REAL,
            updated_at TEXT
        )
        """)

def set_probability(ticker: str, probability: float):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "REPLACE INTO bot_probabilities (ticker, probability, updated_at) VALUES (?, ?, datetime('now'))",
            (ticker.upper(), probability)
        )

def get_all_probabilities() -> dict[str, float]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT ticker, probability FROM bot_probabilities").fetchall()
    return {ticker: prob for ticker, prob in rows}
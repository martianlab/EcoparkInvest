from flask import Flask, render_template, request, redirect, url_for
from bot_manager import start_bot, stop_bot, stop_all_bots, get_status, get_probabilities
from bot_state import init_db
from account_state import init_account_db, update_account_balance, get_account_balance
import threading
import time
import pandas as pd
import os
import glob

app = Flask(__name__)

def load_latest_backtest_csv() -> list[dict]:
    files = sorted(glob.glob("backtest_results_*.csv"), reverse=True)
    if not files:
        return []
    try:
        df = pd.read_csv(files[0])
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"❌ Ошибка загрузки CSV: {e}")
        return []

@app.route("/", methods=["GET"])
def index():
    init_db()
    data = load_latest_backtest_csv()
    status = get_status()
    probs = get_probabilities()
    running = set(ticker for ticker, alive in status.items() if alive)

    data.sort(key=lambda row: probs.get(row["ticker"], 0), reverse=True)
    capital = get_account_balance()

    return render_template("index.html", data=data, running=running, probs=probs, capital=capital)

@app.route("/start/<ticker>", methods=["POST"])
def start(ticker):
    live = "live" in request.form
    start_bot(ticker, live=live)
    return redirect(url_for("index"))

@app.route("/stop/<ticker>", methods=["POST"])
def stop(ticker):
    stop_bot(ticker)
    return redirect(url_for("index"))

@app.route("/stop_all")
def stop_all():
    stop_all_bots()
    return redirect(url_for("index"))

def balance_updater(interval_sec=5):
    while True:
        try:
            update_account_balance()
        except Exception as e:
            print(f"[ERR] Ошибка обновления баланса: {e}")
        time.sleep(interval_sec)

if __name__ == "__main__":
    init_account_db()

    t = threading.Thread(target=balance_updater, daemon=True)
    t.start()

    app.run(debug=True)
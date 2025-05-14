from multiprocessing import Process
from breakout_bot import run_bot
from bot_state import get_all_probabilities, init_db

bot_processes = {}

def start_bot(ticker: str, live: bool = False):
    if ticker in bot_processes and bot_processes[ticker].is_alive():
        return False
    p = Process(target=run_bot, args=(ticker,), kwargs={"live": live})
    p.start()
    bot_processes[ticker] = p
    return True

def stop_bot(ticker: str):
    if ticker in bot_processes:
        bot_processes[ticker].terminate()
        bot_processes[ticker].join()
        del bot_processes[ticker]
        return True
    return False

def stop_all_bots():
    for p in bot_processes.values():
        p.terminate()
        p.join()
    bot_processes.clear()

def get_status():
    return {ticker: proc.is_alive() for ticker, proc in bot_processes.items()}

def get_probabilities():
    init_db()
    return get_all_probabilities()
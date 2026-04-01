import os
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from collections import deque
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WATCHLIST_RAW = os.getenv("WATCHLIST", "").strip()

if not WATCHLIST_RAW:
    raise Exception("❌ WATCHLIST not set")

IST = timezone(timedelta(hours=5, minutes=30))

# ================= WATCHLIST =================
def convert_symbol(sym):
    sym = sym.strip().upper()

    if ":" in sym:
        return sym

    if sym in ["NIFTY", "NIFTY50"]:
        return "NSE:NIFTY50-INDEX"
    if sym in ["BANKNIFTY"]:
        return "NSE:NIFTYBANK-INDEX"

    return f"NSE:{sym}-EQ"

SYMBOLS = [convert_symbol(s) for s in WATCHLIST_RAW.split(",") if s.strip()]

print("✅ Loaded Watchlist:", SYMBOLS)

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    log_path=""
)

# ================= TELEGRAM =================
def send(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=10
        )
    except Exception as e:
        print("Telegram error:", e)

# ================= RATE LIMIT =================
class RateLimiter:
    def __init__(self):
        self.calls = deque()

    def wait(self):
        now = time.time()
        while self.calls and now - self.calls[0] > 1:
            self.calls.popleft()
        if len(self.calls) >= 8:
            time.sleep(0.2)
            return self.wait()
        self.calls.append(time.time())

rl = RateLimiter()

# ================= HELPERS =================
def name(sym):
    return sym.split(":")[1].replace("-EQ", "").replace("-INDEX", "")

def now_ist():
    return datetime.now(IST)

# ================= DATA =================
def get_quotes():
    rl.wait()
    data = fyers.quotes({"symbols": ",".join(SYMBOLS)})
    out = {}
    for d in data.get("d", []):
        out[d["n"]] = d["v"].get("lp", 0)
    return out

def get_history(symbol, resolution):
    rl.wait()
    data = fyers.history({
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": (now_ist() - timedelta(days=5)).strftime("%Y-%m-%d"),
        "range_to": now_ist().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })
    return data.get("candles", [])

def get_option_chain(symbol):
    rl.wait()
    data = fyers.optionchain({"symbol": symbol})
    return data.get("data", {}).get("optionsChain", [])

# ================= OI =================
def get_oi(symbol, ltp):
    rows = get_option_chain(symbol)

    strikes = sorted(set(r.get("strike_price") for r in rows if r.get("strike_price")))
    if not strikes:
        return []

    near = min(strikes, key=lambda x: abs(x - ltp))

    idx = strikes.index(near)
    selected = strikes[max(0, idx-2):idx+2]

    out = []

    for s in selected:
        ce, pe = 0, 0
        for r in rows:
            if r.get("strike_price") == s:
                if r.get("type") == "CE":
                    ce = float(r.get("oi_change") or r.get("changeinopeninterest") or 0)
                if r.get("type") == "PE":
                    pe = float(r.get("oi_change") or r.get("changeinopeninterest") or 0)

        out.append((s, ce, pe))

    return out

# ================= TRADE =================
active = {}
closed = []

def register(symbol, side, entry, target, sl):
    key = f"{symbol}-{side}"

    if key in active:
        return

    active[key] = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "target": target,
        "sl": sl,
        "closed": False,
        "last_oi": 0
    }

def manage(trade, ltp):
    if trade["closed"]:
        return

    if trade["side"] == "SELL":

        if ltp <= trade["target"]:
            trade["closed"] = True
            send(f"🎯 TARGET HIT\n\n{ name(trade['symbol']) }")
            closed.append(trade)
            return

        if ltp >= trade["sl"]:
            trade["closed"] = True
            send(f"🛑 STOPLOSS HIT\n\n{ name(trade['symbol']) }")
            closed.append(trade)
            return

# ================= STRATEGY SAMPLE =================
def strategy(symbol, ltp):
    candles = get_history(symbol, 5)

    if len(candles) < 2:
        return

    last = candles[-1]
    low = last[3]
    high = last[2]

    entry = low
    target = entry * 0.99
    sl = high

    register(symbol, "SELL", entry, target, sl)

# ================= MAIN =================
def main():

    send("🚀 BOT STARTED")

    cache = {}

    while True:

        try:
            quotes = get_quotes()

            for sym, ltp in quotes.items():

                if ltp <= 0:
                    continue

                if sym not in cache:
                    strategy(sym, ltp)
                    cache[sym] = True

            for key in list(active.keys()):

                trade = active[key]
                ltp = quotes.get(trade["symbol"], trade["entry"])

                manage(trade, ltp)

                if trade["closed"]:
                    active.pop(key)
                    continue

                # OI UPDATE
                if time.time() - trade["last_oi"] > 300:
                    oi = get_oi(trade["symbol"], ltp)

                    msg = f"📊 OI UPDATE\n\n{name(trade['symbol'])}\n\n"

                    for s, ce, pe in oi:
                        msg += f"{int(s)} CE:{int(ce)} | PE:{int(pe)}\n"

                    send(msg)

                    trade["last_oi"] = time.time()

            # EOD SUMMARY
            if now_ist().time() >= dtime(15, 25):
                if closed:
                    msg = "📘 EOD SUMMARY\n\n"
                    for t in closed:
                        msg += f"{name(t['symbol'])}\nResult: Done\n\n"

                    send(msg)
                    closed.clear()

            time.sleep(20)

        except Exception as e:
            print("ERROR:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()

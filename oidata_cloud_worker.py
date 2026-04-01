import os
import time
import requests
from datetime import datetime, timedelta, timezone
from collections import deque
from fyers_apiv3 import fyersModel

# ================= ENV =================
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("❌ Missing FYERS credentials in ENV")

# ================= SETTINGS =================
SYMBOLS = [
    "NSE:RELIANCE-EQ","NSE:TCS-EQ","NSE:HDFCBANK-EQ","NSE:ICICIBANK-EQ"
]  # add your 50 stocks

IST = timezone(timedelta(hours=5, minutes=30))

# ================= RATE LIMIT =================
class RateLimiter:
    def __init__(self):
        self.sec = deque()
        self.min = deque()

    def wait(self):
        now = time.time()

        while self.sec and now - self.sec[0] > 1:
            self.sec.popleft()
        while self.min and now - self.min[0] > 60:
            self.min.popleft()

        if len(self.sec) >= 8:
            time.sleep(0.2)
            return self.wait()

        if len(self.min) >= 150:
            time.sleep(1)
            return self.wait()

        self.sec.append(time.time())
        self.min.append(time.time())

rl = RateLimiter()

# ================= FYERS =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

def get_quotes():
    rl.wait()
    data = fyers.quotes({"symbols": ",".join(SYMBOLS)})
    return {i["n"]: i["v"]["lp"] for i in data["d"]}

def get_history(symbol, resolution):
    rl.wait()
    today = datetime.now().strftime("%Y-%m-%d")
    past = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    data = fyers.history({
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": past,
        "range_to": today,
        "cont_flag": "1"
    })

    return data.get("candles", [])

def get_option_chain(symbol):
    rl.wait()
    return fyers.optionchain({"symbol": symbol, "strikecount": 12})

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        print(msg)

# ================= UTIL =================
def name(sym):
    return sym.split(":")[1].replace("-EQ","")

def now_time():
    return datetime.now(IST).strftime("%H:%M:%S")

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ================= CACHE =================
cache = {"quotes":{}, "15m":{}}
trades = {}
alerts_sent = set()

# ================= LOAD DATA =================
def load_15m():
    for batch in chunk(SYMBOLS,5):
        for s in batch:
            cache["15m"][s] = get_history(s,15)
        time.sleep(1)

# ================= OI =================
def get_oi(symbol, ltp):
    data = get_option_chain(symbol)
    rows = data["data"]["optionsChain"]

    strikes = sorted(set(r["strike_price"] for r in rows))
    near = min(strikes, key=lambda x: abs(x-ltp))
    idx = strikes.index(near)

    selected = strikes[max(0,idx-1):idx+3]

    result = []
    ce_count = 0
    pe_count = 0

    for s in selected:
        ce = pe = 0
        for r in rows:
            if r["strike_price"] == s:
                if r["type"] == "CE":
                    ce = r["oi_change"]
                if r["type"] == "PE":
                    pe = r["oi_change"]

        if ce > 0: ce_count += 1
        if pe > 0: pe_count += 1

        result.append((s, ce, pe))

    # bias
    if ce_count >= 2 and pe_count == 0:
        bias = "🔴 STRONG SELL"
        action = "HOLD SELL"
    elif pe_count >= 2 and ce_count == 0:
        bias = "🟢 STRONG BUY"
        action = "HOLD BUY"
    else:
        bias = "⚪ SIDEWAYS"
        action = "WAIT"

    return result, bias, action

# ================= STRATEGY =================
def breakout_15m(symbol, ltp):
    try:
        c1 = cache["15m"][symbol][0]

        high = c1[2]
        low = c1[3]

        if ltp > high:
            return "BUY", high, low
        elif ltp < low:
            return "SELL", high, low
    except:
        return None, None, None

# ================= MAIN LOOP =================
setup_done = False

while True:

    now = datetime.now(IST).time()

    if now < datetime.strptime("09:15","%H:%M").time():
        time.sleep(30)
        continue

    # load data once
    if not setup_done and now > datetime.strptime("09:45","%H:%M").time():
        load_15m()
        setup_done = True
        print("✅ 15m Data Loaded")

    # live quotes
    cache["quotes"] = get_quotes()

    for symbol, ltp in cache["quotes"].items():

        side, high, low = breakout_15m(symbol, ltp)

        if side and symbol not in trades:

            oi, bias, action = get_oi(symbol, ltp)

            entry = high if side=="BUY" else low
            sl = low if side=="BUY" else high
            target = entry*1.01 if side=="BUY" else entry*0.99

            msg = f"""
🕯️ 15M BREAKOUT

{name(symbol)}

Side   : {side}
Entry  : {entry}
LTP    : {ltp}
Target : {round(target,2)}
SL     : {sl}

OI Bias: {bias}
Action : {action}

Time   : {now_time()}
"""

            send(msg)

            trades[symbol] = {
                "side": side,
                "entry": entry,
                "sl": sl,
                "target": target,
                "last_oi": 0
            }

        # ================= TRADE MANAGEMENT =================
        if symbol in trades:
            t = trades[symbol]

            # TARGET / SL
            if t["side"] == "BUY":
                if ltp >= t["target"]:
                    send(f"🎯 TARGET HIT\n{name(symbol)}")
                    del trades[symbol]
                elif ltp <= t["sl"]:
                    send(f"🛑 STOPLOSS HIT\n{name(symbol)}")
                    del trades[symbol]

            elif t["side"] == "SELL":
                if ltp <= t["target"]:
                    send(f"🎯 TARGET HIT\n{name(symbol)}")
                    del trades[symbol]
                elif ltp >= t["sl"]:
                    send(f"🛑 STOPLOSS HIT\n{name(symbol)}")
                    del trades[symbol]

            # ================= OI UPDATE EVERY 5 MIN =================
            if time.time() - t["last_oi"] > 300:

                oi, bias, action = get_oi(symbol, ltp)

                msg = f"\n📊 OI UPDATE - {name(symbol)}\n\n"

                for s, ce, pe in oi:
                    msg += f"{s} → CE:{ce} | PE:{pe}\n"

                msg += f"\nBias: {bias}\nAction: {action}"

                send(msg)

                t["last_oi"] = time.time()

    time.sleep(20)

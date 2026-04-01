import os
import time
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

SYMBOL = "NSE:M&M-EQ"
CHECK_INTERVAL = 10

FIRST_CANDLE_MAX_PCT = float(os.getenv("FIRST_CANDLE_MAX_PCT", "1.0"))

# ================= ENV =================
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing credentials")

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

# ================= TELEGRAM =================
def send(msg):
    print(msg)
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        pass

# ================= DATA =================
def now():
    return datetime.now(IST)

def get_quotes():
    q = fyers.quotes({"symbols": SYMBOL})
    v = q["d"][0]["v"]
    return {
        "ltp": v["lp"],
        "open": v["open_price"],
        "high": v["high_price"],
        "low": v["low_price"],
        "prev_close": v["prev_close_price"]
    }

def get_history(res):
    payload = {
        "symbol": SYMBOL,
        "resolution": str(res),
        "date_format": "1",
        "range_from": (now() - timedelta(days=5)).strftime("%Y-%m-%d"),
        "range_to": now().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }
    return fyers.history(payload).get("candles", [])

def get_option_chain():
    r = fyers.optionchain({"symbol": SYMBOL, "strikecount": 10})
    return r.get("data", {}).get("optionsChain", [])

# ================= OI =================
def get_oi_bias(chain):
    call_oi = 0
    put_oi = 0

    for x in chain:
        oi = float(x.get("oi", 0))

        if str(x.get("option_type")).upper() == "CE":
            call_oi += oi
        else:
            put_oi += oi

    return call_oi, put_oi

# ================= STATE =================
trade = None
signal_sent = False

# ================= STRATEGY =================
def check_setup():
    quotes = get_quotes()
    daily = get_history("D")
    m15 = get_history(15)

    if len(daily) < 2 or len(m15) < 2:
        return None

    prev = daily[-2]
    today = daily[-1]

    first = m15[0]
    second = m15[1]

    # GAPUP+
    gapup = today[1] > prev[2]

    # SMALL CANDLE
    range_pct = ((first[2] - first[3]) / first[4]) * 100
    small = range_pct < FIRST_CANDLE_MAX_PCT

    # INSIDE BAR
    inside = second[2] <= first[2] and second[3] >= first[3]

    if gapup and small and inside:
        return {
            "high": first[2],
            "low": first[3]
        }

    return None

# ================= LOOP =================
send("🚀 FINAL CLOUD BOT STARTED")

while True:
    try:
        setup = check_setup()
        quotes = get_quotes()
        ltp = quotes["ltp"]

        if setup:
            chain = get_option_chain()
            call_oi, put_oi = get_oi_bias(chain)

            # ENTRY
            if not trade:
                if ltp > setup["high"] and put_oi > call_oi:
                    trade = {
                        "side": "BUY",
                        "entry": setup["high"],
                        "sl": setup["low"],
                        "target": setup["high"] + (setup["high"] - setup["low"])
                    }

                elif ltp < setup["low"] and call_oi > put_oi:
                    trade = {
                        "side": "SELL",
                        "entry": setup["low"],
                        "sl": setup["high"],
                        "target": setup["low"] - (setup["high"] - setup["low"])
                    }

                if trade:
                    send(f"""
🔥 ENTRY SIGNAL

Side: {trade['side']}
Entry: {trade['entry']}
SL: {trade['sl']}
Target: {trade['target']}
Time: {now().strftime('%H:%M:%S')}
""")

            # EXIT
            if trade:
                if trade["side"] == "BUY":
                    if ltp >= trade["target"]:
                        send(f"🎯 TARGET HIT BUY @ {ltp}")
                        trade = None
                    elif ltp <= trade["sl"]:
                        send(f"❌ STOPLOSS HIT BUY @ {ltp}")
                        trade = None

                elif trade["side"] == "SELL":
                    if ltp <= trade["target"]:
                        send(f"🎯 TARGET HIT SELL @ {ltp}")
                        trade = None
                    elif ltp >= trade["sl"]:
                        send(f"❌ STOPLOSS HIT SELL @ {ltp}")
                        trade = None

        time.sleep(CHECK_INTERVAL)

    except Exception as e:
        print("Error:", e)
        time.sleep(5)

import os
import time
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = open("client_id.txt").read().strip()
ACCESS_TOKEN = open("access_token.txt").read().strip()

TELEGRAM_TOKEN = "YOUR_BOT_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

SYMBOL = "NSE:M&M-EQ"
STRIKECOUNT = 15

FIRST_CANDLE_MAX_PCT = 1.0

CHECK_INTERVAL = 10  # seconds

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

# ================= HELPERS =================
def now():
    return datetime.now(IST)

def send(msg):
    print(msg)
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        pass

def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0

# ================= DATA =================
def get_quotes():
    r = fyers.quotes({"symbols": SYMBOL})
    v = r["d"][0]["v"]
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
    r = fyers.optionchain({"symbol": SYMBOL, "strikecount": STRIKECOUNT})
    return r.get("data", {}).get("optionsChain", [])

# ================= OI =================
def get_oi_bias(chain):
    call_oi = 0
    put_oi = 0

    for x in chain:
        strike = x.get("strike_price")
        oi = safe_float(x.get("oi"))

        if str(x.get("option_type")).upper() == "CE":
            call_oi += oi
        else:
            put_oi += oi

    return call_oi, put_oi

# ================= STRATEGY =================
def check_strategy():
    quotes = get_quotes()
    daily = get_history("D")
    m15 = get_history(15)

    if len(daily) < 2 or len(m15) < 2:
        return None

    prev_day = daily[-2]
    today = daily[-1]

    first = m15[0]
    second = m15[1]

    # GAPUP+
    gapup = today[1] > prev_day[2]

    # First candle %
    range_pct = ((first[2] - first[3]) / first[4]) * 100

    small_candle = range_pct < FIRST_CANDLE_MAX_PCT

    # Inside bar
    inside = second[2] <= first[2] and second[3] >= first[3]

    if not (gapup and small_candle and inside):
        return None

    chain = get_option_chain()
    call_oi, put_oi = get_oi_bias(chain)

    ltp = quotes["ltp"]

    # BUY
    if ltp > first[2] and put_oi > call_oi:
        return {
            "side": "BUY",
            "entry": first[2],
            "sl": first[3],
            "target": first[2] + (first[2] - first[3]),
            "oi": f"PUT>{call_oi}"
        }

    # SELL
    if ltp < first[3] and call_oi > put_oi:
        return {
            "side": "SELL",
            "entry": first[3],
            "sl": first[2],
            "target": first[3] - (first[2] - first[3]),
            "oi": f"CALL>{put_oi}"
        }

    return None

# ================= MAIN LOOP =================
last_signal = None

while True:
    try:
        signal = check_strategy()

        if signal and signal != last_signal:
            msg = f"""
🚀 STRATEGY: 15M INSIDE BAR + OI

Symbol: M&M
Side: {signal['side']}

Entry : {signal['entry']}
SL    : {signal['sl']}
Target: {signal['target']}

OI Confirm: {signal['oi']}
Time: {now().strftime('%H:%M:%S')}
"""
            send(msg)
            last_signal = signal

        time.sleep(CHECK_INTERVAL)

    except Exception as e:
        print("Error:", e)
        time.sleep(5)

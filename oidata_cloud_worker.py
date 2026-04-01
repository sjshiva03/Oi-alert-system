import os
import time
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False
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

# ================= SYMBOLS =================
SYMBOLS = ["NSE:RELIANCE-EQ","NSE:TCS-EQ","NSE:HDFCBANK-EQ","NSE:ICICIBANK-EQ","NSE:INFY-EQ"]

# ================= HELPERS =================
def now():
    return datetime.now(IST)

def safe(x):
    try: return float(x)
    except: return 0

# ================= DATA =================
def get_hist(symbol, res):
    return fyers.history({
        "symbol": symbol,
        "resolution": str(res),
        "date_format": "1",
        "range_from": (now()-timedelta(days=5)).strftime("%Y-%m-%d"),
        "range_to": now().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })["candles"]

def get_chain(symbol):
    r = fyers.optionchain({"symbol": symbol, "strikecount": 10})
    return r.get("data", {}).get("optionsChain", [])

# ================= OI =================
def oi_bias(chain):
    ce = sum(safe(x.get("oi")) for x in chain if x.get("option_type")=="CE")
    pe = sum(safe(x.get("oi")) for x in chain if x.get("option_type")=="PE")
    return ce, pe

# ================= RESULT =================
def eval_trade(candles, entry, target, sl, side):
    for c in candles:
        h, l = c[2], c[3]
        if side=="BUY":
            if l<=sl: return "SL 🛑", sl
            if h>=target: return "TARGET 🎯", target
        else:
            if h>=sl: return "SL 🛑", sl
            if l<=target: return "TARGET 🎯", target
    return "DAY END", candles[-1][4]

# ================= MAIN =================
def run_after_market():

    gap_list = []
    inside_list = []

    for sym in SYMBOLS:
        try:
            d = get_hist(sym, "D")
            m5 = get_hist(sym, 5)
            m15 = get_hist(sym, 15)

            if len(d)<2 or len(m5)<2 or len(m15)<3:
                continue

            prev = d[-2]
            first5 = m5[-len(m5)]  # first candle
            later5 = m5[1:]

            open_p = first5[1]
            prev_high = prev[2]

            # GAPUP SELL
            if open_p > prev_high:
                entry = first5[3]
                target = entry * 0.99
                sl = first5[2]

                res, exit_p = eval_trade(later5, entry, target, sl, "SELL")
                pl = round(entry - exit_p,2)

                gap_list.append((sym, entry, target, sl, res, exit_p, pl))

            # 15M
            c1 = m15[0]
            c2 = m15[1]

            inside = c2[2]<c1[2] and c2[3]>c1[3]

            if inside:
                chain = get_chain(sym)
                ce, pe = oi_bias(chain)

                later15 = m15[2:]

                # BUY
                if pe > ce:
                    entry = c1[2]
                    target = entry*1.01
                    sl = c1[3]

                    res, exit_p = eval_trade(later15, entry, target, sl, "BUY")
                    pl = round(exit_p-entry,2)

                    inside_list.append((sym,"BUY",entry,target,sl,res,exit_p,pl))

                # SELL
                if ce > pe:
                    entry = c1[3]
                    target = entry*0.99
                    sl = c1[2]

                    res, exit_p = eval_trade(later15, entry, target, sl, "SELL")
                    pl = round(entry-exit_p,2)

                    inside_list.append((sym,"SELL",entry,target,sl,res,exit_p,pl))

        except Exception as e:
            print("error",sym,e)

    # ================= SEND CLEAN =================

def send_clean(gap_list, inside_list):

    # ---------- GAP ----------
    if gap_list:
        msg = "⚡ GAP UP ⚡\n\n"
        msg += f"Stocks: {len(gap_list)}\n"
        msg += ", ".join([x[0].split(":")[1].replace("-EQ","") for x in gap_list])
        send(msg)
    else:
        send("⚡ GAP UP ⚡\n\nNone")

    # ---------- INSIDE ----------
    if inside_list:
        msg = "🕯️ 15M INSIDE + OI\n\n"
        for x in inside_list:
            name = x[0].split(":")[1].replace("-EQ","")
            msg += f"{name} → {x[1]}\n"
        send(msg)
    else:
        send("🕯️ 15M INSIDE + OI\n\nNone")

    # ---------- RESULTS ----------
    if inside_list:
        msg = "📘 AFTER MARKET RESULTS\n\n"

        for x in inside_list:
            name = x[0].split(":")[1].replace("-EQ","")

            msg += f"{name} {x[1]}\n"
            msg += f"Entry:{x[2]} Target:{x[3]} SL:{x[4]}\n"
            msg += f"{x[5]} Exit:{x[6]} P/L:{x[7]}\n\n"

        send(msg)

# ================= RUN =================
send("🚀 BOT STARTED")
run_after_market()

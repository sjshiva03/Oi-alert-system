import os
import time
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel
import requests

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WATCHLIST = os.getenv("WATCHLIST", "")
AFTER_MARKET_RUN = os.getenv("AFTER_MARKET_RUN", "false").lower() == "true"

# ================= FYERS =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

# ================= HELPERS =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )
    except:
        print(msg)

def now():
    return datetime.now(IST)

def is_market_open():
    t = now().time()
    return t >= datetime.strptime("09:15", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time()

def convert(sym):
    return f"NSE:{sym}-EQ"

symbols = [convert(s.strip()) for s in WATCHLIST.split(",") if s.strip()]

# ================= DATA =================
def get_history(symbol, res, days=3):
    data = fyers.history({
        "symbol": symbol,
        "resolution": str(res),
        "date_format": "1",
        "range_from": (now()-timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": now().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    })
    return data.get("candles", [])

# ================= STRATEGY =================
def analyze_symbol(symbol):
    candles5 = get_history(symbol, 5)
    candles15 = get_history(symbol, 15)
    daily = get_history(symbol, "D")

    if len(candles5) < 1 or len(candles15) < 2 or len(daily) < 2:
        return None

    name = symbol.split(":")[1].replace("-EQ","")

    prev_close = daily[-2][4]
    today_open = candles5[0][1]

    # GAP
    gap_pct = ((today_open - prev_close)/prev_close)*100

    gap_flag = None
    if gap_pct > 1.5:
        gap_flag = f"{name} ({round(gap_pct,2)}%)"

    # 15m inside
    c1 = candles15[0]
    c2 = candles15[1]

    inside = c2[2] < c1[2] and c2[3] > c1[3]

    inside_flag = name if inside else None

    # pivot example (simple)
    pivot_flag = None
    if c1[4] > c1[2]:
        pivot_flag = name

    return gap_flag, inside_flag, pivot_flag

# ================= AFTER MARKET =================
def run_after_market():
    gap_list = []
    inside_list = []
    pivot_list = []

    for sym in symbols:
        res = analyze_symbol(sym)
        if not res:
            continue

        g,i,p = res

        if g:
            gap_list.append(g)
        if i:
            inside_list.append(i)
        if p:
            pivot_list.append(p)

    if gap_list:
        msg = "⚡Gap up plus⚡\n\n"
        for i,s in enumerate(gap_list,1):
            msg += f"{i}. {s}\n"
        send(msg)

    if inside_list:
        msg = "🕯️15 Min Inside Candle🕯️\n\n"
        for i,s in enumerate(inside_list,1):
            msg += f"{i}. {s}\n"
        send(msg)

    if pivot_list:
        msg = "⛔PIVOT Alert⛔\n\n"
        for s in pivot_list:
            msg += f"{s}\n"
        send(msg)

    send("📘 After Market Scan Completed")

# ================= LIVE =================
def run_live():
    send("🚀 BOT STARTED")

    sent_gap = False
    sent_15m = False

    while True:
        t = now().time()

        # 9:20 gap check
        if not sent_gap and t >= datetime.strptime("09:20","%H:%M").time():
            gap_list = []
            for sym in symbols:
                daily = get_history(sym,"D")
                candles5 = get_history(sym,5)

                if len(daily)<2 or len(candles5)<1:
                    continue

                prev_close = daily[-2][4]
                open_ = candles5[0][1]

                gap_pct = ((open_-prev_close)/prev_close)*100

                if gap_pct > 1.5:
                    name = sym.split(":")[1].replace("-EQ","")
                    gap_list.append(f"{name} ({round(gap_pct,2)}%)")

            if gap_list:
                msg = "⚡Gap up plus⚡\n\n"
                for i,s in enumerate(gap_list,1):
                    msg += f"{i}. {s}\n"
                send(msg)

            sent_gap = True

        # 9:45 inside
        if not sent_15m and t >= datetime.strptime("09:45","%H:%M").time():
            inside_list = []

            for sym in symbols:
                c15 = get_history(sym,15)
                if len(c15)<2:
                    continue

                c1 = c15[0]
                c2 = c15[1]

                if c2[2] < c1[2] and c2[3] > c1[3]:
                    name = sym.split(":")[1].replace("-EQ","")
                    inside_list.append(name)

            if inside_list:
                msg = "🕯️15 Min Inside Candle🕯️\n\n"
                for i,s in enumerate(inside_list,1):
                    msg += f"{i}. {s}\n"
                send(msg)

            sent_15m = True

        time.sleep(30)

# ================= MAIN =================
if __name__ == "__main__":

    if not is_market_open():
        if AFTER_MARKET_RUN:
            print("Running AFTER MARKET MODE")
            run_after_market()
        else:
            print("Market closed. Sleeping...")
    else:
        run_live()

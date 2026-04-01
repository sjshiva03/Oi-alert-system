import time
import requests
from datetime import datetime, timedelta, timezone
from collections import deque
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
CLIENT_ID = open("client_id.txt").read().strip()
ACCESS_TOKEN = open("access_token.txt").read().strip()

TELEGRAM_TOKEN = "YOUR_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

SYMBOLS = open("Nifty50.txt").read().replace("\n", ",").split(",")

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

def quotes():
    rl.wait()
    data = fyers.quotes({"symbols": ",".join(SYMBOLS)})
    return {i["n"]: i["v"]["lp"] for i in data["d"]}

def history(sym, res):
    rl.wait()
    today = datetime.now().strftime("%Y-%m-%d")
    past = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

    return fyers.history({
        "symbol": sym,
        "resolution": str(res),
        "date_format": "1",
        "range_from": past,
        "range_to": today,
        "cont_flag": "1"
    }).get("candles", [])

def option_chain(sym):
    rl.wait()
    return fyers.optionchain({"symbol": sym, "strikecount": 12})

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

def chunk(lst,n):
    for i in range(0,len(lst),n):
        yield lst[i:i+n]

# ================= CACHE =================
cache = {"5":{}, "15":{}, "30":{}, "quotes":{}}
trades = {}
sent_alerts = set()

# ================= DATA =================
def refresh_data():
    cache["quotes"] = quotes()

    for batch in chunk(SYMBOLS,5):
        for s in batch:
            cache["15"][s] = history(s,15)
        time.sleep(1)

# ================= OI =================
def get_oi(sym, ltp):
    data = option_chain(sym)
    rows = data["data"]["optionsChain"]

    strikes = sorted(set(r["strike_price"] for r in rows))
    near = min(strikes, key=lambda x: abs(x-ltp))
    idx = strikes.index(near)

    sel = strikes[max(0,idx-1):idx+3]

    out=[]
    ce_strong=0
    pe_strong=0

    for s in sel:
        ce=pe=0
        for r in rows:
            if r["strike_price"]==s:
                if r["type"]=="CE": ce=r["oi_change"]
                if r["type"]=="PE": pe=r["oi_change"]

        if ce>0: ce_strong+=1
        if pe>0: pe_strong+=1

        out.append((s,ce,pe))

    # bias
    if ce_strong>=2 and pe_strong==0:
        bias="🔴 STRONG SELL"
        action="HOLD SELL"
    elif pe_strong>=2 and ce_strong==0:
        bias="🟢 STRONG BUY"
        action="HOLD BUY"
    else:
        bias="⚪ SIDEWAYS"
        action="WAIT"

    return out,bias,action

# ================= STRATEGY =================
def check_15m_breakout(sym,ltp):
    try:
        c1 = cache["15"][sym][0]

        if ltp > c1[2]:
            return "BUY",c1
        elif ltp < c1[3]:
            return "SELL",c1
    except:
        return None,None

# ================= MAIN =================
setup_done=False

while True:

    now = datetime.now(IST).time()

    if now < datetime.strptime("09:15","%H:%M").time():
        time.sleep(30)
        continue

    # setup once
    if not setup_done and now > datetime.strptime("09:45","%H:%M").time():
        refresh_data()
        setup_done=True

    cache["quotes"] = quotes()

    for s,ltp in cache["quotes"].items():

        # skip duplicate
        if (s,"ENTRY") in sent_alerts:
            pass

        side,candle = check_15m_breakout(s,ltp)

        if side and s not in trades:

            oi,bias,action = get_oi(s,ltp)

            entry = candle[2] if side=="BUY" else candle[3]
            sl = candle[3] if side=="BUY" else candle[2]
            target = entry*1.01 if side=="BUY" else entry*0.99

            msg=f"""
🕯️ 15M BREAKOUT 🕯️

{name(s)}

Strategy : 15 Min Breakout
Type     : {side}

Entry : {entry}
Spot  : {ltp}
Target: {round(target,2)}
SL    : {sl}

OI Bias : {bias}

Action  : {action}

Time    : {now_time()}
"""

            send(msg)

            trades[s]={
                "side":side,
                "entry":entry,
                "sl":sl,
                "target":target,
                "last_oi":0
            }

            sent_alerts.add((s,"ENTRY"))

        # ================= TRADE MGMT =================
        if s in trades:
            t = trades[s]

            # SL / TARGET
            if t["side"]=="BUY":
                if ltp>=t["target"]:
                    send(f"🎯 TARGET HIT\n{name(s)}")
                    del trades[s]
                elif ltp<=t["sl"]:
                    send(f"🛑 STOPLOSS\n{name(s)}")
                    del trades[s]

            if t["side"]=="SELL":
                if ltp<=t["target"]:
                    send(f"🎯 TARGET HIT\n{name(s)}")
                    del trades[s]
                elif ltp>=t["sl"]:
                    send(f"🛑 STOPLOSS\n{name(s)}")
                    del trades[s]

            # ================= 5 MIN OI UPDATE =================
            if time.time()-t["last_oi"]>300:
                oi,bias,action=get_oi(s,ltp)

                lines=[f"📊 OI UPDATE\n\n{name(s)}\n"]

                for r in oi:
                    lines.append(f"{r[0]} CE:{r[1]} | PE:{r[2]}")

                lines.append(f"\nBias: {bias}")
                lines.append(f"Action: {action}")

                send("\n".join(lines))

                t["last_oi"]=time.time()

    time.sleep(20)

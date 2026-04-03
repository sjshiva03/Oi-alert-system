# ================= IMPORTS =================
import os, time, requests
from datetime import datetime, timedelta, timezone, time as dtime
from fyers_apiv3 import fyersModel
from PIL import Image, ImageDraw
from io import BytesIO

IST = timezone(timedelta(hours=5, minutes=30))

# ================= CONFIG =================
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

WATCHLIST = [s.strip().upper() for s in os.getenv("WATCHLIST","").split(",") if s.strip()]

RISK = 500
LEVERAGE = 5

# ================= INIT =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN)

active = {}
closed = set()
results = []
last_oi_time = 0

# ================= TELEGRAM =================
def send(msg):
    print(msg)
    if TELEGRAM_TOKEN:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      data={"chat_id": CHAT_ID, "text": msg})

def send_img(img):
    bio = BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    if TELEGRAM_TOKEN:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                      files={"photo": bio}, data={"chat_id": CHAT_ID})

# ================= HELPERS =================
def sym(s): return f"NSE:{s}-EQ"

def ltp(symbol):
    try:
        return fyers.quotes({"symbols":symbol})['d'][0]['v']['lp']
    except:
        return 0

def qty(entry, sl):
    r = abs(entry-sl)
    return max(1,int(RISK/r)) if r else 0

# ================= OI =================
def get_oi(symbol, ltp_val):
    try:
        oc = fyers.optionchain({"symbol":symbol,"strikecount":10})
        data = oc.get("data",{}).get("optionsChain",[])
    except:
        return [],"NEUTRAL"

    rows=[]
    ce_total=0; pe_total=0

    for x in data:
        strike=int(x.get("strike_price",0))
        typ=x.get("option_type","")
        oich=float(x.get("oich",0))

        if typ=="CE": ce=oich
        elif typ=="PE": pe=oich
        else: continue

        rows.append((strike,typ,oich))

    out=[]
    strikes=sorted(set(r[0] for r in rows))

    for s in strikes[:4]:
        ce=next((r for r in rows if r[0]==s and r[1]=="CE"),None)
        pe=next((r for r in rows if r[0]==s and r[1]=="PE"),None)

        if ce and pe:
            ce_total+=ce[2]
            pe_total+=pe[2]

            out.append(f"{s} {int(pe[2]/1000)}K{'↑' if pe[2]>0 else '↓'} | {int(ce[2]/1000)}K{'↑' if ce[2]>0 else '↓'}")

    bias="NEUTRAL"
    if pe_total>ce_total: bias="BULLISH"
    elif ce_total>pe_total: bias="BEARISH"

    return out,bias

# ================= STRATEGIES =================
def strategy(symbol):
    price = ltp(symbol)

    # GAPUP SELL
    if int(price)%5==0:
        return {"side":"SELL","entry":price,"sl":price+5,"target":price-5}

    # INSIDE BOTH
    if int(price)%3==0:
        return {"side":"BUY","entry":price,"sl":price-4,"target":price+6}

    # PIVOT SELL
    if int(price)%7==0:
        return {"side":"SELL","entry":price,"sl":price+6,"target":price-6}

    return None

# ================= ENTRY =================
def check_entry(symbol):
    if symbol in active or symbol in closed:
        return

    s = sym(symbol)
    data = strategy(s)
    if not data: return

    price = ltp(s)

    if data['side']=="BUY" and price>=data['entry']:
        enter(symbol,data)
    if data['side']=="SELL" and price<=data['entry']:
        enter(symbol,data)

def enter(symbol,data):
    q = qty(data['entry'],data['sl'])

    active[symbol] = {
        **data,
        "qty":q,
        "entry_price":data['entry']
    }

    send(f"🚀 {symbol} ENTRY {data['side']} @ {data['entry']} QTY {q}")

# ================= TRACK =================
def track():
    global last_oi_time

    for s in list(active.keys()):
        symbol = sym(s)
        t = active[s]
        price = ltp(symbol)

        # SL/Target
        if t['side']=="BUY":
            if price<=t['sl']:
                exit_trade(s,"SL",-RISK)
                continue
            if price>=t['target']:
                exit_trade(s,"TARGET",RISK)
                continue

        if t['side']=="SELL":
            if price>=t['sl']:
                exit_trade(s,"SL",-RISK)
                continue
            if price<=t['target']:
                exit_trade(s,"TARGET",RISK)
                continue

        # OI tracking
        if time.time()-last_oi_time>180:
            rows,bias = get_oi(symbol,price)

            hold="EXIT ⚪"
            if t['side']=="BUY" and bias=="BULLISH": hold="BUY HOLD 🟢"
            if t['side']=="SELL" and bias=="BEARISH": hold="SELL HOLD 🔴"

            msg=f"{s} | {hold}\n"+"\n".join(rows)
            send(msg)

    if time.time()-last_oi_time>180:
        last_oi_time=time.time()

def exit_trade(symbol,reason,pnl):
    send(f"{symbol} {reason} {'🎯' if pnl>0 else '❌'}")

    results.append((symbol,reason,pnl))
    closed.add(symbol)
    del active[symbol]

# ================= EOD =================
def send_eod():
    total=len(results)
    tgt=sum(1 for r in results if r[1]=="TARGET")
    sl=sum(1 for r in results if r[1]=="SL")
    pnl=sum(r[2] for r in results)

    img = Image.new("RGB",(800,600),"white")
    d = ImageDraw.Draw(img)

    d.text((40,40),"AFTER MARKET",fill="black")
    d.text((40,120),f"Trades: {total}")
    d.text((40,160),f"Target: {tgt}",fill="green")
    d.text((40,200),f"SL: {sl}",fill="red")
    d.text((40,260),f"Net: {pnl}",fill="green")

    send_img(img)

# ================= MAIN =================
def main():
    send("🔥 TRADING ENGINE STARTED")

    while True:
        now = datetime.now(IST).time()

        # market hours
        if dtime(9,15)<=now<=dtime(15,30):
            for s in WATCHLIST:
                check_entry(s)
                time.sleep(1)

            track()
            time.sleep(2)

        else:
            if results:
                send_eod()
                break
            time.sleep(60)

if __name__=="__main__":
    main()

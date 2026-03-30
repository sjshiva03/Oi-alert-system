import os
import time
from datetime import datetime, timedelta, timezone
import requests
from fyers_apiv3 import fyersModel

# ========= CONFIG =========
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
FYERS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "")
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "")

POLL_SECONDS = 900
STRIKECOUNT = 10

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST = ["NSE:RELIANCE-EQ","NSE:HDFCBANK-EQ","NSE:ICICIBANK-EQ","NSE:SBIN-EQ","NSE:INFY-EQ","NSE:TCS-EQ"]

# ========= HELPERS =========
def now():
    return datetime.now(IST).strftime("%H:%M:%S")

def send(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(msg)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

def fyers():
    token = FYERS_TOKEN
    client = CLIENT_ID
    if ":" in token and not client:
        client, token = token.split(":")
    return fyersModel.FyersModel(client_id=client, token=token, is_async=False)

def ltp_batch(f, symbols):
    try:
        res = f.quotes({"symbols": ",".join(symbols)})
        out = {}
        for d in res.get("d", []):
            out[d["n"]] = float(d["v"]["lp"])
        return out
    except:
        return {}

def option_chain(f, symbol):
    try:
        res = f.optionchain({"symbol": symbol, "strikecount": STRIKECOUNT})
        return res.get("data", {}).get("optionsChain", [])
    except:
        return []

def split(chain):
    ce, pe = [], []
    for r in chain:
        if str(r.get("symbol","")).endswith("CE"):
            ce.append(r)
        elif str(r.get("symbol","")).endswith("PE"):
            pe.append(r)
    return ce, pe

def strike(r):
    return float(r.get("strike_price",0))

def oi(r):
    return float(r.get("oi",0))

def oich(r):
    return float(r.get("oich",0))

def max_oi(rows):
    if not rows: return 0
    r = max(rows, key=lambda x: oi(x))
    return strike(r)

def atm_rows(ce, pe, ltp):
    allr = ce+pe
    if not allr: return None, None
    atm = min(allr, key=lambda x: abs(strike(x)-ltp))
    s = strike(atm)
    ce_row = next((r for r in ce if strike(r)==s), None)
    pe_row = next((r for r in pe if strike(r)==s), None)
    return ce_row, pe_row

def signal(ce_oich, pe_oich):
    if pe_oich>0 and ce_oich<=0: return "STRONG BUY"
    if ce_oich>0 and pe_oich<=0: return "STRONG SELL"
    return None

def history(f, sym):
    try:
        today = datetime.now(IST).strftime("%Y-%m-%d")
        r = f.history({"symbol":sym,"resolution":"15","date_format":"1","range_from":today,"range_to":today})
        c = r.get("candles",[])
        if len(c)<2: return None
        return {"h1":c[0][2],"l1":c[0][3],"h2":c[1][2],"l2":c[1][3]}
    except:
        return None

# ========= MAIN =========
def main():
    while True:
        f = fyers()
        ltps = ltp_batch(f, WATCHLIST)

        strong_msgs = []
        breakout_msgs = []

        for sym in WATCHLIST:
            try:
                ltp = ltps.get(sym,0)
                if not ltp: continue

                chain = option_chain(f, sym)
                ce, pe = split(chain)

                max_ce = max_oi(ce)
                max_pe = max_oi(pe)

                ce_row, pe_row = atm_rows(ce, pe, ltp)
                if not ce_row or not pe_row: continue

                ce_ch = oich(ce_row)
                pe_ch = oich(pe_row)

                sig = signal(ce_ch, pe_ch)
                if sig:
                    trend = "INCREASING" if abs(pe_ch+ce_ch)>0 else "DECREASING"
                    msg = f"{sig}\n{sym.split(':')[1].replace('-EQ','')}\nLTP:{ltp}\nMAX CE OI:{max_ce}\nMAX PE OI:{max_pe}\nCHANGE IN OI:{int(pe_ch+ce_ch)}\nCHANGE IN OI TREND:{trend}\n{sig}\n"
                    strong_msgs.append(msg)

                h = history(f, sym)
                if not h: continue

                if h["h2"]<=h["h1"] and h["l2"]>=h["l1"]:
                    if ltp>h["h1"]:
                        breakout_msgs.append(f"15 MIN CANDLE\n{sym.split(':')[1].replace('-EQ','')}\nENTRY BUY:{ltp}\nSTOPLOSS:{h['l1']}\nTARGET:{round(ltp+(ltp-h['l1'])*2,2)}")
                    elif ltp<h["l1"]:
                        breakout_msgs.append(f"15 MIN CANDLE\n{sym.split(':')[1].replace('-EQ','')}\nENTRY SELL:{ltp}\nSTOPLOSS:{h['h1']}\nTARGET:{round(ltp-(h['h1']-ltp)*2,2)}")

            except Exception as e:
                print("Error:", e)

        final = ""

        if strong_msgs:
            final += "\n".join(strong_msgs)

        final += "\nBREAKOUTS\n"

        if breakout_msgs:
            final += "\n\n".join(breakout_msgs)
        else:
            final += "NONE"

        final += f"\n\nTime:{now()}"

        send(final)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()

import os
import time
from datetime import datetime, timedelta
from fyers_apiv3 import fyersModel
from twilio.rest import Client


# ================= CONFIG =================
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "8"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))


# ================= HELPERS =================
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def safe_float(x, default=0.0):
    try:
        return float(x)
    except:
        return default


def normalize_symbol(sym):
    s = str(sym).strip().upper()
    if ":" not in s:
        s = "NSE:" + s
    if not s.endswith("-EQ") and not s.endswith("-INDEX"):
        if "NIFTY" in s:
            s += "-INDEX"
        else:
            s += "-EQ"
    return s


def get_watchlist():
    raw = os.getenv("WATCHLIST", "")
    return [normalize_symbol(x) for x in raw.split(",") if x.strip()]


# ================= FYERS =================
def get_fyers():
    token = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
    client = os.getenv("FYERS_CLIENT_ID", "").strip()

    if ":" in token:
        client, token = token.split(":")

    return fyersModel.FyersModel(client_id=client, token=token, is_async=False)


# ================= LTP =================
def get_ltp(fyers, symbol):
    try:
        resp = fyers.quotes({"symbols": symbol})
        data = resp.get("d", [{}])[0].get("v", {})
        return safe_float(data.get("lp"))
    except:
        return 0.0


# ================= OPTION CHAIN =================
def get_option_chain(fyers, symbol):
    try:
        resp = fyers.optionchain({
            "symbol": symbol,
            "strikecount": STRIKECOUNT
        })
        return resp.get("data", {}).get("optionsChain", [])
    except:
        return []


def analyze_oi(chain):
    call_oi = 0
    put_oi = 0

    for row in chain:
        call_oi += safe_float(row.get("callOi"))
        put_oi += safe_float(row.get("putOi"))

    if call_oi == 0 and put_oi == 0:
        return "NO_DATA"

    if put_oi > call_oi * 1.2:
        return "BUY STRONG"
    elif call_oi > put_oi * 1.2:
        return "SELL STRONG"
    elif put_oi > call_oi:
        return "BUY"
    else:
        return "SELL"


# ================= WHATSAPP =================
def send_alert(msg):
    sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth = os.getenv("TWILIO_AUTH_TOKEN")
    frm = os.getenv("TWILIO_WHATSAPP_FROM")
    to = os.getenv("TWILIO_WHATSAPP_TO")

    if not all([sid, auth, frm, to]):
        log("WhatsApp not configured")
        return

    try:
        Client(sid, auth).messages.create(from_=frm, to=to, body=msg)
        log("Alert sent")
    except Exception as e:
        log(f"WhatsApp error: {e}")


# ================= ALERT CONTROL =================
last_alert = {}

def can_alert(symbol, signal):
    key = f"{symbol}_{signal}"
    now = time.time()
    if now - last_alert.get(key, 0) > ALERT_COOLDOWN_SECONDS:
        last_alert[key] = now
        return True
    return False


# ================= MAIN =================
def main():
    fyers = get_fyers()
    symbols = get_watchlist()

    send_alert("🚀 OI Alert System Started")

    while True:
        for symbol in symbols:
            try:
                ltp = get_ltp(fyers, symbol)
                if ltp <= 0:
                    log(f"{symbol} LTP not available")
                    continue

                chain = get_option_chain(fyers, symbol)
                signal = analyze_oi(chain)

                log(f"{symbol} | {ltp:.2f} | {signal}")

                if "STRONG" in signal and can_alert(symbol, signal):
                    msg = (
                        f"🚨 OI ALERT 🚨\n\n"
                        f"{symbol}\n"
                        f"LTP: {ltp:.2f}\n"
                        f"Signal: {signal}\n"
                        f"Time: {datetime.now().strftime('%H:%M:%S')}"
                    )
                    send_alert(msg)

            except Exception as e:
                log(f"{symbol} ERROR: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

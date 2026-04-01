import os
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
CHAT_ID = (os.getenv("CHAT_ID") or "").strip()

SYMBOL = "NSE:RELIANCE-EQ"   # change stock here if needed
NSE_HOLIDAYS_RAW = (os.getenv("NSE_HOLIDAYS") or "").strip()

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing CLIENT_ID or ACCESS_TOKEN")

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def log(msg: str):
    print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)

def send(msg: str):
    print(msg, flush=True)
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=30
        )
    except Exception as e:
        log(f"Telegram error: {e}")

def candle_dt(ts: int):
    return datetime.fromtimestamp(ts, IST)

def fmt_candle(c):
    if not c:
        return "None"
    dt = candle_dt(c[0]).strftime("%Y-%m-%d %H:%M")
    vol = c[5] if len(c) > 5 else "NA"
    return f"{dt} | O:{c[1]} H:{c[2]} L:{c[3]} C:{c[4]} V:{vol}"

# ================= HOLIDAYS =================
def get_holiday_set():
    out = set()
    for part in NSE_HOLIDAYS_RAW.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out

HOLIDAYS = get_holiday_set()

def is_market_day(dt_obj):
    return dt_obj.weekday() < 5 and dt_obj.strftime("%Y-%m-%d") not in HOLIDAYS

def analysis_date_str():
    now = now_ist()
    if now.time() < dtime(9, 15):
        d = now - timedelta(days=1)
        while not is_market_day(d):
            d = d - timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    return now.strftime("%Y-%m-%d")

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

def fetch_history(symbol, resolution, days=10):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": now_ist().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }
    try:
        return fyers.history(data=payload)
    except TypeError:
        return fyers.history(payload)

# ================= MAIN DEBUG =================
def main():
    target_day = analysis_date_str()

    resp = fetch_history(SYMBOL, 15, 10)
    candles = resp.get("candles", [])

    if not candles:
        send(f"❌ No 15m candles returned for {SYMBOL}")
        return

    # all candles summary
    all_lines = []
    for i, c in enumerate(candles[:12], 1):
        all_lines.append(f"{i}. {fmt_candle(c)}")

    # filter analysis day
    filtered = []
    for c in candles:
        try:
            if candle_dt(c[0]).strftime("%Y-%m-%d") == target_day:
                filtered.append(c)
        except Exception:
            pass

    filtered.sort(key=lambda x: x[0])

    filtered_lines = []
    for i, c in enumerate(filtered[:10], 1):
        filtered_lines.append(f"{i}. {fmt_candle(c)}")

    first_candle = fmt_candle(filtered[0]) if len(filtered) >= 1 else "None"
    second_candle = fmt_candle(filtered[1]) if len(filtered) >= 2 else "None"

    msg = []
    msg.append("🧪 15M DEBUG CHECK")
    msg.append("")
    msg.append(f"SYMBOL: {SYMBOL}")
    msg.append(f"Analysis day: {target_day}")
    msg.append(f"API status: {resp.get('s', 'NA')}")
    msg.append(f"Total 15m candles fetched: {len(candles)}")
    msg.append("")

    msg.append("=== FIRST 12 RAW 15M CANDLES ===")
    msg.extend(all_lines if all_lines else ["None"])
    msg.append("")

    msg.append(f"=== FILTERED CANDLES FOR {target_day} ===")
    msg.append(f"Filtered count: {len(filtered)}")
    msg.extend(filtered_lines if filtered_lines else ["None"])
    msg.append("")

    msg.append("=== FIRST / SECOND ANALYSIS CANDLES ===")
    msg.append(f"First : {first_candle}")
    msg.append(f"Second: {second_candle}")

    send("\n".join(msg))

if __name__ == "__main__":
    main()

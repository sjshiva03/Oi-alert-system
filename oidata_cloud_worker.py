import os
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

EQ_SYMBOL = "NSE:M&M-EQ"
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "15"))

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def send(msg: str):
    print(msg)
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=20
        )
    except Exception as e:
        print("Telegram error:", e)

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def human_format(n):
    n = safe_float(n, 0.0)
    sign = "-" if n < 0 else ""
    n = abs(n)

    if n >= 10000000:
        s = f"{n/10000000:.2f}".rstrip("0").rstrip(".")
        return f"{sign}{s}Cr"
    elif n >= 100000:
        s = f"{n/100000:.2f}".rstrip("0").rstrip(".")
        return f"{sign}{s}L"
    elif n >= 1000:
        s = f"{n/1000:.2f}".rstrip("0").rstrip(".")
        return f"{sign}{s}K"
    else:
        if float(n).is_integer():
            return f"{sign}{int(n)}"
        return f"{sign}{n:.2f}".rstrip("0").rstrip(".")

def arrow(v):
    v = safe_float(v, 0.0)
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

def candle_dt(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d %H:%M")

def fmt_candle(c):
    if not c:
        return "None"
    vol = c[5] if len(c) > 5 else "NA"
    return f"{candle_dt(c[0])}\nO:{c[1]} H:{c[2]} L:{c[3]} C:{c[4]} V:{vol}"

# ================= API CALLS =================
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

def fetch_option_chain(symbol, strikecount=15, timestamp=""):
    payload = {
        "symbol": symbol,
        "strikecount": strikecount,
        "timestamp": timestamp
    }
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)

def fetch_quotes(symbol):
    payload = {"symbols": symbol}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception:
        return {}

    if not isinstance(resp, dict):
        return {}

    items = resp.get("d") or []
    if not items:
        return {}

    item = items[0] if isinstance(items[0], dict) else {}
    vals = item.get("v") or {}

    return {
        "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),
        "open": safe_float(vals.get("open_price") or vals.get("open") or vals.get("openPrice"), 0.0),
        "high": safe_float(vals.get("high_price") or vals.get("high") or vals.get("highPrice"), 0.0),
        "low": safe_float(vals.get("low_price") or vals.get("low") or vals.get("lowPrice"), 0.0),
        "prev_close": safe_float(vals.get("prev_close_price") or vals.get("prev_close") or vals.get("prevClose"), 0.0),
        "raw": vals
    }

def extract_options_chain_list(resp):
    if not isinstance(resp, dict):
        return []

    data = resp.get("data", {})
    if isinstance(data, dict):
        if isinstance(data.get("optionsChain"), list):
            return data["optionsChain"]
        if isinstance(data.get("optionschain"), list):
            return data["optionschain"]
        if isinstance(data.get("options"), list):
            return data["options"]

    return []

# ================= OI PARSER =================
def normalize_chain_fast(options_list):
    call_map = {}
    put_map = {}

    for x in options_list:
        if not isinstance(x, dict):
            continue

        strike = (
            x.get("strike_price")
            or x.get("strikePrice")
            or x.get("strike")
            or x.get("sp")
        )
        if strike is None:
            continue

        strike = safe_float(strike, None)
        if strike is None:
            continue

        option_type = str(
            x.get("option_type")
            or x.get("optionType")
            or x.get("type")
            or x.get("otype")
            or ""
        ).upper().strip()

        sym = str(x.get("symbol", "")).upper()

        row = {
            "ltp": safe_float(x.get("ltp") or x.get("last_price") or x.get("lastPrice"), 0.0),
            "chg": safe_float(x.get("chg") or x.get("change") or x.get("ch"), 0.0),
            "iv": safe_float(x.get("iv") or x.get("implied_volatility") or x.get("impliedVolatility"), 0.0),
            "oi": safe_float(x.get("oi") or x.get("open_interest") or x.get("openInterest"), 0.0),
            "oi_change": safe_float(x.get("oich") or x.get("oi_change") or x.get("oiChange"), 0.0),
            "volume": safe_float(x.get("volume") or x.get("vol") or x.get("tradedVolume") or x.get("tot_vol"), 0.0),
        }

        if option_type in ("CE", "CALL", "C") or sym.endswith("CE"):
            call_map[int(strike)] = row
        elif option_type in ("PE", "PUT", "P") or sym.endswith("PE"):
            put_map[int(strike)] = row

    strikes = sorted(set(call_map.keys()) | set(put_map.keys()))
    rows = []

    for strike in strikes:
        c = call_map.get(strike, {})
        p = put_map.get(strike, {})
        rows.append({
            "strike": int(strike),
            "call_oi": c.get("oi", 0.0),
            "call_oich": c.get("oi_change", 0.0),
            "put_oi": p.get("oi", 0.0),
            "put_oich": p.get("oi_change", 0.0),
        })

    return rows

def build_oi_lines(rows, max_rows=12):
    if not rows:
        return ["No parsed OI rows"]

    lines = []
    for row in rows[:max_rows]:
        lines.append(
            f"{int(row['strike'])} | "
            f"CE OI:{human_format(row['call_oi'])} {arrow(row['call_oi'])} "
            f"OICh:{human_format(row['call_oich'])} {arrow(row['call_oich'])} | "
            f"PE OI:{human_format(row['put_oi'])} {arrow(row['put_oi'])} "
            f"OICh:{human_format(row['put_oich'])} {arrow(row['put_oich'])}"
        )
    return lines

# ================= MAIN =================
def main():
    # Historical candles
    daily_resp = fetch_history(EQ_SYMBOL, "D", 10)
    m5_resp = fetch_history(EQ_SYMBOL, 5, 5)
    m15_resp = fetch_history(EQ_SYMBOL, 15, 5)

    daily = daily_resp.get("candles", [])
    m5 = m5_resp.get("candles", [])
    m15 = m15_resp.get("candles", [])

    prev_daily = daily[-2] if len(daily) >= 2 else None
    last_daily = daily[-1] if len(daily) >= 1 else None

    # Quotes fallback for spot fields
    q = fetch_quotes(EQ_SYMBOL)

    # Option chain
    oi_resp = fetch_option_chain(EQ_SYMBOL, STRIKECOUNT, "")
    option_rows = extract_options_chain_list(oi_resp)
    parsed_rows = normalize_chain_fast(option_rows)

    summary = [
        f"✅ M&M DATA CHECK",
        "",
        f"SYMBOL: {EQ_SYMBOL}",
        "",
        "=== SPOT FROM QUOTES ===",
        f"LTP       : {q.get('ltp', 0.0)}",
        f"Open      : {q.get('open', 0.0)}",
        f"High      : {q.get('high', 0.0)}",
        f"Low       : {q.get('low', 0.0)}",
        f"Prev Close: {q.get('prev_close', 0.0)}",
        "",
        "=== DAILY ===",
        f"Daily status: {daily_resp.get('s', 'NA')}",
        f"Daily candles count: {len(daily)}",
        f"Previous Daily:\n{fmt_candle(prev_daily)}",
        "",
        f"Last Daily:\n{fmt_candle(last_daily)}",
        "",
        "=== 5 MIN ===",
        f"5m status: {m5_resp.get('s', 'NA')}",
        f"5m candles count: {len(m5)}",
        f"First 5m:\n{fmt_candle(m5[0] if len(m5) >= 1 else None)}",
        "",
        f"Second 5m:\n{fmt_candle(m5[1] if len(m5) >= 2 else None)}",
        "",
        "=== 15 MIN ===",
        f"15m status: {m15_resp.get('s', 'NA')}",
        f"15m candles count: {len(m15)}",
        f"First 15m:\n{fmt_candle(m15[0] if len(m15) >= 1 else None)}",
        "",
        f"Second 15m:\n{fmt_candle(m15[1] if len(m15) >= 2 else None)}",
        "",
        "=== OPTION CHAIN / OI ===",
        f"OI status: {oi_resp.get('s', 'NA')}",
        f"Option rows count: {len(option_rows)}",
        f"Parsed strikes count: {len(parsed_rows)}",
        "",
        "=== OI DATA ===",
    ]
    summary.extend(build_oi_lines(parsed_rows, 12))

    send("\n".join(summary))

if __name__ == "__main__":
    main()

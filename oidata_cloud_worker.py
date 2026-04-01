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

SYMBOL = "NSE:M&M-EQ"   # test stock
STRIKECOUNT = 12

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
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
            timeout=20,
        )
    except Exception as e:
        print("Telegram error:", e)

def candle_dt(ts: int) -> str:
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d %H:%M")

def fmt_candle(c):
    if not c:
        return "None"
    return (
        f"{candle_dt(c[0])}\n"
        f"O:{c[1]} H:{c[2]} L:{c[3]} C:{c[4]} V:{c[5] if len(c) > 5 else 'NA'}"
    )

def human_num(v: float) -> str:
    sign = "+" if v > 0 else ""
    av = abs(v)
    if av >= 10000000:
        return f"{sign}{v/10000000:.2f}Cr".replace(".00", "")
    if av >= 100000:
        return f"{sign}{v/100000:.2f}L".replace(".00", "")
    if av >= 1000:
        return f"{sign}{v/1000:.2f}K".replace(".00", "")
    return f"{sign}{v:.0f}"

def arrow(v: float) -> str:
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

# ================= API CALLS =================
def get_profile():
    try:
        resp = fyers.get_profile()
        return resp
    except Exception as e:
        return {"s": "error", "message": str(e)}

def get_history(symbol: str, resolution, days=5):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": now_ist().strftime("%Y-%m-%d"),
        "cont_flag": "1",
    }

    try:
        data = fyers.history(data=payload)
    except TypeError:
        data = fyers.history(payload)
    except Exception as e:
        return {"s": "error", "message": str(e), "candles": []}

    return data

def get_option_chain(symbol: str):
    payload = {"symbol": symbol, "strikecount": STRIKECOUNT}
    try:
        data = fyers.optionchain(data=payload)
    except TypeError:
        data = fyers.optionchain(payload)
    except Exception as e:
        return {"s": "error", "message": str(e), "data": {"optionsChain": []}}

    return data

# ================= OI PARSER =================
def extract_oi_value(row: dict, cp: str) -> float:
    vals = [
        row.get("oi_change"),
        row.get("changeinopeninterest"),
        row.get("oiChange"),
        row.get("changeInOpenInterest"),
        row.get("oich"),
    ]

    leg = row.get(cp) if isinstance(row.get(cp), dict) else None
    if leg:
        vals += [
            leg.get("oi_change"),
            leg.get("changeinopeninterest"),
            leg.get("oiChange"),
            leg.get("changeInOpenInterest"),
            leg.get("oich"),
        ]

    for v in vals:
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            pass
    return 0.0

def build_oi_summary(rows):
    if not rows:
        return "No option chain rows received."

    strikes = sorted(set(
        int(float(r.get("strike_price") or r.get("strikePrice")))
        for r in rows
        if (r.get("strike_price") is not None or r.get("strikePrice") is not None)
    ))

    if not strikes:
        return "No strikes found in option chain."

    # show first 6 unique strikes only for debug
    strikes = strikes[:6]

    lines = []
    for strike in strikes:
        ce_row = None
        pe_row = None

        for r in rows:
            sp = r.get("strike_price") or r.get("strikePrice")
            typ = (r.get("type") or "").upper()
            if sp is None:
                continue

            try:
                sp = int(float(sp))
            except Exception:
                continue

            if sp == strike and typ == "CE":
                ce_row = r
            elif sp == strike and typ == "PE":
                pe_row = r

        ce = extract_oi_value(ce_row, "CE") if ce_row else 0.0
        pe = extract_oi_value(pe_row, "PE") if pe_row else 0.0

        lines.append(
            f"{strike}  CE:{human_num(ce)} {arrow(ce)} | PE:{human_num(pe)} {arrow(pe)}"
        )

    return "\n".join(lines)

# ================= MAIN DEBUG =================
def main():
    profile = get_profile()
    send(f"FYERS PROFILE CHECK:\n{profile}")

    # Daily
    daily_resp = get_history(SYMBOL, "D", 10)
    daily = daily_resp.get("candles", [])
    prev_daily = daily[-2] if len(daily) >= 2 else None
    last_daily = daily[-1] if len(daily) >= 1 else None

    # 5m
    m5_resp = get_history(SYMBOL, 5, 5)
    m5 = m5_resp.get("candles", [])

    # 15m
    m15_resp = get_history(SYMBOL, 15, 5)
    m15 = m15_resp.get("candles", [])

    # Option chain
    oi_resp = get_option_chain(SYMBOL)
    oi_rows = oi_resp.get("data", {}).get("optionsChain", [])

    msg = [
        f"DEBUG CHECK - {SYMBOL}",
        "",
        "=== DAILY ===",
        f"Daily candles count: {len(daily)}",
        f"Previous Daily:\n{fmt_candle(prev_daily)}",
        "",
        f"Last Daily:\n{fmt_candle(last_daily)}",
        "",
        "=== 5 MIN ===",
        f"5m candles count: {len(m5)}",
        f"First 5m:\n{fmt_candle(m5[0] if len(m5) >= 1 else None)}",
        "",
        f"Second 5m:\n{fmt_candle(m5[1] if len(m5) >= 2 else None)}",
        "",
        "=== 15 MIN ===",
        f"15m candles count: {len(m15)}",
        f"First 15m:\n{fmt_candle(m15[0] if len(m15) >= 1 else None)}",
        "",
        f"Second 15m:\n{fmt_candle(m15[1] if len(m15) >= 2 else None)}",
        "",
        "=== OPTION CHAIN / OI ===",
        f"Option rows count: {len(oi_rows)}",
        build_oi_summary(oi_rows),
        "",
        "=== RAW STATUS ===",
        f"Daily status: {daily_resp.get('s', 'NA')}",
        f"5m status: {m5_resp.get('s', 'NA')}",
        f"15m status: {m15_resp.get('s', 'NA')}",
        f"OI status: {oi_resp.get('s', 'NA')}",
    ]

    send("\n".join(msg))

if __name__ == "__main__":
    main()

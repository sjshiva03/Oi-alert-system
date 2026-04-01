import os
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

EQ_SYMBOL = "NSE:M&M-EQ"   # candles
FO_SYMBOL = "NSE:M&M"      # OI (IMPORTANT FIX)

fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

# ================= TELEGRAM =================
def send(msg):
    print(msg)
    if TELEGRAM_TOKEN and CHAT_ID:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg}
        )

# ================= HISTORY =================
def get_history(symbol, res):
    payload = {
        "symbol": symbol,
        "resolution": str(res),
        "date_format": "1",
        "range_from": (datetime.now(IST) - timedelta(days=5)).strftime("%Y-%m-%d"),
        "range_to": datetime.now(IST).strftime("%Y-%m-%d"),
        "cont_flag": "1",
    }
    return fyers.history(data=payload)

# ================= OPTION CHAIN =================
def get_oi():
    data = fyers.optionchain({"symbol": FO_SYMBOL, "strikecount": 10})
    rows = data.get("data", {}).get("optionsChain", [])

    if not rows:
        return "No OI Data"

    out = []
    for r in rows[:6]:
        strike = int(float(r.get("strike_price", 0)))

        ce = r.get("CE", {})
        pe = r.get("PE", {})

        ce_val = ce.get("oi_change") or ce.get("changeinopeninterest") or 0
        pe_val = pe.get("oi_change") or pe.get("changeinopeninterest") or 0

        arrow_ce = "↑" if ce_val > 0 else "↓" if ce_val < 0 else "→"
        arrow_pe = "↑" if pe_val > 0 else "↓" if pe_val < 0 else "→"

        out.append(f"{strike} CE:{int(ce_val)} {arrow_ce} | PE:{int(pe_val)} {arrow_pe}")

    return "\n".join(out)

# ================= MAIN =================
def main():
    daily = get_history(EQ_SYMBOL, "D")["candles"]
    m5 = get_history(EQ_SYMBOL, 5)["candles"]
    m15 = get_history(EQ_SYMBOL, 15)["candles"]

    msg = f"""
✅ DATA CHECK WORKING

=== DAILY ===
Prev Close: {daily[-2][4] if len(daily)>1 else "NA"}

=== 5M ===
First: {m5[0] if len(m5)>0 else "NA"}

=== 15M ===
First: {m15[0] if len(m15)>0 else "NA"}

=== OI DATA (FIXED) ===
{get_oi()}
"""

    send(msg)

if __name__ == "__main__":
    main()

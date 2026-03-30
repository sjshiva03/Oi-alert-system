import os
import time
from datetime import datetime, timedelta, timezone
import requests
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FYERS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))
RR_MULTIPLIER = float(os.getenv("RR_MULTIPLIER", "2.0"))
PARTIAL_AT_R = float(os.getenv("PARTIAL_AT_R", "1.0"))
TRAIL_AFTER_R = float(os.getenv("TRAIL_AFTER_R", "1.0"))
PARTIAL_EXIT_PCT = float(os.getenv("PARTIAL_EXIT_PCT", "50"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").strip().lower() == "true"

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST_RAW = os.getenv(
    "WATCHLIST",
    "NSE:RELIANCE-EQ,NSE:SBIN-EQ,NSE:HDFCBANK-EQ,NSE:ICICIBANK-EQ"
).strip()

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def log(msg):
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [PRO-15M-OI] {msg}", flush=True)

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def normalize_symbol(sym):
    s = str(sym).strip().upper()
    if not s:
        return ""
    if s.endswith("-INDEX") and ":" in s:
        return s
    if ":" not in s:
        s = "NSE:" + s
    if s.startswith("NSE:") or s.startswith("BSE:"):
        tail = s.split(":", 1)[1]
        if not tail.endswith("-EQ") and not tail.endswith("-INDEX"):
            if "NIFTY" in tail:
                s = s + "-INDEX"
            else:
                s = s + "-EQ"
    return s

def display_symbol_name(symbol):
    s = str(symbol).upper()
    if "NIFTYBANK" in s:
        return "BANKNIFTY"
    if "NIFTY50" in s or s.endswith("NIFTY-INDEX"):
        return "NIFTY"
    if s.endswith("-EQ"):
        return s.split(":")[-1].replace("-EQ", "")
    return s.split(":")[-1]

def get_watchlist():
    items = []
    normalized = WATCHLIST_RAW.replace("\n", ",").replace(";", ",")
    for part in normalized.split(","):
        s = normalize_symbol(part)
        if s:
            items.append(s)
    out = list(dict.fromkeys(items))
    log(f"WATCHLIST: {out}")
    return out

# ================= TELEGRAM =================
def send_telegram(msg):
    if not (TELEGRAM_TOKEN and CHAT_ID):
        log("Telegram not configured")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=20)
        if not r.ok:
            log(f"Telegram error: {r.text}")
    except Exception as e:
        log(f"Telegram exception: {e}")

# ================= FYERS =================
def get_fyers():
    token = FYERS_TOKEN
    client = CLIENT_ID
    if ":" in token and not client:
        client, token = token.split(":", 1)
    if not client or not token:
        raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
    return fyersModel.FyersModel(client_id=client, token=token, is_async=False, log_path="")

def get_ltp(fyers, symbol):
    try:
        res = fyers.quotes({"symbols": symbol})
        return safe_float(res["d"][0]["v"]["lp"], 0.0)
    except Exception:
        return 0.0

def get_oi(fyers, symbol):
    try:
        res = fyers.quotes({"symbols": symbol})
        return safe_float(res["d"][0]["v"].get("oi"), 0.0)
    except Exception:
        return 0.0

def get_15min_data(fyers, symbol):
    try:
        today = now_ist().strftime("%Y-%m-%d")
        data = {
            "symbol": symbol,
            "resolution": "15",
            "date_format": "1",
            "range_from": today,
            "range_to": today,
            "cont_flag": "1"
        }
        res = fyers.history(data=data)
        candles = res.get("candles", [])
        if len(candles) < 2:
            return None

        first = candles[0]
        second = candles[1]

        first_high = safe_float(first[2])
        first_low = safe_float(first[3])
        second_high = safe_float(second[2])
        second_low = safe_float(second[3])

        range_pct = 0.0
        if first_low > 0:
            range_pct = ((first_high - first_low) / first_low) * 100.0

        return {
            "first_high": first_high,
            "first_low": first_low,
            "second_high": second_high,
            "second_low": second_low,
            "range_pct": range_pct,
        }
    except Exception:
        return None

# ================= STRATEGY =================
def is_inside_bar(data):
    return data["second_high"] <= data["first_high"] and data["second_low"] >= data["first_low"]

def is_small_first_candle(data):
    return data["range_pct"] < 1.0

def buy_signal(ltp, data, current_oi, prev_oi):
    return (
        is_inside_bar(data) and
        is_small_first_candle(data) and
        ltp > data["first_high"] and
        current_oi > prev_oi
    )

def sell_signal(ltp, data, current_oi, prev_oi):
    return (
        is_inside_bar(data) and
        is_small_first_candle(data) and
        ltp < data["first_low"] and
        current_oi > prev_oi
    )

# ================= TRADE ENGINE =================
trades = {}
oi_data = {}

def create_trade(symbol, ltp, side, sl, target):
    risk = abs(ltp - sl)
    if risk <= 0:
        return

    partial_target = ltp + (risk * PARTIAL_AT_R) if side == "BUY" else ltp - (risk * PARTIAL_AT_R)

    trades[symbol] = {
        "entry": ltp,
        "sl": sl,
        "initial_sl": sl,
        "target": target,
        "partial_target": partial_target,
        "side": side,
        "active": True,
        "partial_done": False,
        "trail_done": False,
        "highest_price": ltp,
        "lowest_price": ltp,
        "risk": risk,
        "partial_exit_pct": PARTIAL_EXIT_PCT,
    }

    send_telegram(
        f"馃殌 PRO TRADE ENTRY\n\n"
        f"{display_symbol_name(symbol)}\n"
        f"Side: {side}\n"
        f"Entry: {ltp:.2f}\n"
        f"SL: {sl:.2f}\n"
        f"Partial: {partial_target:.2f} ({PARTIAL_AT_R}R)\n"
        f"Target: {target:.2f} ({RR_MULTIPLIER}R)\n"
        f"Time (IST): {ist_time_str()}"
    )

def update_trailing_sl(symbol, ltp):
    t = trades[symbol]
    if not t["active"]:
        return

    if t["side"] == "BUY":
        t["highest_price"] = max(t["highest_price"], ltp)
        one_r_price = t["entry"] + (t["risk"] * TRAIL_AFTER_R)

        if not t["trail_done"] and ltp >= one_r_price:
            new_sl = t["entry"]
            if new_sl > t["sl"]:
                t["sl"] = new_sl
                t["trail_done"] = True
                send_telegram(
                    f"馃攧 TRAILING SL UPDATED\n\n"
                    f"{display_symbol_name(symbol)}\n"
                    f"Side: BUY\n"
                    f"New SL: {t['sl']:.2f}\n"
                    f"Reason: Price reached {TRAIL_AFTER_R}R\n"
                    f"Time (IST): {ist_time_str()}"
                )

        if t["trail_done"]:
            candidate_sl = t["highest_price"] - (t["risk"] * 0.5)
            if candidate_sl > t["sl"]:
                t["sl"] = candidate_sl

    else:
        t["lowest_price"] = min(t["lowest_price"], ltp)
        one_r_price = t["entry"] - (t["risk"] * TRAIL_AFTER_R)

        if not t["trail_done"] and ltp <= one_r_price:
            new_sl = t["entry"]
            if new_sl < t["sl"]:
                t["sl"] = new_sl
                t["trail_done"] = True
                send_telegram(
                    f"馃攧 TRAILING SL UPDATED\n\n"
                    f"{display_symbol_name(symbol)}\n"
                    f"Side: SELL\n"
                    f"New SL: {t['sl']:.2f}\n"
                    f"Reason: Price reached {TRAIL_AFTER_R}R\n"
                    f"Time (IST): {ist_time_str()}"
                )

        if t["trail_done"]:
            candidate_sl = t["lowest_price"] + (t["risk"] * 0.5)
            if candidate_sl < t["sl"]:
                t["sl"] = candidate_sl

def manage_trade(symbol, ltp):
    t = trades[symbol]
    if not t["active"]:
        return

    update_trailing_sl(symbol, ltp)

    if not t["partial_done"]:
        if t["side"] == "BUY" and ltp >= t["partial_target"]:
            t["partial_done"] = True
            send_telegram(
                f"馃挵 PARTIAL BOOKING DONE\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: BUY\n"
                f"Booked: {t['partial_exit_pct']:.0f}%\n"
                f"Price: {ltp:.2f}\n"
                f"Remaining: {100 - t['partial_exit_pct']:.0f}%\n"
                f"Time (IST): {ist_time_str()}"
            )
        elif t["side"] == "SELL" and ltp <= t["partial_target"]:
            t["partial_done"] = True
            send_telegram(
                f"馃挵 PARTIAL BOOKING DONE\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: SELL\n"
                f"Booked: {t['partial_exit_pct']:.0f}%\n"
                f"Price: {ltp:.2f}\n"
                f"Remaining: {100 - t['partial_exit_pct']:.0f}%\n"
                f"Time (IST): {ist_time_str()}"
            )

    if t["side"] == "BUY":
        if ltp <= t["sl"]:
            send_telegram(
                f"馃洃 SL HIT\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: BUY\n"
                f"Exit Price: {ltp:.2f}\n"
                f"SL: {t['sl']:.2f}\n"
                f"Time (IST): {ist_time_str()}"
            )
            t["active"] = False
        elif ltp >= t["target"]:
            send_telegram(
                f"馃幆 FINAL TARGET HIT\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: BUY\n"
                f"Exit Price: {ltp:.2f}\n"
                f"Target: {t['target']:.2f}\n"
                f"Time (IST): {ist_time_str()}"
            )
            t["active"] = False
    else:
        if ltp >= t["sl"]:
            send_telegram(
                f"馃洃 SL HIT\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: SELL\n"
                f"Exit Price: {ltp:.2f}\n"
                f"SL: {t['sl']:.2f}\n"
                f"Time (IST): {ist_time_str()}"
            )
            t["active"] = False
        elif ltp <= t["target"]:
            send_telegram(
                f"馃幆 FINAL TARGET HIT\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: SELL\n"
                f"Exit Price: {ltp:.2f}\n"
                f"Target: {t['target']:.2f}\n"
                f"Time (IST): {ist_time_str()}"
            )
            t["active"] = False

# ================= MAIN =================
def main():
    symbols = get_watchlist()
    if SEND_STARTUP_MESSAGE:
        send_telegram(f"馃殌 PRO 15M + OI system started\nTime (IST): {ist_time_str()}")

    while True:
        try:
            fyers = get_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(10)
            continue

        for symbol in symbols:
            try:
                ltp = get_ltp(fyers, symbol)
                if not ltp:
                    log(f"{symbol} | LTP not available")
                    continue

                oi = get_oi(fyers, symbol)
                prev_oi = oi_data.get(symbol, oi)
                oi_data[symbol] = oi

                candle_data = get_15min_data(fyers, symbol)
                if not candle_data:
                    continue

                if symbol not in trades or not trades[symbol]["active"]:
                    if buy_signal(ltp, candle_data, oi, prev_oi):
                        sl = candle_data["first_low"]
                        target = ltp + ((ltp - sl) * RR_MULTIPLIER)
                        create_trade(symbol, ltp, "BUY", sl, target)
                    elif sell_signal(ltp, candle_data, oi, prev_oi):
                        sl = candle_data["first_high"]
                        target = ltp - ((sl - ltp) * RR_MULTIPLIER)
                        create_trade(symbol, ltp, "SELL", sl, target)

                if symbol in trades and trades[symbol]["active"]:
                    manage_trade(symbol, ltp)

                log(f"{display_symbol_name(symbol)} | LTP={ltp:.2f} | OI={oi:.0f}")

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        time.sleep(max(5, POLL_SECONDS))

if __name__ == "__main__":
    main()

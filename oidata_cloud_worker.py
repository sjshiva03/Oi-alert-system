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
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "8"))
RR_MULTIPLIER = float(os.getenv("RR_MULTIPLIER", "2.0"))
PARTIAL_AT_R = float(os.getenv("PARTIAL_AT_R", "1.0"))
TRAIL_AFTER_R = float(os.getenv("TRAIL_AFTER_R", "1.0"))
PARTIAL_EXIT_PCT = float(os.getenv("PARTIAL_EXIT_PCT", "50"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").strip().lower() == "true"

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST_RAW = os.getenv(
    "WATCHLIST",
    "NSE:RELIANCE-EQ,NSE:SBIN-EQ,NSE:HDFCBANK-EQ,NSE:ICICIBANK-EQ,NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX"
).strip()

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def log(msg):
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [PRO-15M-CHAIN-OI] {msg}", flush=True)

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

    return fyersModel.FyersModel(
        client_id=client,
        token=token,
        is_async=False,
        log_path=""
    )

def get_ltp(fyers, symbol):
    try:
        res = fyers.quotes({"symbols": symbol})
        return safe_float(res["d"][0]["v"]["lp"], 0.0)
    except Exception:
        return 0.0

def fetch_option_chain(fyers, symbol, strikecount=8, timestamp=""):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": timestamp}
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)
    except Exception as e:
        log(f"optionchain error for {symbol}: {e}")
        return {}

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

def get_15min_data(fyers, symbol):
    try:
        today = today_ist_str()
        data = {
            "symbol": symbol,
            "resolution": "15",
            "date_format": "1",
            "range_from": today,
            "range_to": today,
            "cont_flag": "1",
        }
        res = fyers.history(data=data)
        candles = res.get("candles", []) or res.get("data", {}).get("candles", [])
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

# ================= OPTION-CHAIN OI HELPERS =================
def split_ce_pe_rows(options_list):
    ce_rows = []
    pe_rows = []

    for row in options_list:
        if not isinstance(row, dict):
            continue

        symbol = str(row.get("symbol", "")).upper()
        option_type = str(
            row.get("option_type")
            or row.get("optionType")
            or row.get("type")
            or row.get("otype")
            or ""
        ).upper()

        if option_type in ("CE", "CALL", "C") or symbol.endswith("CE"):
            ce_rows.append(row)
        elif option_type in ("PE", "PUT", "P") or symbol.endswith("PE"):
            pe_rows.append(row)

    return ce_rows, pe_rows

def get_row_strike(row):
    return safe_float(
        row.get("strike_price")
        or row.get("strikePrice")
        or row.get("strike")
        or row.get("sp"),
        0.0,
    )

def get_row_oi_change(row):
    return safe_float(
        row.get("oich")
        or row.get("oi_change")
        or row.get("oiChange")
        or row.get("open_interest_change")
        or row.get("openInterestChange"),
        0.0,
    )

def get_row_oi(row):
    return safe_float(
        row.get("oi")
        or row.get("open_interest")
        or row.get("openInterest"),
        0.0,
    )

def nearest_atm_rows(ce_rows, pe_rows, underlying_ltp):
    all_rows = ce_rows + pe_rows
    if not all_rows:
        return None, None, 0.0

    strikes = [get_row_strike(r) for r in all_rows if get_row_strike(r) > 0]
    if not strikes:
        return None, None, 0.0

    atm_strike = min(strikes, key=lambda x: abs(x - underlying_ltp))

    atm_ce = None
    atm_pe = None

    for row in ce_rows:
        if get_row_strike(row) == atm_strike:
            atm_ce = row
            break

    for row in pe_rows:
        if get_row_strike(row) == atm_strike:
            atm_pe = row
            break

    return atm_ce, atm_pe, atm_strike

def get_chain_oi_snapshot(options_list, underlying_ltp):
    ce_rows, pe_rows = split_ce_pe_rows(options_list)
    atm_ce, atm_pe, atm_strike = nearest_atm_rows(ce_rows, pe_rows, underlying_ltp)

    if atm_ce is None and atm_pe is None:
        return None

    ce_oich = get_row_oi_change(atm_ce) if atm_ce else 0.0
    pe_oich = get_row_oi_change(atm_pe) if atm_pe else 0.0
    ce_oi = get_row_oi(atm_ce) if atm_ce else 0.0
    pe_oi = get_row_oi(atm_pe) if atm_pe else 0.0

    return {
        "atm_strike": atm_strike,
        "ce_oi": ce_oi,
        "pe_oi": pe_oi,
        "ce_oich": ce_oich,
        "pe_oich": pe_oich,
    }

def classify_chain_oi_signal(snapshot):
    if not snapshot:
        return "NO_DATA"

    ce_oich = safe_float(snapshot.get("ce_oich"), 0.0)
    pe_oich = safe_float(snapshot.get("pe_oich"), 0.0)

    if pe_oich > 0 and ce_oich <= 0:
        return "BUY STRONG"
    if ce_oich > 0 and pe_oich <= 0:
        return "SELL STRONG"
    if pe_oich > ce_oich:
        return "BUY"
    if ce_oich > pe_oich:
        return "SELL"
    return "SIDEWAYS"

# ================= STRATEGY =================
def is_inside_bar(data):
    return data["second_high"] <= data["first_high"] and data["second_low"] >= data["first_low"]

def is_small_first_candle(data):
    return data["range_pct"] < 1.0

def buy_signal(ltp, data, oi_signal):
    return (
        is_inside_bar(data) and
        is_small_first_candle(data) and
        ltp > data["first_high"] and
        oi_signal in ("BUY STRONG", "BUY")
    )

def sell_signal(ltp, data, oi_signal):
    return (
        is_inside_bar(data) and
        is_small_first_candle(data) and
        ltp < data["first_low"] and
        oi_signal in ("SELL STRONG", "SELL")
    )

# ================= TRADE ENGINE =================
trades = {}

def create_trade(symbol, ltp, side, sl, target, oi_snapshot, oi_signal):
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
        "oi_signal": oi_signal,
        "atm_strike": safe_float(oi_snapshot.get("atm_strike"), 0.0),
        "ce_oich": safe_float(oi_snapshot.get("ce_oich"), 0.0),
        "pe_oich": safe_float(oi_snapshot.get("pe_oich"), 0.0),
    }

    send_telegram(
        f"ðŸš€ PRO TRADE ENTRY\n\n"
        f"{display_symbol_name(symbol)}\n"
        f"Side: {side}\n"
        f"OI: {oi_signal}\n"
        f"ATM Strike: {safe_float(oi_snapshot.get('atm_strike'), 0.0):.0f}\n"
        f"CE OI Chg: {safe_float(oi_snapshot.get('ce_oich'), 0.0):.0f}\n"
        f"PE OI Chg: {safe_float(oi_snapshot.get('pe_oich'), 0.0):.0f}\n"
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
                    f"ðŸ”„ TRAILING SL UPDATED\n\n"
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
                    f"ðŸ”„ TRAILING SL UPDATED\n\n"
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
                f"ðŸ’° PARTIAL BOOKING DONE\n\n"
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
                f"ðŸ’° PARTIAL BOOKING DONE\n\n"
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
                f"ðŸ›‘ SL HIT\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: BUY\n"
                f"Exit Price: {ltp:.2f}\n"
                f"SL: {t['sl']:.2f}\n"
                f"Time (IST): {ist_time_str()}"
            )
            t["active"] = False
        elif ltp >= t["target"]:
            send_telegram(
                f"ðŸŽ¯ FINAL TARGET HIT\n\n"
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
                f"ðŸ›‘ SL HIT\n\n"
                f"{display_symbol_name(symbol)}\n"
                f"Side: SELL\n"
                f"Exit Price: {ltp:.2f}\n"
                f"SL: {t['sl']:.2f}\n"
                f"Time (IST): {ist_time_str()}"
            )
            t["active"] = False
        elif ltp <= t["target"]:
            send_telegram(
                f"ðŸŽ¯ FINAL TARGET HIT\n\n"
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
        send_telegram(f"ðŸš€ PRO 15M + OPTION-CHAIN OI system started\nTime (IST): {ist_time_str()}")

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

                candle_data = get_15min_data(fyers, symbol)
                if not candle_data:
                    continue

                chain_resp = fetch_option_chain(fyers, symbol, STRIKECOUNT, "")
                options_list = extract_options_chain_list(chain_resp)
                oi_snapshot = get_chain_oi_snapshot(options_list, ltp)
                oi_signal = classify_chain_oi_signal(oi_snapshot)

                if symbol not in trades or not trades[symbol]["active"]:
                    if buy_signal(ltp, candle_data, oi_signal):
                        sl = candle_data["first_low"]
                        target = ltp + ((ltp - sl) * RR_MULTIPLIER)
                        create_trade(symbol, ltp, "BUY", sl, target, oi_snapshot or {}, oi_signal)
                    elif sell_signal(ltp, candle_data, oi_signal):
                        sl = candle_data["first_high"]
                        target = ltp - ((sl - ltp) * RR_MULTIPLIER)
                        create_trade(symbol, ltp, "SELL", sl, target, oi_snapshot or {}, oi_signal)

                if symbol in trades and trades[symbol]["active"]:
                    manage_trade(symbol, ltp)

                if oi_snapshot:
                    log(
                        f"{display_symbol_name(symbol)} | "
                        f"LTP={ltp:.2f} | OI={oi_signal} | "
                        f"ATM={safe_float(oi_snapshot['atm_strike'], 0.0):.0f} | "
                        f"CE_OICH={safe_float(oi_snapshot['ce_oich'], 0.0):.0f} | "
                        f"PE_OICH={safe_float(oi_snapshot['pe_oich'], 0.0):.0f}"
                    )
                else:
                    log(f"{display_symbol_name(symbol)} | LTP={ltp:.2f} | OI=NO_DATA")

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        time.sleep(max(5, POLL_SECONDS))

if __name__ == "__main__":
    main()

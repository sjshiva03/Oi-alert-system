import os
import time
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# =========================
# CONFIG
# =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FYERS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "8"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").strip().lower() == "true"

# 0.1% buffer for SL
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.001"))
# 1% target
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.01"))

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST_RAW = os.getenv(
    "WATCHLIST",
    "NSE:HDFCBANK-EQ,NSE:ICICIBANK-EQ,NSE:SBIN-EQ,NSE:RELIANCE-EQ,NSE:INFY-EQ"
).strip()

# To avoid repeat alerts for same day/symbol/side
alerted_setups = set()


# =========================
# HELPERS
# =========================
def now_ist():
    return datetime.now(IST)

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def log(msg):
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [15M-BREAKOUT-OI-TG] {msg}", flush=True)

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def display_symbol_name(symbol):
    s = str(symbol).upper()
    if s.endswith("-EQ"):
        return s.split(":")[-1].replace("-EQ", "")
    if s.endswith("-INDEX"):
        return s.split(":")[-1].replace("-INDEX", "")
    return s.split(":")[-1]

def normalize_symbol(sym):
    s = str(sym).strip().upper()
    if not s:
        return ""
    if ":" not in s:
        s = "NSE:" + s
    if s.startswith("NSE:") or s.startswith("BSE:"):
        tail = s.split(":", 1)[1]
        if not tail.endswith("-EQ") and not tail.endswith("-INDEX"):
            if "NIFTY" in tail:
                s += "-INDEX"
            else:
                s += "-EQ"
    return s

def get_watchlist():
    out = []
    raw = WATCHLIST_RAW.replace("\n", ",").replace(";", ",")
    for part in raw.split(","):
        s = normalize_symbol(part)
        if s:
            out.append(s)
    return list(dict.fromkeys(out))

def is_market_open():
    t = now_ist().time()
    return t >= datetime.strptime("09:15", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time()

def wait_until_market_open():
    while not is_market_open():
        log("Market closed. Waiting...")
        time.sleep(60)


# =========================
# TELEGRAM
# =========================
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log("Telegram not configured")
        print(msg)
        return

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=20
        )
        if not r.ok:
            log(f"Telegram error: {r.text}")
    except Exception as e:
        log(f"Telegram exception: {e}")


# =========================
# FYERS
# =========================
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

def fetch_quotes_map(fyers, symbols):
    if not symbols:
        return {}

    payload = {"symbols": ",".join(symbols)}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception as e:
        log(f"quotes error: {e}")
        return {}

    out = {}
    if not isinstance(resp, dict):
        return out

    items = resp.get("d") or resp.get("data") or []
    if not isinstance(items, list):
        return out

    for item in items:
        if not isinstance(item, dict):
            continue

        sym = item.get("n") or item.get("symbol") or item.get("name") or ""
        vals = item.get("v") or item.get("values") or item

        if sym:
            out[sym] = {
                "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0)
            }

    return out

def get_today_15m_candles(fyers, symbol):
    payload = {
        "symbol": symbol,
        "resolution": "15",
        "date_format": "1",
        "range_from": today_ist_str(),
        "range_to": today_ist_str(),
        "cont_flag": "1"
    }

    try:
        resp = fyers.history(data=payload)
    except TypeError:
        resp = fyers.history(payload)
    except Exception as e:
        log(f"{symbol} history error: {e}")
        return []

    candles = resp.get("candles", []) or resp.get("data", {}).get("candles", [])
    return candles

def fetch_option_chain(fyers, symbol, strikecount=8):
    payload = {
        "symbol": symbol,
        "strikecount": strikecount,
        "timestamp": ""
    }

    try:
        resp = fyers.optionchain(data=payload)
    except TypeError:
        resp = fyers.optionchain(payload)
    except Exception as e:
        log(f"{symbol} optionchain error: {e}")
        return {}

    return resp


# =========================
# 15M SETUP
# =========================
def get_first_two_market_candles(candles):
    parsed = []

    for row in candles:
        if not isinstance(row, list) or len(row) < 6:
            continue

        ts, o, h, l, c, v = row[:6]
        dt = datetime.fromtimestamp(ts, IST)

        parsed.append({
            "dt": dt,
            "open": safe_float(o),
            "high": safe_float(h),
            "low": safe_float(l),
            "close": safe_float(c),
            "volume": safe_float(v),
        })

    parsed.sort(key=lambda x: x["dt"])

    market = []
    for c in parsed:
        t = c["dt"].time()
        if t >= datetime.strptime("09:15", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time():
            market.append(c)

    if len(market) < 2:
        return None, None

    return market[0], market[1]

def setup_valid(c1, c2):
    if not c1 or not c2:
        return False, 0.0

    if c1["close"] <= 0:
        return False, 0.0

    range_pct = ((c1["high"] - c1["low"]) / c1["close"]) * 100.0
    inside_bar = c2["high"] < c1["high"] and c2["low"] > c1["low"]

    return (range_pct < 1.0 and inside_bar), range_pct


# =========================
# OI CONFIRMATION
# SELL: CE writers should build at nearby strikes
# BUY : PE writers should build at nearby strikes
# =========================
def get_option_rows(resp):
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

def get_row_strike(row):
    return safe_float(
        row.get("strike_price")
        or row.get("strikePrice")
        or row.get("strike")
        or row.get("sp"),
        0.0
    )

def get_row_oich(row):
    return safe_float(
        row.get("oich")
        or row.get("oi_change")
        or row.get("oiChange")
        or row.get("open_interest_change")
        or row.get("openInterestChange"),
        0.0
    )

def split_ce_pe(option_rows):
    ce_rows = []
    pe_rows = []

    for row in option_rows:
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

def nearest_strike_list(rows, ltp, count=3):
    enriched = []
    for row in rows:
        strike = get_row_strike(row)
        if strike > 0:
            enriched.append((abs(strike - ltp), strike, row))

    enriched.sort(key=lambda x: x[0])
    return [x[2] for x in enriched[:count]]

def check_ce_writer_build(optionchain_resp, ltp):
    option_rows = get_option_rows(optionchain_resp)
    ce_rows, _ = split_ce_pe(option_rows)

    nearby = nearest_strike_list(ce_rows, ltp, count=3)
    built = []

    for row in nearby:
        strike = get_row_strike(row)
        oich = get_row_oich(row)
        if oich > 0:
            built.append((strike, oich))

    return built

def check_pe_writer_build(optionchain_resp, ltp):
    option_rows = get_option_rows(optionchain_resp)
    _, pe_rows = split_ce_pe(option_rows)

    nearby = nearest_strike_list(pe_rows, ltp, count=3)
    built = []

    for row in nearby:
        strike = get_row_strike(row)
        oich = get_row_oich(row)
        if oich > 0:
            built.append((strike, oich))

    return built


# =========================
# MESSAGE
# =========================
def build_sell_message(symbol, c1, c2, entry, sl, target, ce_builds):
    lines = [
        "🔴 SELL BREAKOUT CONFIRMED",
        display_symbol_name(symbol),
        "",
        f"1st 15m Candle H: {round(c1['high'], 2)}",
        f"1st 15m Candle L: {round(c1['low'], 2)}",
        f"2nd 15m Candle H: {round(c2['high'], 2)}",
        f"2nd 15m Candle L: {round(c2['low'], 2)}",
        "",
        f"ENTRY SELL: {round(entry, 2)}",
        f"STOPLOSS: {round(sl, 2)}",
        f"TARGET: {round(target, 2)}",
        "",
        "CE WRITER BUILD:"
    ]

    for strike, oich in ce_builds:
        lines.append(f"{round(strike, 2)} -> OI Change {round(oich, 2)}")

    lines.append("")
    lines.append(f"Time (IST): {ist_time_str()}")
    return "\n".join(lines)

def build_buy_message(symbol, c1, c2, entry, sl, target, pe_builds):
    lines = [
        "🟢 BUY BREAKOUT CONFIRMED",
        display_symbol_name(symbol),
        "",
        f"1st 15m Candle H: {round(c1['high'], 2)}",
        f"1st 15m Candle L: {round(c1['low'], 2)}",
        f"2nd 15m Candle H: {round(c2['high'], 2)}",
        f"2nd 15m Candle L: {round(c2['low'], 2)}",
        "",
        f"ENTRY BUY: {round(entry, 2)}",
        f"STOPLOSS: {round(sl, 2)}",
        f"TARGET: {round(target, 2)}",
        "",
        "PE WRITER BUILD:"
    ]

    for strike, oich in pe_builds:
        lines.append(f"{round(strike, 2)} -> OI Change {round(oich, 2)}")

    lines.append("")
    lines.append(f"Time (IST): {ist_time_str()}")
    return "\n".join(lines)


# =========================
# MAIN
# =========================
def main():
    symbols = get_watchlist()

    if SEND_STARTUP_MESSAGE and is_market_open():
        send_telegram(
            "🚀 15m breakout + OI confirmation system started\n"
            f"Symbols: {', '.join(display_symbol_name(s) for s in symbols)}\n"
            "Runs only in market hours 09:15 to 15:30 IST"
        )

    while True:
        if not is_market_open():
            wait_until_market_open()
            continue

        try:
            fyers = get_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(30)
            continue

        quotes_map = fetch_quotes_map(fyers, symbols)

        for symbol in symbols:
            try:
                ltp = safe_float(quotes_map.get(symbol, {}).get("ltp"), 0.0)
                if ltp <= 0:
                    log(f"{symbol} | LTP not available")
                    continue

                candles = get_today_15m_candles(fyers, symbol)
                c1, c2 = get_first_two_market_candles(candles)

                valid, range_pct = setup_valid(c1, c2)
                if not valid:
                    log(f"{display_symbol_name(symbol)} | No valid 15m setup")
                    continue

                # SELL condition
                if ltp < c1["low"]:
                    ce_builds = check_ce_writer_build(fetch_option_chain(fyers, symbol, STRIKECOUNT), ltp)

                    if len(ce_builds) >= 2:
                        entry = c1["low"]
                        sl = c1["high"] * (1 + SL_BUFFER_PCT)
                        target = entry - (entry * TARGET_PCT)

                        key = (today_ist_str(), symbol, "SELL")
                        if key not in alerted_setups:
                            alerted_setups.add(key)
                            send_telegram(build_sell_message(symbol, c1, c2, entry, sl, target, ce_builds))
                            log(f"{display_symbol_name(symbol)} | SELL ALERT SENT")
                    else:
                        log(f"{display_symbol_name(symbol)} | Sell breakout but no CE writer confirmation")

                # BUY condition
                elif ltp > c1["high"]:
                    pe_builds = check_pe_writer_build(fetch_option_chain(fyers, symbol, STRIKECOUNT), ltp)

                    if len(pe_builds) >= 2:
                        entry = c1["high"]
                        sl = c1["low"] * (1 - SL_BUFFER_PCT)
                        target = entry + (entry * TARGET_PCT)

                        key = (today_ist_str(), symbol, "BUY")
                        if key not in alerted_setups:
                            alerted_setups.add(key)
                            send_telegram(build_buy_message(symbol, c1, c2, entry, sl, target, pe_builds))
                            log(f"{display_symbol_name(symbol)} | BUY ALERT SENT")
                    else:
                        log(f"{display_symbol_name(symbol)} | Buy breakout but no PE writer confirmation")

                else:
                    log(
                        f"{display_symbol_name(symbol)} | "
                        f"Setup ready | LTP={round(ltp,2)} | "
                        f"C1H={round(c1['high'],2)} | C1L={round(c1['low'],2)} | "
                        f"Range%={round(range_pct,2)}"
                    )

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        time.sleep(max(5, POLL_SECONDS))


if __name__ == "__main__":
    main()

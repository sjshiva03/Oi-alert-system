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

SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.001"))   # 0.1%
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.01"))          # 1%

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST_RAW = os.getenv(
    "WATCHLIST",
    "NSE:HDFCBANK-EQ,NSE:ICICIBANK-EQ,NSE:SBIN-EQ,NSE:RELIANCE-EQ,NSE:INFY-EQ"
).strip()

# 2026 NSE trading holidays
HOLIDAYS = {
    "2026-01-26",
    "2026-03-03",
    "2026-03-26",
    "2026-03-31",
    "2026-04-03",
    "2026-04-14",
    "2026-05-01",
    "2026-05-28",
    "2026-06-26",
    "2026-09-14",
    "2026-10-02",
    "2026-10-20",
    "2026-11-10",
    "2026-11-24",
    "2026-12-25",
}

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
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [LIVE-MERGED] {msg}", flush=True)

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

def is_weekend(dt_obj):
    return dt_obj.weekday() >= 5

def is_holiday(dt_obj):
    return dt_obj.strftime("%Y-%m-%d") in HOLIDAYS

def is_market_day(dt_obj):
    return (not is_weekend(dt_obj)) and (not is_holiday(dt_obj))

def market_open_time(dt_obj):
    return dt_obj.replace(hour=9, minute=15, second=0, microsecond=0)

def market_close_time(dt_obj):
    return dt_obj.replace(hour=15, minute=30, second=0, microsecond=0)

def is_market_open():
    now = now_ist()
    if not is_market_day(now):
        return False
    return market_open_time(now) <= now <= market_close_time(now)

def next_market_open_datetime():
    now = now_ist()

    if is_market_day(now) and now < market_open_time(now):
        return market_open_time(now)

    candidate = now + timedelta(days=1)
    candidate = candidate.replace(hour=9, minute=15, second=0, microsecond=0)

    while not is_market_day(candidate):
        candidate += timedelta(days=1)
        candidate = candidate.replace(hour=9, minute=15, second=0, microsecond=0)

    return candidate

def seconds_until(dt_obj):
    return max(1, int((dt_obj - now_ist()).total_seconds()))

def sleep_until_next_market_open():
    nxt = next_market_open_datetime()
    log(f"Market closed. Sleeping until {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
    while True:
        remaining = seconds_until(nxt)
        if remaining <= 1:
            break
        time.sleep(min(60, remaining))

def reset_daily_alert_cache_if_needed():
    today = today_ist_str()
    stale = [x for x in alerted_setups if x[0] != today]
    for x in stale:
        alerted_setups.discard(x)

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
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=20)
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

def get_history(fyers, symbol, resolution, range_from, range_to):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": range_from,
        "range_to": range_to,
        "cont_flag": "1"
    }
    try:
        resp = fyers.history(data=payload)
    except TypeError:
        resp = fyers.history(payload)
    except Exception as e:
        log(f"{symbol} history {resolution} error: {e}")
        return []
    return resp.get("candles", []) or resp.get("data", {}).get("candles", [])

def fetch_option_chain(fyers, symbol, strikecount=8):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": ""}
    try:
        resp = fyers.optionchain(data=payload)
    except TypeError:
        resp = fyers.optionchain(payload)
    except Exception as e:
        log(f"{symbol} optionchain error: {e}")
        return {}
    return resp

# =========================
# CANDLE PARSING
# =========================
def parse_candles(candles):
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
    return parsed

def get_market_candles_for_day(fyers, symbol, resolution):
    raw = get_history(fyers, symbol, resolution, today_ist_str(), today_ist_str())
    parsed = parse_candles(raw)
    market = []
    for c in parsed:
        t = c["dt"].time()
        if datetime.strptime("09:15", "%H:%M").time() <= t <= datetime.strptime("15:30", "%H:%M").time():
            market.append(c)
    return market

# =========================
# OPTION CHAIN OI LOGIC
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
# 15M INSIDE BAR BREAKOUT
# =========================
def get_first_two_15m_candles(fyers, symbol):
    market = get_market_candles_for_day(fyers, symbol, "15")
    if len(market) < 2:
        return None, None
    return market[0], market[1]

def setup_valid_15m(c1, c2):
    if not c1 or not c2 or c1["close"] <= 0:
        return False, 0.0
    range_pct = ((c1["high"] - c1["low"]) / c1["close"]) * 100.0
    inside_bar = c2["high"] < c1["high"] and c2["low"] > c1["low"]
    return (range_pct < 1.0 and inside_bar), range_pct

# =========================
# 30M PIVOT REJECTION
# =========================
def get_prev_day_30m_candles(fyers, symbol):
    end_dt = now_ist()
    start_dt = end_dt - timedelta(days=7)
    raw = get_history(fyers, symbol, "30", start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    parsed = parse_candles(raw)

    grouped = {}
    for c in parsed:
        d = c["dt"].date()
        grouped.setdefault(d, []).append(c)

    days = sorted(grouped.keys())
    if len(days) < 2:
        return None, None

    prev_day = days[-2]
    curr_day = days[-1]
    return grouped.get(prev_day, []), grouped.get(curr_day, [])

def calc_pivot_levels(prev_day_candles):
    if not prev_day_candles:
        return {}

    h = max(x["high"] for x in prev_day_candles)
    l = min(x["low"] for x in prev_day_candles)
    c = prev_day_candles[-1]["close"]

    p = (h + l + c) / 3.0
    r1 = 2 * p - l
    s1 = 2 * p - h
    r2 = p + (h - l)
    s2 = p - (h - l)
    r3 = h + 2 * (p - l)
    s3 = l - 2 * (h - p)
    r4 = r3 + (r2 - r1)
    s4 = s3 - (s1 - s2)
    r5 = r4 + (r2 - r1)

    return {"S4": s4, "S3": s3, "S2": s2, "S1": s1, "P": p, "R1": r1, "R2": r2, "R3": r3, "R4": r4, "R5": r5}

def touches_level(candle, level):
    return candle["low"] <= level <= candle["high"]

def get_latest_pivot_pattern(curr_day_30m, levels):
    if not curr_day_30m or len(curr_day_30m) < 2:
        return None

    c1 = curr_day_30m[-2]
    c2 = curr_day_30m[-1]

    for level_name, level_value in levels.items():
        if (
            c1["close"] > c1["open"] and
            touches_level(c1, level_value) and
            c2["close"] < c2["open"] and
            touches_level(c2, level_value) and
            c2["close"] < level_value
        ):
            return {
                "side": "SELL",
                "level_name": level_name,
                "level_value": level_value,
                "c1": c1,
                "c2": c2,
                "entry": c2["low"],
                "sl": max(c1["high"], c2["high"]),
            }

        if (
            c1["close"] < c1["open"] and
            touches_level(c1, level_value) and
            c2["close"] > c2["open"] and
            touches_level(c2, level_value) and
            c2["close"] > level_value
        ):
            return {
                "side": "BUY",
                "level_name": level_name,
                "level_value": level_value,
                "c1": c1,
                "c2": c2,
                "entry": c2["high"],
                "sl": min(c1["low"], c2["low"]),
            }

    return None

# =========================
# GAP-UP SELL
# =========================
def get_prev_day_high_and_today_5m(fyers, symbol):
    end_dt = now_ist()
    start_dt = end_dt - timedelta(days=7)

    daily_raw = get_history(fyers, symbol, "D", start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    if len(daily_raw) < 2:
        return None, None

    daily_sorted = sorted(daily_raw, key=lambda x: x[0])
    prev_day = daily_sorted[-2]
    prev_day_high = safe_float(prev_day[2])

    today_5m = get_market_candles_for_day(fyers, symbol, "5")
    if len(today_5m) < 1:
        return prev_day_high, None

    first_5m = today_5m[0]
    return prev_day_high, first_5m

def setup_gapup_sell(prev_day_high, first_5m):
    if prev_day_high is None or not first_5m:
        return False, 0.0

    open_above_pdh = first_5m["open"] > prev_day_high
    if first_5m["close"] <= 0:
        return False, 0.0

    range_pct = ((first_5m["high"] - first_5m["low"]) / first_5m["close"]) * 100.0
    small_candle = range_pct < 1.0

    return (open_above_pdh and small_candle), range_pct

# =========================
# MESSAGE BUILDERS
# =========================
def build_sell_15m_message(symbol, c1, c2, entry, sl, target, ce_builds):
    lines = [
        "🔴 15M SELL BREAKOUT CONFIRMED",
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
    return "\\n".join(lines)

def build_buy_15m_message(symbol, c1, c2, entry, sl, target, pe_builds):
    lines = [
        "🟢 15M BUY BREAKOUT CONFIRMED",
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
    return "\\n".join(lines)

def build_sell_30m_pivot_message(symbol, pattern, sl, target, ce_builds):
    c1 = pattern["c1"]
    c2 = pattern["c2"]
    lines = [
        "🔴 30M PIVOT REJECTION SELL",
        display_symbol_name(symbol),
        "",
        f"Level: {pattern['level_name']} = {round(pattern['level_value'], 2)}",
        f"1st 30m Candle H: {round(c1['high'], 2)}",
        f"1st 30m Candle L: {round(c1['low'], 2)}",
        f"2nd 30m Candle H: {round(c2['high'], 2)}",
        f"2nd 30m Candle L: {round(c2['low'], 2)}",
        "",
        f"ENTRY SELL: {round(pattern['entry'], 2)}",
        f"STOPLOSS: {round(sl, 2)}",
        f"TARGET: {round(target, 2)}",
        "",
        "CE WRITER BUILD:"
    ]
    for strike, oich in ce_builds:
        lines.append(f"{round(strike, 2)} -> OI Change {round(oich, 2)}")
    lines.append("")
    lines.append(f"Time (IST): {ist_time_str()}")
    return "\\n".join(lines)

def build_buy_30m_pivot_message(symbol, pattern, sl, target, pe_builds):
    c1 = pattern["c1"]
    c2 = pattern["c2"]
    lines = [
        "🟢 30M PIVOT REJECTION BUY",
        display_symbol_name(symbol),
        "",
        f"Level: {pattern['level_name']} = {round(pattern['level_value'], 2)}",
        f"1st 30m Candle H: {round(c1['high'], 2)}",
        f"1st 30m Candle L: {round(c1['low'], 2)}",
        f"2nd 30m Candle H: {round(c2['high'], 2)}",
        f"2nd 30m Candle L: {round(c2['low'], 2)}",
        "",
        f"ENTRY BUY: {round(pattern['entry'], 2)}",
        f"STOPLOSS: {round(sl, 2)}",
        f"TARGET: {round(target, 2)}",
        "",
        "PE WRITER BUILD:"
    ]
    for strike, oich in pe_builds:
        lines.append(f"{round(strike, 2)} -> OI Change {round(oich, 2)}")
    lines.append("")
    lines.append(f"Time (IST): {ist_time_str()}")
    return "\\n".join(lines)

def build_gapup_sell_message(symbol, prev_day_high, first_5m, entry, sl, target, ce_builds):
    lines = [
        "🔴 GAP-UP SELL CONFIRMED",
        display_symbol_name(symbol),
        "",
        f"Previous Day High: {round(prev_day_high, 2)}",
        f"1st 5m Candle O: {round(first_5m['open'], 2)}",
        f"1st 5m Candle H: {round(first_5m['high'], 2)}",
        f"1st 5m Candle L: {round(first_5m['low'], 2)}",
        f"1st 5m Candle C: {round(first_5m['close'], 2)}",
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
    return "\\n".join(lines)

def main():
    symbols = get_watchlist()

    if SEND_STARTUP_MESSAGE and is_market_open():
        send_telegram(
            "🚀 15m breakout + 30m pivot rejection + gap-up sell + OI confirmation started\\n"
            f"Symbols: {', '.join(display_symbol_name(s) for s in symbols)}\\n"
            "Runs only in market hours 09:15 to 15:30 IST"
        )

    while True:
        reset_daily_alert_cache_if_needed()

        if not is_market_open():
            sleep_until_next_market_open()
            continue

        if now_ist() > market_close_time(now_ist()):
            log("Reached market close. Stopping scans for today.")
            sleep_until_next_market_open()
            continue

        try:
            fyers = get_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(30)
            continue

        quotes_map = fetch_quotes_map(fyers, symbols)

        for symbol in symbols:
            if not is_market_open():
                log("Market closed during scan loop. Stopping for today.")
                break

            try:
                ltp = safe_float(quotes_map.get(symbol, {}).get("ltp"), 0.0)
                if ltp <= 0:
                    log(f"{symbol} | LTP not available")
                    continue

                option_chain = fetch_option_chain(fyers, symbol, STRIKECOUNT)

                c1_15, c2_15 = get_first_two_15m_candles(fyers, symbol)
                valid_15m, _ = setup_valid_15m(c1_15, c2_15)

                if valid_15m:
                    if ltp < c1_15["low"]:
                        ce_builds = check_ce_writer_build(option_chain, ltp)
                        if len(ce_builds) >= 2:
                            entry = c1_15["low"]
                            sl = c1_15["high"] * (1 + SL_BUFFER_PCT)
                            target = entry - (entry * TARGET_PCT)

                            key = (today_ist_str(), symbol, "15M_SELL")
                            if key not in alerted_setups:
                                alerted_setups.add(key)
                                send_telegram(build_sell_15m_message(symbol, c1_15, c2_15, entry, sl, target, ce_builds))
                                log(f"{display_symbol_name(symbol)} | 15M SELL ALERT SENT")

                    elif ltp > c1_15["high"]:
                        pe_builds = check_pe_writer_build(option_chain, ltp)
                        if len(pe_builds) >= 2:
                            entry = c1_15["high"]
                            sl = c1_15["low"] * (1 - SL_BUFFER_PCT)
                            target = entry + (entry * TARGET_PCT)

                            key = (today_ist_str(), symbol, "15M_BUY")
                            if key not in alerted_setups:
                                alerted_setups.add(key)
                                send_telegram(build_buy_15m_message(symbol, c1_15, c2_15, entry, sl, target, pe_builds))
                                log(f"{display_symbol_name(symbol)} | 15M BUY ALERT SENT")

                prev_day_30m, curr_day_30m = get_prev_day_30m_candles(fyers, symbol)
                levels = calc_pivot_levels(prev_day_30m)
                pattern = get_latest_pivot_pattern(curr_day_30m, levels)

                if pattern:
                    if pattern["side"] == "SELL" and ltp < pattern["entry"]:
                        ce_builds = check_ce_writer_build(option_chain, ltp)
                        if len(ce_builds) >= 2:
                            sl = pattern["sl"] * (1 + SL_BUFFER_PCT)
                            target = pattern["entry"] - (sl - pattern["entry"])

                            key = (today_ist_str(), symbol, "30M_PIVOT_SELL", pattern["level_name"])
                            if key not in alerted_setups:
                                alerted_setups.add(key)
                                send_telegram(build_sell_30m_pivot_message(symbol, pattern, sl, target, ce_builds))
                                log(f"{display_symbol_name(symbol)} | 30M PIVOT SELL ALERT SENT")

                    elif pattern["side"] == "BUY" and ltp > pattern["entry"]:
                        pe_builds = check_pe_writer_build(option_chain, ltp)
                        if len(pe_builds) >= 2:
                            sl = pattern["sl"] * (1 - SL_BUFFER_PCT)
                            target = pattern["entry"] + (pattern["entry"] - sl)

                            key = (today_ist_str(), symbol, "30M_PIVOT_BUY", pattern["level_name"])
                            if key not in alerted_setups:
                                alerted_setups.add(key)
                                send_telegram(build_buy_30m_pivot_message(symbol, pattern, sl, target, pe_builds))
                                log(f"{display_symbol_name(symbol)} | 30M PIVOT BUY ALERT SENT")

                prev_day_high, first_5m = get_prev_day_high_and_today_5m(fyers, symbol)
                valid_gapup, _ = setup_gapup_sell(prev_day_high, first_5m)

                if valid_gapup and first_5m and ltp < first_5m["low"]:
                    ce_builds = check_ce_writer_build(option_chain, ltp)
                    if len(ce_builds) >= 2:
                        entry = first_5m["low"]
                        sl = first_5m["high"] * (1 + SL_BUFFER_PCT)
                        target = entry - (entry * TARGET_PCT)

                        key = (today_ist_str(), symbol, "GAPUP_SELL")
                        if key not in alerted_setups:
                            alerted_setups.add(key)
                            send_telegram(build_gapup_sell_message(symbol, prev_day_high, first_5m, entry, sl, target, ce_builds))
                            log(f"{display_symbol_name(symbol)} | GAP-UP SELL ALERT SENT")

                log(f"{display_symbol_name(symbol)} | LTP={round(ltp,2)} | scanned")

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        time.sleep(max(5, POLL_SECONDS))

if __name__ == "__main__":
    main()

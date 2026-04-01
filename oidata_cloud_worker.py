import os
import time
import requests
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel

# =========================================================
# CONFIG
# =========================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FYERS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()

WATCHLIST_RAW = os.getenv(
    "WATCHLIST",
    "NSE:AXISBANK-EQ,NSE:ICICIBANK-EQ,NSE:HDFCBANK-EQ,NSE:INFY-EQ,NSE:NTPC-EQ,NSE:TATASTEEL-EQ,NSE:HINDALCO-EQ,NSE:SBIN-EQ"
).strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "12"))

# Rules
FIRST_15M_MAX_RANGE_PCT = float(os.getenv("FIRST_15M_MAX_RANGE_PCT", "2.0"))   # 15m first candle below 2%
GAPUP_FIRST_5M_MAX_RANGE_PCT = float(os.getenv("GAPUP_FIRST_5M_MAX_RANGE_PCT", "1.5"))  # first 5m below 1.5%
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.001"))   # 0.1%
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.01"))          # 1%

OI_UPDATE_EVERY_SECONDS = int(os.getenv("OI_UPDATE_EVERY_SECONDS", "300"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").strip().lower() == "true"

NSE_HOLIDAYS_RAW = os.getenv(
    "NSE_HOLIDAYS",
    "2026-01-26,2026-03-03,2026-03-26,2026-03-31,2026-04-03,2026-04-14,2026-05-01,2026-05-28,2026-06-26,2026-09-14,2026-10-02,2026-10-20,2026-11-10,2026-11-24,2026-12-25"
).strip()

IST = timezone(timedelta(hours=5, minutes=30))

# =========================================================
# GLOBAL STATE
# =========================================================
gapup_summary_sent_for_day = None
inside15_summary_sent_for_day = None
pivot_alert_seen_for_day = set()
eod_sent_for_day = None

active_trades = {}   # key -> trade dict
closed_trades = []   # list of closed trade dict

# =========================================================
# HELPERS
# =========================================================
def now_ist():
    return datetime.now(IST)

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def log(msg):
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [FINAL-FLOW] {msg}", flush=True)

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def display_symbol_name(symbol):
    s = str(symbol).upper()
    if ":" in s:
        s = s.split(":", 1)[1]
    return s.replace("-EQ", "").replace("-INDEX", "")

def normalize_symbol(sym):
    s = str(sym).strip().upper()
    if not s:
        return ""
    if ":" not in s:
        s = "NSE:" + s
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

def get_holiday_set():
    out = set()
    for part in NSE_HOLIDAYS_RAW.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out

HOLIDAYS = get_holiday_set()

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

    candidate = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    while not is_market_day(candidate):
        candidate = (candidate + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    return candidate

def sleep_until_next_market_open():
    nxt = next_market_open_datetime()
    log(f"Market closed. Sleeping until {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
    while True:
        rem = int((nxt - now_ist()).total_seconds())
        if rem <= 1:
            break
        time.sleep(min(60, rem))

def human_oi(v):
    v = safe_float(v, 0.0)
    sign = "+" if v > 0 else ""
    av = abs(v)
    if av >= 10000000:
        txt = f"{sign}{v/10000000:.2f}Cr"
    elif av >= 100000:
        txt = f"{sign}{v/100000:.2f}L"
    elif av >= 1000:
        txt = f"{sign}{v/1000:.2f}K"
    else:
        txt = f"{sign}{v:.0f}"
    return txt.replace(".00", "")

def arrow_from_value(v):
    v = safe_float(v, 0.0)
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

# =========================================================
# TELEGRAM
# =========================================================
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(msg)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=20)
        if not resp.ok:
            log(f"Telegram error: {resp.text}")
    except Exception as e:
        log(f"Telegram send failed: {e}")

# =========================================================
# FYERS
# =========================================================
def get_fyers():
    token = FYERS_TOKEN
    client = CLIENT_ID
    if ":" in token and not client:
        client, token = token.split(":", 1)
    if not client or not token:
        raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
    return fyersModel.FyersModel(client_id=client, token=token, is_async=False, log_path="")

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
    items = resp.get("d") or resp.get("data") or []
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = item.get("n") or item.get("symbol") or item.get("name") or ""
        vals = item.get("v") or item.get("values") or item
        out[sym] = {"ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0)}
    return out

def get_history(fyers, symbol, resolution, range_from, range_to):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": range_from,
        "range_to": range_to,
        "cont_flag": "1",
    }
    try:
        resp = fyers.history(data=payload)
    except TypeError:
        resp = fyers.history(payload)
    except Exception as e:
        log(f"{symbol} history error {resolution}: {e}")
        return []
    return resp.get("candles", []) or resp.get("data", {}).get("candles", [])

def fetch_option_chain(fyers, symbol, strikecount=12):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": ""}
    try:
        resp = fyers.optionchain(data=payload)
    except TypeError:
        resp = fyers.optionchain(payload)
    except Exception as e:
        log(f"{symbol} optionchain error: {e}")
        return {}
    return resp

# =========================================================
# PARSING
# =========================================================
def parse_candles(candles):
    out = []
    for row in candles:
        if not isinstance(row, list) or len(row) < 6:
            continue
        ts, o, h, l, c, v = row[:6]
        out.append({
            "dt": datetime.fromtimestamp(ts, IST),
            "open": safe_float(o),
            "high": safe_float(h),
            "low": safe_float(l),
            "close": safe_float(c),
            "volume": safe_float(v),
        })
    out.sort(key=lambda x: x["dt"])
    return out

def get_market_candles_for_day(fyers, symbol, resolution):
    raw = get_history(fyers, symbol, resolution, today_ist_str(), today_ist_str())
    parsed = parse_candles(raw)
    market = []
    for c in parsed:
        t = c["dt"].time()
        if datetime.strptime("09:15", "%H:%M").time() <= t <= datetime.strptime("15:30", "%H:%M").time():
            market.append(c)
    return market

# =========================================================
# OPTION CHAIN UTIL
# =========================================================
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
    return safe_float(row.get("strike_price") or row.get("strikePrice") or row.get("strike") or row.get("sp"), 0.0)

def get_row_oich(row):
    return safe_float(
        row.get("oich")
        or row.get("oi_change")
        or row.get("oiChange")
        or row.get("open_interest_change")
        or row.get("openInterestChange"),
        0.0
    )

def split_ce_pe(rows):
    ce_rows, pe_rows = [], []
    for row in rows:
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

def get_four_strikes_snapshot(option_chain_resp, ltp):
    rows = get_option_rows(option_chain_resp)
    ce_rows, pe_rows = split_ce_pe(rows)

    ce_map = {}
    pe_map = {}

    for r in ce_rows:
        strike = get_row_strike(r)
        if strike > 0:
            ce_map[strike] = get_row_oich(r)

    for r in pe_rows:
        strike = get_row_strike(r)
        if strike > 0:
            pe_map[strike] = get_row_oich(r)

    strikes = sorted(set(ce_map.keys()) | set(pe_map.keys()))
    if not strikes:
        return []

    nearest_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - ltp))
    start = max(0, nearest_idx - 1)
    end = min(len(strikes), start + 4)
    start = max(0, end - 4)
    selected = strikes[start:end]

    out = []
    for strike in selected:
        ce = ce_map.get(strike, 0.0)
        pe = pe_map.get(strike, 0.0)
        out.append({
            "strike": strike,
            "ce": ce,
            "pe": pe,
            "ce_text": f"{human_oi(ce)} {arrow_from_value(ce)}",
            "pe_text": f"{human_oi(pe)} {arrow_from_value(pe)}",
        })
    out.sort(key=lambda x: x["strike"])
    return out

def classify_bias(snapshot, side):
    if not snapshot:
        return "NO DATA", "WAIT"

    strong_count = 0
    normal_count = 0
    weak_count = 0

    for r in snapshot:
        ce = r["ce"]
        pe = r["pe"]

        if side == "SELL":
            if ce > 0 and pe < 0:
                strong_count += 1
            elif ce > 0:
                normal_count += 1
            elif ce < 0 and pe > 0:
                weak_count += 1

        elif side == "BUY":
            if pe > 0 and ce < 0:
                strong_count += 1
            elif pe > 0:
                normal_count += 1
            elif pe < 0 and ce > 0:
                weak_count += 1

    if strong_count >= 2:
        return ("🔴 STRONG SELL", "STAY SELL") if side == "SELL" else ("🟢 STRONG BUY", "STAY BUY")
    if normal_count >= 2:
        return ("🟠 SELL", "HOLD SELL") if side == "SELL" else ("🟢 BUY", "HOLD BUY")
    if weak_count >= 2:
        return ("⚠️ SELL WEAKENING", "TRAIL SL") if side == "SELL" else ("⚠️ BUY WEAKENING", "TRAIL SL")
    return ("🟢 REVERSAL", "EXIT SELL") if side == "SELL" else ("🔴 REVERSAL", "EXIT BUY")

# =========================================================
# STRATEGIES
# =========================================================
def get_first_two_15m_candles(fyers, symbol):
    market = get_market_candles_for_day(fyers, symbol, "15")
    if len(market) < 2:
        return None, None
    return market[0], market[1]

def setup_valid_15m(c1, c2):
    if not c1 or not c2 or c1["close"] <= 0:
        return False, 0.0
    range_pct = ((c1["high"] - c1["low"]) / c1["close"]) * 100.0
    inside = c2["high"] < c1["high"] and c2["low"] > c1["low"]
    return (range_pct < FIRST_15M_MAX_RANGE_PCT and inside), range_pct

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
    first_5m = today_5m[0] if today_5m else None
    return prev_day_high, first_5m

def setup_gapup_sell(prev_day_high, first_5m):
    if prev_day_high is None or not first_5m or first_5m["close"] <= 0:
        return False, 0.0, 0.0
    open_above_pdh = first_5m["open"] > prev_day_high
    range_pct = ((first_5m["high"] - first_5m["low"]) / first_5m["close"]) * 100.0
    gap_pct = ((first_5m["open"] - prev_day_high) / prev_day_high) * 100.0 if prev_day_high > 0 else 0.0
    return (open_above_pdh and range_pct < GAPUP_FIRST_5M_MAX_RANGE_PCT), range_pct, gap_pct

def get_prev_day_30m_candles(fyers, symbol):
    end_dt = now_ist()
    start_dt = end_dt - timedelta(days=7)
    raw = get_history(fyers, symbol, "30", start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    parsed = parse_candles(raw)
    grouped = {}
    for c in parsed:
        grouped.setdefault(c["dt"].date(), []).append(c)
    days = sorted(grouped.keys())
    if len(days) < 2:
        return None, None
    return grouped.get(days[-2], []), grouped.get(days[-1], [])

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

def get_latest_pivot_sell_pattern(curr_day_30m, levels):
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
                "level_name": level_name,
                "level_value": level_value,
                "c1": c1,
                "c2": c2,
                "entry": c2["low"],
                "sl": max(c1["high"], c2["high"]),
            }
    return None

# =========================================================
# SUMMARIES
# =========================================================
def send_gapup_summary_if_due(fyers, symbols):
    global gapup_summary_sent_for_day
    now = now_ist()
    today = today_ist_str()

    if now.time() < datetime.strptime("09:20", "%H:%M").time():
        return
    if gapup_summary_sent_for_day == today:
        return

    found = []
    for symbol in symbols:
        prev_day_high, first_5m = get_prev_day_high_and_today_5m(fyers, symbol)
        ok, _, gap_pct = setup_gapup_sell(prev_day_high, first_5m)
        if ok:
            found.append((display_symbol_name(symbol), round(gap_pct, 2)))

    if found:
        lines = ["⚡Gap up plus⚡"]
        for i, (name, gap_pct) in enumerate(found, start=1):
            lines.append(f"{i}.{name} ({gap_pct}%)")
        send_telegram("\n".join(lines))
    else:
        send_telegram("⚡Gap up plus⚡\nNone")

    gapup_summary_sent_for_day = today

def send_inside15_summary_if_due(fyers, symbols):
    global inside15_summary_sent_for_day
    now = now_ist()
    today = today_ist_str()

    if now.time() < datetime.strptime("09:45", "%H:%M").time():
        return
    if inside15_summary_sent_for_day == today:
        return

    found = []
    for symbol in symbols:
        c1, c2 = get_first_two_15m_candles(fyers, symbol)
        ok, _ = setup_valid_15m(c1, c2)
        if ok:
            found.append(display_symbol_name(symbol))

    if found:
        lines = ["🕯️15 Min Inside Candle🕯️"]
        for i, name in enumerate(found, start=1):
            lines.append(f"{i}.{name}")
        send_telegram("\n".join(lines))
    else:
        send_telegram("🕯️15 Min Inside Candle🕯️\nNone")

    inside15_summary_sent_for_day = today

def send_pivot_watch_summary_if_new(new_names):
    if not new_names:
        return
    lines = ["⛔PIVOT Alert⛔"]
    for name in sorted(new_names):
        lines.append(name)
    send_telegram("\n".join(lines))

# =========================================================
# TRADE MANAGEMENT
# =========================================================
def trade_key(strategy, symbol):
    return f"{today_ist_str()}|{strategy}|{symbol}"

def register_trade(strategy, symbol, side, entry, target, stoploss, oi_snapshot):
    key = trade_key(strategy, symbol)
    if key in active_trades:
        return

    active_trades[key] = {
        "key": key,
        "symbol": symbol,
        "strategy": strategy,
        "side": side,
        "entry": round(entry, 2),
        "target": round(target, 2),
        "stoploss": round(stoploss, 2),
        "entry_time": ist_time_str(),
        "oi_snapshot_entry": oi_snapshot,
        "last_oi_update_ts": 0.0,
    }

def close_trade(trade, result, exit_price, ltp=None):
    entry = trade["entry"]
    side = trade["side"]

    if side == "SELL":
        pl = round(entry - exit_price, 2)
    else:
        pl = round(exit_price - entry, 2)

    closed = {
        "symbol": display_symbol_name(trade["symbol"]),
        "strategy": trade["strategy"],
        "side": side,
        "entry": trade["entry"],
        "target": trade["target"],
        "stoploss": trade["stoploss"],
        "result": result,
        "pl": pl,
        "ltp": round(ltp, 2) if ltp is not None else None,
        "oi_snapshot_entry": trade["oi_snapshot_entry"],
    }
    closed_trades.append(closed)

    if trade["key"] in active_trades:
        del active_trades[trade["key"]]

def manage_trade_by_price(trade, ltp):
    if trade["side"] == "SELL":
        if ltp <= trade["target"]:
            close_trade(trade, "Target 🎯", trade["target"])
            return
        if ltp >= trade["stoploss"]:
            close_trade(trade, "Stoploss 🛑", trade["stoploss"])
            return
    else:
        if ltp >= trade["target"]:
            close_trade(trade, "Target 🎯", trade["target"])
            return
        if ltp <= trade["stoploss"]:
            close_trade(trade, "Stoploss 🛑", trade["stoploss"])
            return

def send_5min_oi_update_for_trade(symbol, ltp, snapshot, side):
    bias, action = classify_bias(snapshot, side)
    lines = [
        "📊 OI CHANGE (5 MIN)",
        "",
        display_symbol_name(symbol),
        f"Spot: {round(ltp, 2)}",
        "",
    ]
    for row in snapshot:
        lines.append(f"{int(row['strike'])}  CE:{row['ce_text']} | PE:{row['pe_text']}")
    lines.append("")
    lines.append(f"Bias   : {bias}")
    lines.append(f"Action : {action}")
    lines.append("")
    lines.append(f"Time   : {ist_time_str()}")
    send_telegram("\n".join(lines))

def maybe_send_oi_update(trade, fyers, ltp):
    now_ts = time.time()
    if now_ts - trade["last_oi_update_ts"] < OI_UPDATE_EVERY_SECONDS:
        return

    option_chain = fetch_option_chain(fyers, trade["symbol"], STRIKECOUNT)
    snapshot = get_four_strikes_snapshot(option_chain, ltp)
    if not snapshot:
        return

    send_5min_oi_update_for_trade(trade["symbol"], ltp, snapshot, trade["side"])
    trade["last_oi_update_ts"] = now_ts

# =========================================================
# END OF DAY
# =========================================================
def build_eod_summary_message(trades):
    lines = ["📘 END OF DAY SUMMARY", ""]
    for t in trades:
        lines.append(t["symbol"])
        lines.append(f"Strategy : {t['strategy']}")
        lines.append(f"Entry    : {round(t['entry'], 2)}")
        lines.append(f"Target   : {round(t['target'], 2)}")
        lines.append(f"Stoploss : {round(t['stoploss'], 2)}")

        if t.get("oi_snapshot_entry"):
            lines.append("")
            lines.append("OI @ ENTRY")
            for row in t["oi_snapshot_entry"]:
                lines.append(f"{int(row['strike'])}  CE:{row['ce_text']} | PE:{row['pe_text']}")

        lines.append("")
        lines.append(f"Result   : {t['result']}")
        if t["result"] == "Day End" and t.get("ltp") is not None:
            lines.append(f"LTP      : {round(t['ltp'], 2)}")
        sign = "+" if t["pl"] > 0 else ""
        lines.append(f"P/L      : {sign}{round(t['pl'], 2)}")
        lines.append("")
        lines.append("")

    return "\n".join(lines).strip()

def close_all_open_trades_day_end(quotes_map):
    for key in list(active_trades.keys()):
        trade = active_trades.get(key)
        if not trade:
            continue
        ltp = safe_float(quotes_map.get(trade["symbol"], {}).get("ltp"), trade["entry"])
        close_trade(trade, "Day End", ltp, ltp=ltp)

def send_eod_summary_if_any():
    if not closed_trades:
        send_telegram("📘 END OF DAY SUMMARY\n\nNo triggered stocks today.")
        return
    send_telegram(build_eod_summary_message(closed_trades))

def reset_next_day_state():
    global gapup_summary_sent_for_day, inside15_summary_sent_for_day
    gapup_summary_sent_for_day = None
    inside15_summary_sent_for_day = None
    pivot_alert_seen_for_day.clear()
    active_trades.clear()
    closed_trades.clear()

# =========================================================
# MAIN
# =========================================================
def main():
    global eod_sent_for_day

    symbols = get_watchlist()

    if SEND_STARTUP_MESSAGE:
        send_telegram(
            "🚀 FINAL FLOW BOT STARTED\n"
            "Gap-up summary + 15m summary + Pivot summary + triggers + 5m OI + EOD summary\n"
            f"15m first candle < {FIRST_15M_MAX_RANGE_PCT}%\n"
            f"Gap-up first 5m candle < {GAPUP_FIRST_5M_MAX_RANGE_PCT}%"
        )

    while True:
        now = now_ist()
        today = today_ist_str()

        if not is_market_open():
            if now.time() > datetime.strptime("15:30", "%H:%M").time() and eod_sent_for_day != today:
                try:
                    fyers = get_fyers()
                    quotes_map = fetch_quotes_map(fyers, symbols)
                    close_all_open_trades_day_end(quotes_map)
                except Exception:
                    pass

                send_eod_summary_if_any()
                eod_sent_for_day = today
                sleep_until_next_market_open()
                reset_next_day_state()
                continue

            sleep_until_next_market_open()
            reset_next_day_state()
            continue

        try:
            fyers = get_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(30)
            continue

        # Summary messages
        send_gapup_summary_if_due(fyers, symbols)
        send_inside15_summary_if_due(fyers, symbols)

        quotes_map = fetch_quotes_map(fyers, symbols)
        new_pivot_names = set()

        for symbol in symbols:
            try:
                ltp = safe_float(quotes_map.get(symbol, {}).get("ltp"), 0.0)
                if ltp <= 0:
                    continue

                # ---------------- GAPUP SELL TRIGGER ----------------
                prev_day_high, first_5m = get_prev_day_high_and_today_5m(fyers, symbol)
                valid_gapup, _, _ = setup_gapup_sell(prev_day_high, first_5m)
                if valid_gapup and first_5m and ltp < first_5m["low"]:
                    option_chain = fetch_option_chain(fyers, symbol, STRIKECOUNT)
                    snapshot = get_four_strikes_snapshot(option_chain, ltp)
                    bias, _ = classify_bias(snapshot, "SELL")

                    entry = first_5m["low"]
                    target = entry - (entry * TARGET_PCT)
                    stoploss = first_5m["high"] * (1 + SL_BUFFER_PCT)

                    if trade_key("GAPUP SELL", symbol) not in active_trades:
                        send_telegram(
                            "\n".join([
                                "⚡ GAPUP SELL TRIGGER ⚡",
                                "",
                                display_symbol_name(symbol),
                                "",
                                f"Entry : {round(entry, 2)} ↓ Break",
                                f"Spot  : {round(ltp, 2)}",
                                f"Target: {round(target, 2)}",
                                f"SL    : {round(stoploss, 2)}",
                                "",
                                f"Signal: {bias}",
                                f"Time  : {ist_time_str()}",
                            ])
                        )
                        register_trade("GAPUP SELL", symbol, "SELL", entry, target, stoploss, snapshot)

                # ---------------- 15M BUY / SELL TRIGGER ----------------
                c1_15, c2_15 = get_first_two_15m_candles(fyers, symbol)
                valid_15m, _ = setup_valid_15m(c1_15, c2_15)

                if valid_15m and c1_15:
                    # SELL
                    if ltp < c1_15["low"]:
                        option_chain = fetch_option_chain(fyers, symbol, STRIKECOUNT)
                        snapshot = get_four_strikes_snapshot(option_chain, ltp)
                        bias, _ = classify_bias(snapshot, "SELL")

                        entry = c1_15["low"]
                        target = entry - (entry * TARGET_PCT)
                        stoploss = c1_15["high"] * (1 + SL_BUFFER_PCT)

                        if trade_key("15M BREAKOUT SELL", symbol) not in active_trades:
                            send_telegram(
                                "\n".join([
                                    "🕯️ 15M BREAKOUT SELL 🕯️",
                                    "",
                                    display_symbol_name(symbol),
                                    "",
                                    f"Entry : {round(entry, 2)} ↓ Break",
                                    f"Spot  : {round(ltp, 2)}",
                                    f"Target: {round(target, 2)}",
                                    f"SL    : {round(stoploss, 2)}",
                                    "",
                                    f"Signal: {bias}",
                                    f"Time  : {ist_time_str()}",
                                ])
                            )
                            register_trade("15M BREAKOUT SELL", symbol, "SELL", entry, target, stoploss, snapshot)

                    # BUY
                    elif ltp > c1_15["high"]:
                        option_chain = fetch_option_chain(fyers, symbol, STRIKECOUNT)
                        snapshot = get_four_strikes_snapshot(option_chain, ltp)
                        bias, _ = classify_bias(snapshot, "BUY")

                        entry = c1_15["high"]
                        target = entry + (entry * TARGET_PCT)
                        stoploss = c1_15["low"] * (1 - SL_BUFFER_PCT)

                        if trade_key("15M BREAKOUT BUY", symbol) not in active_trades:
                            send_telegram(
                                "\n".join([
                                    "🕯️ 15M BREAKOUT BUY 🕯️",
                                    "",
                                    display_symbol_name(symbol),
                                    "",
                                    f"Entry : {round(entry, 2)} ↑ Break",
                                    f"Spot  : {round(ltp, 2)}",
                                    f"Target: {round(target, 2)}",
                                    f"SL    : {round(stoploss, 2)}",
                                    "",
                                    f"Signal: {bias}",
                                    f"Time  : {ist_time_str()}",
                                ])
                            )
                            register_trade("15M BREAKOUT BUY", symbol, "BUY", entry, target, stoploss, snapshot)

                # ---------------- PIVOT SELL SUMMARY + TRIGGER ----------------
                prev_day_30m, curr_day_30m = get_prev_day_30m_candles(fyers, symbol)
                levels = calc_pivot_levels(prev_day_30m)
                pattern = get_latest_pivot_sell_pattern(curr_day_30m, levels)

                if pattern:
                    name = display_symbol_name(symbol)
                    if name not in pivot_alert_seen_for_day:
                        new_pivot_names.add(name)

                    if ltp <= pattern["entry"]:
                        option_chain = fetch_option_chain(fyers, symbol, STRIKECOUNT)
                        snapshot = get_four_strikes_snapshot(option_chain, ltp)
                        bias, _ = classify_bias(snapshot, "SELL")

                        entry = pattern["entry"]
                        target = entry - (entry * TARGET_PCT)
                        stoploss = pattern["sl"] * (1 + SL_BUFFER_PCT)

                        if trade_key("PIVOT SELL", symbol) not in active_trades:
                            send_telegram(
                                "\n".join([
                                    "⛔ PIVOT SELL TRIGGER ⛔",
                                    "",
                                    name,
                                    "",
                                    f"Level : {pattern['level_name']} @ {round(pattern['level_value'], 2)}",
                                    f"Entry : {round(entry, 2)} ↓ Break",
                                    f"Spot  : {round(ltp, 2)}",
                                    f"Target: {round(target, 2)}",
                                    f"SL    : {round(stoploss, 2)}",
                                    "",
                                    f"Signal: {bias}",
                                    f"Time  : {ist_time_str()}",
                                ])
                            )
                            register_trade("PIVOT SELL", symbol, "SELL", entry, target, stoploss, snapshot)

                # ---------------- ACTIVE TRADE MGMT + OI UPDATE ----------------
                for key in list(active_trades.keys()):
                    trade = active_trades.get(key)
                    if not trade or trade["symbol"] != symbol:
                        continue
                    manage_trade_by_price(trade, ltp)
                    if key in active_trades:
                        maybe_send_oi_update(active_trades[key], fyers, ltp)

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        if new_pivot_names:
            pivot_alert_seen_for_day.update(new_pivot_names)
            send_pivot_watch_summary_if_new(new_pivot_names)

        time.sleep(max(5, POLL_SECONDS))


if __name__ == "__main__":
    main()

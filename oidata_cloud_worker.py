import os
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from collections import deque
from fyers_apiv3 import fyersModel

# ================= ENV =================
CLIENT_ID = os.getenv("FYERS_CLIENT_ID")
ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NSE_HOLIDAYS_RAW = os.getenv(
    "NSE_HOLIDAYS",
    "2026-01-26,2026-03-03,2026-03-26,2026-03-31,2026-04-03,2026-04-14,2026-05-01,2026-05-28,2026-06-26,2026-09-14,2026-10-02,2026-10-20,2026-11-10,2026-11-24,2026-12-25"
).strip()

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
if not TELEGRAM_TOKEN or not CHAT_ID:
    raise Exception("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

# ================= SETTINGS =================
SYMBOLS = [
    "NSE:ADANIENT-EQ","NSE:ADANIPORTS-EQ","NSE:APOLLOHOSP-EQ","NSE:ASIANPAINT-EQ",
    "NSE:AXISBANK-EQ","NSE:BAJAJ-AUTO-EQ","NSE:BAJFINANCE-EQ","NSE:BAJAJFINSV-EQ",
    "NSE:BEL-EQ","NSE:BHARTIARTL-EQ","NSE:BPCL-EQ","NSE:BRITANNIA-EQ",
    "NSE:CIPLA-EQ","NSE:COALINDIA-EQ","NSE:DRREDDY-EQ","NSE:EICHERMOT-EQ",
    "NSE:ETERNAL-EQ","NSE:GRASIM-EQ","NSE:HCLTECH-EQ","NSE:HDFCBANK-EQ",
    "NSE:HDFCLIFE-EQ","NSE:HEROMOTOCO-EQ","NSE:HINDALCO-EQ","NSE:HINDUNILVR-EQ",
    "NSE:ICICIBANK-EQ","NSE:INDIGO-EQ","NSE:INFY-EQ","NSE:ITC-EQ",
    "NSE:JSWSTEEL-EQ","NSE:KOTAKBANK-EQ","NSE:LT-EQ","NSE:M&M-EQ",
    "NSE:MARUTI-EQ","NSE:NESTLEIND-EQ","NSE:NTPC-EQ","NSE:ONGC-EQ",
    "NSE:POWERGRID-EQ","NSE:RELIANCE-EQ","NSE:SBILIFE-EQ","NSE:SHRIRAMFIN-EQ",
    "NSE:SBIN-EQ","NSE:SUNPHARMA-EQ","NSE:TATACONSUM-EQ","NSE:TATAMOTORS-EQ",
    "NSE:TATASTEEL-EQ","NSE:TCS-EQ","NSE:TECHM-EQ","NSE:TITAN-EQ",
    "NSE:TRENT-EQ","NSE:WIPRO-EQ"
]

IST = timezone(timedelta(hours=5, minutes=30))

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "12"))
FIRST_15M_MAX_RANGE_PCT = float(os.getenv("FIRST_15M_MAX_RANGE_PCT", "2.0"))
GAPUP_FIRST_5M_MAX_RANGE_PCT = float(os.getenv("GAPUP_FIRST_5M_MAX_RANGE_PCT", "1.5"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.01"))
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.001"))
OI_UPDATE_EVERY_SECONDS = int(os.getenv("OI_UPDATE_EVERY_SECONDS", "300"))

# Debug stocks for late-start verification
DEBUG_STOCKS = {"NSE:M&M-EQ", "NSE:AXISBANK-EQ", "NSE:INFY-EQ"}
SEND_DEBUG_TO_TELEGRAM = os.getenv("SEND_DEBUG_TO_TELEGRAM", "false").strip().lower() == "true"
DEBUG_PRINT_DONE_FOR_DAY = False

# ================= HOLIDAYS / SCHEDULER =================
def get_holiday_set():
    out = set()
    for part in NSE_HOLIDAYS_RAW.replace(";", ",").split(","):
        p = part.strip()
        if p:
            out.add(p)
    return out

HOLIDAYS = get_holiday_set()

def now_ist():
    return datetime.now(IST)

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def log(msg):
    print(f"[{ist_time_str()}] {msg}", flush=True)

def is_weekend(dt_obj):
    return dt_obj.weekday() >= 5

def is_holiday(dt_obj):
    return dt_obj.strftime("%Y-%m-%d") in HOLIDAYS

def is_market_day(dt_obj):
    return (not is_weekend(dt_obj)) and (not is_holiday(dt_obj))

def market_open_time(dt_obj):
    return dt_obj.replace(hour=9, minute=15, second=0, microsecond=0)

def is_market_open():
    now = now_ist()
    if not is_market_day(now):
        return False
    return dtime(9, 15) <= now.time() <= dtime(15, 30)

def next_market_open_datetime():
    now = now_ist()

    if is_market_day(now) and now < market_open_time(now):
        return market_open_time(now)

    candidate = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    while not is_market_day(candidate):
        candidate = (candidate + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    return candidate

_last_sleep_log_for = None

def sleep_until_next_market_open():
    global _last_sleep_log_for
    nxt = next_market_open_datetime()
    nxt_key = nxt.strftime("%Y-%m-%d %H:%M:%S")

    if _last_sleep_log_for != nxt_key:
        log(f"Market closed. Sleeping until {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
        _last_sleep_log_for = nxt_key

    while True:
        remaining = (nxt - now_ist()).total_seconds()
        if remaining <= 1:
            _last_sleep_log_for = None
            break
        time.sleep(min(60, max(1, int(remaining))))

# ================= RATE LIMIT =================
class RateLimiter:
    def __init__(self):
        self.sec = deque()
        self.min = deque()

    def wait(self):
        now = time.time()

        while self.sec and now - self.sec[0] > 1:
            self.sec.popleft()
        while self.min and now - self.min[0] > 60:
            self.min.popleft()

        if len(self.sec) >= 8:
            time.sleep(0.2)
            return self.wait()

        if len(self.min) >= 150:
            time.sleep(1)
            return self.wait()

        self.sec.append(time.time())
        self.min.append(time.time())

rl = RateLimiter()

# ================= FYERS =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

def get_quotes():
    rl.wait()
    data = fyers.quotes({"symbols": ",".join(SYMBOLS)})
    out = {}
    for item in data.get("d", []):
        try:
            out[item["n"]] = float(item["v"]["lp"])
        except Exception:
            pass
    return out

def get_history(symbol, resolution, days=5):
    rl.wait()
    today = now_ist().strftime("%Y-%m-%d")
    past = (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d")
    data = fyers.history({
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": past,
        "range_to": today,
        "cont_flag": "1"
    })
    return data.get("candles", [])

def get_option_chain(symbol):
    rl.wait()
    return fyers.optionchain({"symbol": symbol, "strikecount": STRIKECOUNT})

# ================= TELEGRAM =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=20
        )
    except Exception as e:
        log(f"Telegram error: {e}")

# ================= UTIL =================
def name(sym):
    return sym.split(":")[1].replace("-EQ", "")

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def human_oi(v):
    v = float(v)
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

def arrow(v):
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

def candle_dt(ts):
    return datetime.fromtimestamp(ts, IST)

def fmt_candle(c):
    if c is None:
        return "None"
    dt = candle_dt(c[0]).strftime("%Y-%m-%d %H:%M")
    return f"{dt} | O:{c[1]} H:{c[2]} L:{c[3]} C:{c[4]}"

# ================= CACHE =================
cache = {
    "quotes": {},
    "5m": {},
    "15m": {},
    "30m": {},
    "daily": {},
}

gapup_summary_sent_for_day = None
inside15_summary_sent_for_day = None
pivot_alert_seen_for_day = set()
eod_sent_for_day = None

gap_setup_done_for_day = None
full_setup_done_for_day = None

active_trades = {}
closed_trades = []

# ================= CANDLE HELPERS =================
def get_today_candles(symbol, tf):
    candles = cache[tf].get(symbol, [])
    today = today_ist_str()

    out = []
    for c in candles:
        try:
            dt = candle_dt(c[0])
            if dt.strftime("%Y-%m-%d") == today:
                out.append(c)
        except Exception:
            pass

    out.sort(key=lambda x: x[0])
    return out

def get_first_5m_candle_today(symbol):
    today_5m = get_today_candles(symbol, "5m")
    return today_5m[0] if len(today_5m) >= 1 else None

def get_first_two_15m_candles_today(symbol):
    today_15m = get_today_candles(symbol, "15m")
    if len(today_15m) >= 2:
        return today_15m[0], today_15m[1]
    return None, None

def get_today_30m_candles(symbol):
    return get_today_candles(symbol, "30m")

def get_previous_daily_candle(symbol):
    daily = cache["daily"].get(symbol, [])
    parsed = []

    for c in daily:
        try:
            dt = candle_dt(c[0])
            parsed.append((dt, c))
        except Exception:
            pass

    parsed.sort(key=lambda x: x[0])

    today = today_ist_str()
    prev = [c for dt, c in parsed if dt.strftime("%Y-%m-%d") < today]
    return prev[-1] if prev else None

# ================= DEBUG =================
def send_or_log_debug(msg):
    log(msg)
    if SEND_DEBUG_TO_TELEGRAM:
        send(msg)

def debug_stock_history(symbol):
    if symbol not in DEBUG_STOCKS:
        return

    prev_day = get_previous_daily_candle(symbol)
    first_5m = get_first_5m_candle_today(symbol)
    first_15m, second_15m = get_first_two_15m_candles_today(symbol)
    today_30m = get_today_30m_candles(symbol)

    lines = [
        f"DEBUG {name(symbol)}",
        "",
        f"Prev Daily : {fmt_candle(prev_day)}",
        f"First 5m   : {fmt_candle(first_5m)}",
        f"First 15m  : {fmt_candle(first_15m)}",
        f"Second 15m : {fmt_candle(second_15m)}",
        f"30m Count  : {len(today_30m)}",
    ]

    if prev_day is not None and first_5m is not None:
        prev_high = prev_day[2]
        o, h, l, c = first_5m[1], first_5m[2], first_5m[3], first_5m[4]
        gap_pct = ((o - prev_high) / prev_high) * 100 if prev_high else 0
        rng_pct = ((h - l) / c) * 100 if c else 0
        lines.append("")
        lines.append(f"GapCheck   : Open={o} PrevHigh={prev_high} Gap%={gap_pct:.2f} Range%={rng_pct:.2f}")
        lines.append(f"GapValid   : {o > prev_high and rng_pct < GAPUP_FIRST_5M_MAX_RANGE_PCT}")

    if first_15m is not None and second_15m is not None:
        h1, l1, c1 = first_15m[2], first_15m[3], first_15m[4]
        h2, l2 = second_15m[2], second_15m[3]
        rng = ((h1 - l1) / c1) * 100 if c1 else 0
        inside = h2 < h1 and l2 > l1
        lines.append("")
        lines.append(f"15mCheck   : Range%={rng:.2f} Inside={inside}")
        lines.append(f"15mValid   : {rng < FIRST_15M_MAX_RANGE_PCT and inside}")

    send_or_log_debug("\n".join(lines))

def debug_selected_stocks_once():
    global DEBUG_PRINT_DONE_FOR_DAY
    if DEBUG_PRINT_DONE_FOR_DAY:
        return
    for s in DEBUG_STOCKS:
        if s in SYMBOLS:
            debug_stock_history(s)
    DEBUG_PRINT_DONE_FOR_DAY = True

# ================= DATA LOADERS =================
def load_gapup_caches():
    log("Loading gap-up caches...")
    for batch in chunk(SYMBOLS, 5):
        for s in batch:
            cache["daily"][s] = get_history(s, "D", 10)
            cache["5m"][s] = get_history(s, 5, 5)
        time.sleep(1)
    log("Gap-up caches loaded.")

def load_full_caches():
    log("Loading 15m/30m caches...")
    for batch in chunk(SYMBOLS, 5):
        for s in batch:
            cache["15m"][s] = get_history(s, 15, 5)
            cache["30m"][s] = get_history(s, 30, 5)
        time.sleep(1)
    log("15m/30m caches loaded.")

def refresh_quotes_only():
    cache["quotes"] = get_quotes()

# ================= OI =================
def get_oi_snapshot(symbol, ltp):
    data = get_option_chain(symbol)
    rows = data.get("data", {}).get("optionsChain", [])
    if not rows:
        return []

    strikes = sorted(set(r["strike_price"] for r in rows if r.get("strike_price") is not None))
    if not strikes:
        return []

    near = min(strikes, key=lambda x: abs(x - ltp))
    idx = strikes.index(near)
    selected = strikes[max(0, idx - 1):idx + 3]

    result = []
    for s in selected:
        ce = 0.0
        pe = 0.0
        for r in rows:
            if r.get("strike_price") == s:
                if r.get("type") == "CE":
                    ce = float(r.get("oi_change", 0))
                if r.get("type") == "PE":
                    pe = float(r.get("oi_change", 0))
        result.append({
            "strike": s,
            "ce": ce,
            "pe": pe,
            "ce_text": f"{human_oi(ce)} {arrow(ce)}",
            "pe_text": f"{human_oi(pe)} {arrow(pe)}",
        })

    return result

def classify_bias(snapshot, side):
    strong = 0
    normal = 0
    weak = 0

    for row in snapshot:
        ce = row["ce"]
        pe = row["pe"]

        if side == "SELL":
            if ce > 0 and pe < 0:
                strong += 1
            elif ce > 0:
                normal += 1
            elif ce < 0 and pe > 0:
                weak += 1
        else:
            if pe > 0 and ce < 0:
                strong += 1
            elif pe > 0:
                normal += 1
            elif pe < 0 and ce > 0:
                weak += 1

    if strong >= 2:
        return ("🔴 STRONG SELL", "HOLD SELL") if side == "SELL" else ("🟢 STRONG BUY", "HOLD BUY")
    if normal >= 2:
        return ("🟠 SELL", "HOLD SELL") if side == "SELL" else ("🟢 BUY", "HOLD BUY")
    if weak >= 2:
        return ("⚠️ SELL WEAKENING", "TRAIL SL") if side == "SELL" else ("⚠️ BUY WEAKENING", "TRAIL SL")
    return ("🟢 REVERSAL", "EXIT SELL") if side == "SELL" else ("🔴 REVERSAL", "EXIT BUY")

# ================= SUMMARIES =================
def send_gapup_summary_if_due():
    global gapup_summary_sent_for_day

    if now_ist().time() < dtime(9, 20):
        return
    if gapup_summary_sent_for_day == today_ist_str():
        return

    found = []

    for s in SYMBOLS:
        prev_day = get_previous_daily_candle(s)
        c1 = get_first_5m_candle_today(s)

        if prev_day is None or c1 is None:
            continue

        prev_day_high = prev_day[2]
        o, h, l, c = c1[1], c1[2], c1[3], c1[4]

        if c <= 0:
            continue

        gap_pct = ((o - prev_day_high) / prev_day_high) * 100 if prev_day_high else 0
        rng_pct = ((h - l) / c) * 100

        log(f"GAPCHK {name(s)} | Open:{o} PrevHigh:{prev_day_high} Gap%:{gap_pct:.2f} Range%:{rng_pct:.2f}")

        if o > prev_day_high and rng_pct < GAPUP_FIRST_5M_MAX_RANGE_PCT:
            found.append((name(s), round(gap_pct, 2)))

    if found:
        msg = "⚡Gap up plus⚡\n\n" + "\n".join(
            [f"{i}.{n} ({g}%)" for i, (n, g) in enumerate(found, start=1)]
        )
    else:
        msg = "⚡Gap up plus⚡\n\nNone"

    send(msg)
    gapup_summary_sent_for_day = today_ist_str()

def send_inside15_summary_if_due():
    global inside15_summary_sent_for_day

    if now_ist().time() < dtime(9, 45):
        return
    if inside15_summary_sent_for_day == today_ist_str():
        return

    found = []

    for s in SYMBOLS:
        c1, c2 = get_first_two_15m_candles_today(s)
        if c1 is None or c2 is None:
            continue

        h1, l1, c1c = c1[2], c1[3], c1[4]
        h2, l2 = c2[2], c2[3]

        if c1c <= 0:
            continue

        rng = ((h1 - l1) / c1c) * 100
        inside = h2 < h1 and l2 > l1

        log(f"15MCHK {name(s)} | H1:{h1} L1:{l1} H2:{h2} L2:{l2} Range%:{rng:.2f} Inside:{inside}")

        if rng < FIRST_15M_MAX_RANGE_PCT and inside:
            found.append(name(s))

    if found:
        msg = "🕯️15 Min Inside Candle🕯️\n\n" + "\n".join(
            [f"{i}.{n}" for i, n in enumerate(found, start=1)]
        )
    else:
        msg = "🕯️15 Min Inside Candle🕯️\n\nNone"

    send(msg)
    inside15_summary_sent_for_day = today_ist_str()

def send_pivot_watch_summary_if_new(new_names):
    if not new_names:
        return
    msg = "⛔PIVOT Alert⛔\n\n" + "\n".join(sorted(new_names))
    send(msg)

# ================= TRADE =================
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
        "oi_snapshot_entry": oi_snapshot,
        "last_oi_update_ts": time.time(),
    }

def close_trade(trade, result, exit_price, ltp=None):
    if trade["side"] == "SELL":
        pl = round(trade["entry"] - exit_price, 2)
    else:
        pl = round(exit_price - trade["entry"], 2)

    closed_trades.append({
        "symbol": name(trade["symbol"]),
        "strategy": trade["strategy"],
        "side": trade["side"],
        "entry": trade["entry"],
        "target": trade["target"],
        "stoploss": trade["stoploss"],
        "result": result,
        "ltp": ltp,
        "pl": pl,
        "oi_snapshot_entry": trade["oi_snapshot_entry"],
    })
    active_trades.pop(trade["key"], None)

def manage_trade_by_price(trade, ltp):
    if trade["side"] == "SELL":
        if ltp <= trade["target"]:
            send(f"🎯 TARGET HIT\n\n{name(trade['symbol'])}\n\nStrategy : {trade['strategy']}")
            close_trade(trade, "Target 🎯", trade["target"])
            return
        if ltp >= trade["stoploss"]:
            send(f"🛑 STOPLOSS HIT\n\n{name(trade['symbol'])}\n\nStrategy : {trade['strategy']}")
            close_trade(trade, "Stoploss 🛑", trade["stoploss"])
            return
    else:
        if ltp >= trade["target"]:
            send(f"🎯 TARGET HIT\n\n{name(trade['symbol'])}\n\nStrategy : {trade['strategy']}")
            close_trade(trade, "Target 🎯", trade["target"])
            return
        if ltp <= trade["stoploss"]:
            send(f"🛑 STOPLOSS HIT\n\n{name(trade['symbol'])}\n\nStrategy : {trade['strategy']}")
            close_trade(trade, "Stoploss 🛑", trade["stoploss"])
            return

def maybe_send_oi_update(trade, ltp):
    if time.time() - trade["last_oi_update_ts"] < OI_UPDATE_EVERY_SECONDS:
        return

    snapshot = get_oi_snapshot(trade["symbol"], ltp)
    if not snapshot:
        return

    bias, action = classify_bias(snapshot, trade["side"])
    lines = [
        "📊 OI UPDATE (5 MIN)",
        "",
        name(trade["symbol"]),
        "",
        f"Strategy : {trade['strategy']}",
        f"Type     : {trade['side']}",
        "",
        f"Spot: {round(ltp, 2)}",
        "",
    ]
    for row in snapshot:
        lines.append(f"{int(row['strike'])}  CE:{row['ce_text']} | PE:{row['pe_text']}")
    lines += [
        "",
        f"Bias   : {bias}",
        f"Action : {action}",
        "",
        f"Time   : {ist_time_str()}",
    ]
    send("\n".join(lines))
    trade["last_oi_update_ts"] = time.time()

# ================= EOD =================
def close_all_open_trades_day_end():
    for key in list(active_trades.keys()):
        trade = active_trades.get(key)
        if not trade:
            continue
        ltp = float(cache["quotes"].get(trade["symbol"], trade["entry"]))
        close_trade(trade, "Day End", ltp, ltp=round(ltp, 2))

def build_eod_summary():
    if not closed_trades:
        return "📘 END OF DAY SUMMARY\n\nNo triggered stocks today."

    lines = ["📘 END OF DAY SUMMARY", ""]
    for t in closed_trades:
        lines += [
            t["symbol"],
            f"Strategy : {t['strategy']}",
            f"Type     : {t['side']}",
            "",
            f"Entry    : {t['entry']}",
            f"Target   : {t['target']}",
            f"Stoploss : {t['stoploss']}",
            "",
        ]

        if t.get("oi_snapshot_entry"):
            lines.append("OI @ ENTRY")
            for row in t["oi_snapshot_entry"]:
                lines.append(f"{int(row['strike'])}  CE:{row['ce_text']} | PE:{row['pe_text']}")
            lines.append("")

        lines.append(f"Result   : {t['result']}")
        if t["result"] == "Day End" and t.get("ltp") is not None:
            lines.append(f"LTP      : {t['ltp']}")
        sign = "+" if t["pl"] > 0 else ""
        lines.append(f"P/L      : {sign}{t['pl']}")
        lines.append("")
        lines.append("")

    return "\n".join(lines).strip()

def reset_next_day_state():
    global gapup_summary_sent_for_day, inside15_summary_sent_for_day
    global gap_setup_done_for_day, full_setup_done_for_day, DEBUG_PRINT_DONE_FOR_DAY

    gapup_summary_sent_for_day = None
    inside15_summary_sent_for_day = None
    gap_setup_done_for_day = None
    full_setup_done_for_day = None
    DEBUG_PRINT_DONE_FOR_DAY = False

    pivot_alert_seen_for_day.clear()
    active_trades.clear()
    closed_trades.clear()

# ================= STRATEGY CHECKS =================
def check_gapup_sell(symbol, ltp):
    prev_day = get_previous_daily_candle(symbol)
    c1 = get_first_5m_candle_today(symbol)

    if prev_day is None or c1 is None:
        return

    prev_day_high = prev_day[2]
    o, h, low, c = c1[1], c1[2], c1[3], c1[4]
    if c <= 0:
        return

    rng_pct = ((h - low) / c) * 100
    valid = o > prev_day_high and rng_pct < GAPUP_FIRST_5M_MAX_RANGE_PCT

    if valid and ltp < low and trade_key("Gap-Up Breakdown", symbol) not in active_trades:
        oi_snapshot = get_oi_snapshot(symbol, ltp)
        bias, action = classify_bias(oi_snapshot, "SELL")
        entry = low
        target = entry * (1 - TARGET_PCT)
        stoploss = h * (1 + SL_BUFFER_PCT)

        msg = "\n".join([
            "⚡ GAP UP SELL ⚡",
            "",
            name(symbol),
            "",
            "Strategy : Gap-Up Breakdown",
            "Type     : SELL",
            "",
            f"Entry : {round(entry, 2)} ↓ Break",
            f"Spot  : {round(ltp, 2)}",
            f"Target: {round(target, 2)}",
            f"SL    : {round(stoploss, 2)}",
            "",
            f"OI Bias : {bias}",
            f"Action  : {action}",
            "",
            f"Time    : {ist_time_str()}",
        ])
        send(msg)
        register_trade("Gap-Up Breakdown", symbol, "SELL", entry, target, stoploss, oi_snapshot)

def check_15m_breakout(symbol, ltp):
    c1, c2 = get_first_two_15m_candles_today(symbol)
    if c1 is None or c2 is None:
        return

    h1, l1, c1c = c1[2], c1[3], c1[4]
    h2, l2 = c2[2], c2[3]
    if c1c <= 0:
        return

    rng = ((h1 - l1) / c1c) * 100
    valid = rng < FIRST_15M_MAX_RANGE_PCT and h2 < h1 and l2 > l1
    if not valid:
        return

    if ltp > h1 and trade_key("15 Min Breakout", symbol) not in active_trades:
        oi_snapshot = get_oi_snapshot(symbol, ltp)
        bias, action = classify_bias(oi_snapshot, "BUY")
        entry = h1
        target = entry * (1 + TARGET_PCT)
        stoploss = l1 * (1 - SL_BUFFER_PCT)

        msg = "\n".join([
            "🕯️ 15M BREAKOUT 🕯️",
            "",
            name(symbol),
            "",
            "Strategy : 15 Min Inside Candle Breakout",
            "Type     : BUY",
            "",
            f"Entry : {round(entry, 2)} ↑ Break",
            f"Spot  : {round(ltp, 2)}",
            f"Target: {round(target, 2)}",
            f"SL    : {round(stoploss, 2)}",
            "",

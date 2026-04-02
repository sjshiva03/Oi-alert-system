import os
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
CHAT_ID = (os.getenv("CHAT_ID") or "").strip()

WATCHLIST_RAW = (os.getenv("WATCHLIST") or "").strip()

AFTER_MARKET_RUN = (os.getenv("AFTER_MARKET_RUN", "true").strip().lower() == "true")

GAPUP_MIN_PCT = float(os.getenv("GAPUP_MIN_PCT", "0.0"))
GAPUP_CANDLE_MAX_PCT = float(os.getenv("GAPUP_CANDLE_MAX_PCT", "1.5"))
INSIDE15_FIRST_CANDLE_MAX_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MAX_PCT", "2.0"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "1.0")) / 100.0
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.1")) / 100.0
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))

NSE_HOLIDAYS_RAW = (os.getenv("NSE_HOLIDAYS") or "").strip()

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing CLIENT_ID or ACCESS_TOKEN")

if not WATCHLIST_RAW:
    raise Exception("Missing WATCHLIST. Example: WATCHLIST=RELIANCE,TCS,HDFCBANK,ICICIBANK,INFY,M&M")

# ================= WATCHLIST =================
def convert_symbol(sym: str) -> str:
    s = sym.strip().upper()
    if not s:
        return ""
    if ":" in s:
        return s
    if s in {"NIFTY", "NIFTY50"}:
        return "NSE:NIFTY50-INDEX"
    if s == "BANKNIFTY":
        return "NSE:NIFTYBANK-INDEX"
    return f"NSE:{s}-EQ"

SYMBOLS = [convert_symbol(s) for s in WATCHLIST_RAW.split(",") if s.strip()]

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

def send_long_message(text: str, chunk_size: int = 3500):
    if not text:
        return
    while len(text) > chunk_size:
        cut = text.rfind("\n", 0, chunk_size)
        if cut == -1:
            cut = chunk_size
        send(text[:cut])
        text = text[cut:].lstrip()
    if text:
        send(text)

def short_name(symbol: str) -> str:
    right = symbol.split(":")[1]
    return right.replace("-EQ", "").replace("-INDEX", "")

def candle_dt(ts: int):
    return datetime.fromtimestamp(ts, IST)

def pct_range(high: float, low: float, close: float) -> float:
    if close == 0:
        return 0.0
    return ((high - low) / close) * 100.0

def dedupe_candles_by_ts(candles):
    seen = {}
    for c in candles:
        try:
            ts = int(c[0])
            seen[ts] = c
        except Exception:
            pass
    out = list(seen.values())
    out.sort(key=lambda x: x[0])
    return out

# ================= MARKET TIME =================
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

def is_market_open():
    now = now_ist()
    return is_market_day(now) and dtime(9, 15) <= now.time() <= dtime(15, 30)

def next_market_open_datetime():
    now = now_ist()
    if is_market_day(now) and now.time() < dtime(9, 15):
        return now.replace(hour=9, minute=15, second=0, microsecond=0)

    nxt = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    while not is_market_day(nxt):
        nxt = (nxt + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    return nxt

def sleep_until_next_market_open():
    nxt = next_market_open_datetime()
    log(f"Sleeping until {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
    while True:
        rem = (nxt - now_ist()).total_seconds()
        if rem <= 1:
            return
        time.sleep(min(60, max(1, int(rem))))

# ================= ANALYSIS DATE =================
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

def check_auth():
    profile = fyers.get_profile()
    if profile.get("s") != "ok":
        raise Exception(f"FYERS auth failed: {profile}")
    return profile

def get_history(symbol, resolution, days=20):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": now_ist().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }
    try:
        data = fyers.history(data=payload)
    except TypeError:
        data = fyers.history(payload)
    except Exception as e:
        log(f"HISTORY ERROR {symbol} {resolution}: {e}")
        return []
    return dedupe_candles_by_ts(data.get("candles", []))

# ================= CANDLE HELPERS =================
def get_analysis_day_candles(symbol, resolution, days=20):
    candles = get_history(symbol, resolution, days)
    target_day = analysis_date_str()

    out = []
    for c in candles:
        try:
            if candle_dt(c[0]).strftime("%Y-%m-%d") == target_day:
                out.append(c)
        except Exception:
            pass

    out = dedupe_candles_by_ts(out)
    out.sort(key=lambda x: x[0])
    return out

def get_previous_daily(symbol):
    daily = get_history(symbol, "D", 30)
    target_day = analysis_date_str()

    prev = []
    for c in daily:
        try:
            if candle_dt(c[0]).strftime("%Y-%m-%d") < target_day:
                prev.append(c)
        except Exception:
            pass

    prev.sort(key=lambda x: x[0])
    return prev[-1] if prev else None

def get_previous_weekly(symbol):
    weekly = get_history(symbol, "W", 60)
    target_day = analysis_date_str()

    prev = []
    for c in weekly:
        try:
            if candle_dt(c[0]).strftime("%Y-%m-%d") < target_day:
                prev.append(c)
        except Exception:
            pass

    prev.sort(key=lambda x: x[0])
    return prev[-1] if prev else None

# ================= RESULT ENGINE =================
def evaluate_sell_result(candles_after_entry, entry, target, stoploss):
    for c in candles_after_entry:
        high = float(c[2])
        low = float(c[3])

        if high >= stoploss and low <= target:
            return "Stoploss 🛑", stoploss
        if high >= stoploss:
            return "Stoploss 🛑", stoploss
        if low <= target:
            return "Target 🎯", target

    if candles_after_entry:
        return "Day End ⚪", float(candles_after_entry[-1][4])

    return "No Data", entry

def evaluate_buy_result(candles_after_entry, entry, target, stoploss):
    for c in candles_after_entry:
        high = float(c[2])
        low = float(c[3])

        if low <= stoploss and high >= target:
            return "Stoploss 🛑", stoploss
        if low <= stoploss:
            return "Stoploss 🛑", stoploss
        if high >= target:
            return "Target 🎯", target

    if candles_after_entry:
        return "Day End ⚪", float(candles_after_entry[-1][4])

    return "No Data", entry

# ================= GAP UP PLUS =================
def analyze_gapup_sell(symbol):
    prev_day = get_previous_daily(symbol)
    day_5m = get_analysis_day_candles(symbol, 5, 7)

    if prev_day is None or len(day_5m) < 1:
        return None

    first = day_5m[0]
    prev_high = float(prev_day[2])

    o = float(first[1])
    h = float(first[2])
    l = float(first[3])
    c = float(first[4])

    gap_pct = ((o - prev_high) / prev_high) * 100 if prev_high else 0.0
    candle_pct = pct_range(h, l, c)

    valid = (
        o > prev_high and
        gap_pct >= GAPUP_MIN_PCT and
        candle_pct <= GAPUP_CANDLE_MAX_PCT
    )

    if not valid:
        return None

    entry = round(l, 2)
    target = round(entry * (1 - TARGET_PCT), 2)
    stoploss = round(h * (1 + SL_BUFFER_PCT), 2)

    later = day_5m[1:]
    result, exit_price = evaluate_sell_result(later, entry, target, stoploss)
    pl = round(entry - exit_price, 2)

    return {
        "symbol": short_name(symbol),
        "gap_pct": round(gap_pct, 2),
        "entry": entry,
        "target": target,
        "stoploss": stoploss,
        "result": result,
        "exit_price": round(exit_price, 2),
        "pl": pl
    }

# ================= 15 MIN INSIDE =================
def analyze_15m_inside(symbol):
    day_15m = get_analysis_day_candles(symbol, 15, 7)
    if len(day_15m) < 2:
        return None

    c1 = day_15m[0]
    c2 = day_15m[1]

    h1 = float(c1[2])
    l1 = float(c1[3])
    c1_close = float(c1[4])

    h2 = float(c2[2])
    l2 = float(c2[3])

    if c1_close <= 0:
        return None

    range_pct = pct_range(h1, l1, c1_close)
    inside = h2 <= h1 and l2 >= l1

    if not (range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    later = day_15m[2:]

    buy_entry = round(h1, 2)
    buy_target = round(buy_entry * (1 + TARGET_PCT), 2)
    buy_sl = round(l1 * (1 - SL_BUFFER_PCT), 2)
    buy_result, buy_exit = evaluate_buy_result(later, buy_entry, buy_target, buy_sl)
    buy_pl = round(buy_exit - buy_entry, 2)

    sell_entry = round(l1, 2)
    sell_target = round(sell_entry * (1 - TARGET_PCT), 2)
    sell_sl = round(h1 * (1 + SL_BUFFER_PCT), 2)
    sell_result, sell_exit = evaluate_sell_result(later, sell_entry, sell_target, sell_sl)
    sell_pl = round(sell_entry - sell_exit, 2)

    return {
        "symbol": short_name(symbol),
        "range_pct": round(range_pct, 2),
        "buy": {
            "entry": buy_entry,
            "target": buy_target,
            "stoploss": buy_sl,
            "result": buy_result,
            "exit_price": round(buy_exit, 2),
            "pl": buy_pl
        },
        "sell": {
            "entry": sell_entry,
            "target": sell_target,
            "stoploss": sell_sl,
            "result": sell_result,
            "exit_price": round(sell_exit, 2),
            "pl": sell_pl
        }
    }

# ================= 30 MIN PIVOT SELL ONLY (WEEKLY) =================
def compute_weekly_r_levels(prev_week):
    h = float(prev_week[2])
    l = float(prev_week[3])
    c = float(prev_week[4])

    p = (h + l + c) / 3.0
    r1 = 2 * p - l
    r2 = p + (h - l)
    r3 = h + 2 * (p - l)
    step = r2 - r1
    r4 = r3 + step
    r5 = r4 + step

    return {
        "R1": round(r1, 2),
        "R2": round(r2, 2),
        "R3": round(r3, 2),
        "R4": round(r4, 2),
        "R5": round(r5, 2),
    }

def candle_touches_level(candle, level):
    high = float(candle[2])
    low = float(candle[3])
    return low <= level <= high

def analyze_30m_pivot(symbol):
    prev_week = get_previous_weekly(symbol)
    day_30m = get_analysis_day_candles(symbol, 30, 21)

    if prev_week is None or len(day_30m) < 3:
        return None

    c1 = day_30m[0]
    c2 = day_30m[1]
    c3 = day_30m[2]

    c1_open = float(c1[1]); c1_close = float(c1[4])
    c2_open = float(c2[1]); c2_close = float(c2[4])
    c2_high = float(c2[2]); c2_low = float(c2[3])
    c3_low = float(c3[3]); c3_high = float(c3[2])

    # first green, second red
    if not (c1_close > c1_open and c2_close < c2_open):
        return None

    r_levels = compute_weekly_r_levels(prev_week)

    touched_levels = []
    for name, value in r_levels.items():
        if candle_touches_level(c1, value) and candle_touches_level(c2, value):
            touched_levels.append((name, value))

    if not touched_levels:
        return None

    pivot_name, pivot_value = touched_levels[-1]

    # SELL ONLY
    entry = round(c2_low, 2)
    stoploss = round(c2_high, 2)

    if stoploss <= entry:
        return None

    target = round(entry - (stoploss - entry), 2)

    # entry only on 3rd candle, not later candles
    if c3_low > entry:
        return {
            "symbol": short_name(symbol),
            "pivot_name": pivot_name,
            "pivot_value": pivot_value,
            "entry": entry,
            "target": target,
            "stoploss": stoploss,
            "result": "No Entry",
            "exit_price": entry,
            "pl": 0.0
        }

    if c3_high >= stoploss and c3_low <= target:
        result = "Stoploss 🛑"
        exit_price = stoploss
    elif c3_high >= stoploss:
        result = "Stoploss 🛑"
        exit_price = stoploss
    elif c3_low <= target:
        result = "Target 🎯"
        exit_price = target
    else:
        result = "Day End ⚪"
        exit_price = float(c3[4])

    pl = round(entry - exit_price, 2)

    return {
        "symbol": short_name(symbol),
        "pivot_name": pivot_name,
        "pivot_value": pivot_value,
        "entry": entry,
        "target": target,
        "stoploss": stoploss,
        "result": result,
        "exit_price": round(exit_price, 2),
        "pl": pl
    }

# ================= MESSAGE FORMATTERS =================
def format_gapup_message(gap_items):
    if not gap_items:
        return "⚡ GAP UP PLUS STOCKS (%) ⚡\n\nNone"

    lines = ["⚡ GAP UP PLUS STOCKS (%) ⚡", ""]
    for i, x in enumerate(sorted(gap_items, key=lambda z: z["gap_pct"], reverse=True), 1):
        lines.append(f"{i}. {x['symbol']} ({x['gap_pct']}%)")
    return "\n".join(lines)

def format_inside_list_message(inside_items):
    if not inside_items:
        return "🕯️ 15 MIN INSIDE CANDLE STOCKS (%) 🕯️\n\nNone"

    lines = ["🕯️ 15 MIN INSIDE CANDLE STOCKS (%) 🕯️", ""]
    for i, x in enumerate(sorted(inside_items, key=lambda z: z["range_pct"]), 1):
        lines.append(f"{i}. {x['symbol']} ({x['range_pct']}%)")
    return "\n".join(lines)

def format_gapup_results(gap_items):
    if not gap_items:
        return "📘 GAP UP PLUS - IF ENTRY TAKEN\n\nNone"

    lines = ["📘 GAP UP PLUS - IF ENTRY TAKEN", ""]
    for x in gap_items:
        sign = "+" if x["pl"] > 0 else ""
        lines += [
            x["symbol"],
            f"SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
            f"Result:{x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

def format_inside_results(inside_items):
    if not inside_items:
        return "📘 15 MIN INSIDE CANDLE - IF ENTRY TAKEN\n\nNone"

    lines = ["📘 15 MIN INSIDE CANDLE - IF ENTRY TAKEN", ""]
    for x in inside_items:
        b = x["buy"]
        s = x["sell"]
        bsign = "+" if b["pl"] > 0 else ""
        ssign = "+" if s["pl"] > 0 else ""

        lines += [
            f"{x['symbol']} ({x['range_pct']}%)",
            f"🟢 BUY  Entry:{b['entry']} Target:{b['target']} SL:{b['stoploss']}",
            f"      {b['result']} Exit:{b['exit_price']} P/L:{bsign}{b['pl']}",
            f"🔴 SELL Entry:{s['entry']} Target:{s['target']} SL:{s['stoploss']}",
            f"      {s['result']} Exit:{s['exit_price']} P/L:{ssign}{s['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

def format_pivot_list_message(pivot_items):
    if not pivot_items:
        return "📍 30 MIN WEEKLY PIVOT SELL STOCKS\n\nNone"

    lines = ["📍 30 MIN WEEKLY PIVOT SELL STOCKS", ""]
    for i, x in enumerate(pivot_items, 1):
        lines.append(f"{i}. {x['symbol']} ({x['pivot_name']}={x['pivot_value']})")
    return "\n".join(lines)

def format_pivot_results(pivot_items):
    if not pivot_items:
        return "📘 30 MIN WEEKLY PIVOT SELL - IF ENTRY TAKEN\n\nNone"

    lines = ["📘 30 MIN WEEKLY PIVOT SELL - IF ENTRY TAKEN", ""]
    for x in pivot_items:
        sign = "+" if x["pl"] > 0 else ""
        lines += [
            x["symbol"],
            f"Level:{x['pivot_name']} ({x['pivot_value']})",
            f"🔴 SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
            f"      {x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

# ================= RUNNERS =================
def run_after_market_once():
    send("📡 Running after-market scan...")

    # ================= 1) GAP UP PLUS =================
    gap_items = []
    for sym in SYMBOLS:
        try:
            g = analyze_gapup_sell(sym)
            if g:
                gap_items.append(g)
        except Exception as e:
            log(f"GAP ERROR {sym}: {e}")

    send_long_message(format_gapup_message(gap_items))
    send_long_message(format_gapup_results(gap_items))

    # ================= 2) 15 MIN INSIDE =================
    inside_items = []
    for sym in SYMBOLS:
        try:
            i = analyze_15m_inside(sym)
            if i:
                inside_items.append(i)
        except Exception as e:
            log(f"15M ERROR {sym}: {e}")

    send_long_message(format_inside_list_message(inside_items))
    send_long_message(format_inside_results(inside_items))

    # ================= 3) 30 MIN WEEKLY PIVOT SELL =================
    pivot_items = []
    for sym in SYMBOLS:
        try:
            p = analyze_30m_pivot(sym)
            if p:
                pivot_items.append(p)
        except Exception as e:
            log(f"PIVOT ERROR {sym}: {e}")

    send_long_message(format_pivot_list_message(pivot_items))
    send_long_message(format_pivot_results(pivot_items))

    nxt = next_market_open_datetime()
    send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")

def run_live_day():
def run_live_day():
    gap_sent = False
    inside_sent = False
    pivot_sent = False
    eod_gap_sent = False
    eod_inside_sent = False
    eod_pivot_sent = False
    close_sent = False

    while True:
        if not is_market_open():
            return

        t = now_ist().time()

        # 1) GAP UP LIST
        if not gap_sent and t >= dtime(9, 20):
            gap_items = []
            for sym in SYMBOLS:
                try:
                    g = analyze_gapup_sell(sym)
                    if g:
                        gap_items.append(g)
                except Exception as e:
                    log(f"LIVE GAP ERROR {sym}: {e}")

            send_long_message(format_gapup_message(gap_items))
            gap_sent = True

        # 2) 15M INSIDE LIST
        if not inside_sent and t >= dtime(9, 45):
            inside_items = []
            for sym in SYMBOLS:
                try:
                    i = analyze_15m_inside(sym)
                    if i:
                        inside_items.append(i)
                except Exception as e:
                    log(f"LIVE 15M ERROR {sym}: {e}")

            send_long_message(format_inside_list_message(inside_items))
            inside_sent = True

        # 3) 30M PIVOT LIST
        if not pivot_sent and t >= dtime(10, 30):
            pivot_items = []
            for sym in SYMBOLS:
                try:
                    p = analyze_30m_pivot(sym)
                    if p:
                        pivot_items.append(p)
                except Exception as e:
                    log(f"LIVE PIVOT ERROR {sym}: {e}")

            send_long_message(format_pivot_list_message(pivot_items))
            pivot_sent = True

        # 4) EOD GAP RESULTS
        if not eod_gap_sent and t >= dtime(15, 25):
            gap_items = []
            for sym in SYMBOLS:
                try:
                    g = analyze_gapup_sell(sym)
                    if g:
                        gap_items.append(g)
                except Exception as e:
                    log(f"EOD GAP ERROR {sym}: {e}")

            send_long_message(format_gapup_results(gap_items))
            eod_gap_sent = True

        # 5) EOD 15M RESULTS
        if not eod_inside_sent and t >= dtime(15, 26):
            inside_items = []
            for sym in SYMBOLS:
                try:
                    i = analyze_15m_inside(sym)
                    if i:
                        inside_items.append(i)
                except Exception as e:
                    log(f"EOD 15M ERROR {sym}: {e}")

            send_long_message(format_inside_results(inside_items))
            eod_inside_sent = True

        # 6) EOD PIVOT RESULTS
        if not eod_pivot_sent and t >= dtime(15, 27):
            pivot_items = []
            for sym in SYMBOLS:
                try:
                    p = analyze_30m_pivot(sym)
                    if p:
                        pivot_items.append(p)
                except Exception as e:
                    log(f"EOD PIVOT ERROR {sym}: {e}")

            send_long_message(format_pivot_results(pivot_items))
            eod_pivot_sent = True

        # 7) MARKET CLOSED MESSAGE
        if not close_sent and t >= dtime(15, 28):
            nxt = next_market_open_datetime()
            send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            close_sent = True

        time.sleep(POLL_SECONDS)
# ================= MAIN =================
def main():
    profile = check_auth()
    send(
        f"🚀 BOT STARTED\n"
        f"Profile status: {profile.get('s')}\n"
        f"AFTER_MARKET_RUN={AFTER_MARKET_RUN}\n"
        f"Analysis day={analysis_date_str()}\n"
        f"WATCHLIST={WATCHLIST_RAW}"
    )

    while True:
        if is_market_open():
            run_live_day()
        else:
            if AFTER_MARKET_RUN:
                run_after_market_once()
            sleep_until_next_market_open()

if __name__ == "__main__":
    main()

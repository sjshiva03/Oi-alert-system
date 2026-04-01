import os
import time
from datetime import datetime, timedelta, timezone
import requests
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
WATCHLIST = (os.getenv("WATCHLIST") or "").strip()

AFTER_MARKET_RUN = (os.getenv("AFTER_MARKET_RUN", "false").strip().lower() == "true")

# Strategy variables from Railway
GAPUP_MIN_PCT = float(os.getenv("GAPUP_MIN_PCT", "0.0"))                 # open > prev high is main rule; extra gap % filter optional
GAPUP_CANDLE_MAX_PCT = float(os.getenv("GAPUP_CANDLE_MAX_PCT", "1.5"))   # 1st 5m candle max range %
INSIDE15_FIRST_CANDLE_MAX_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MAX_PCT", "2.0"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "1.0")) / 100.0               # 1.0 -> 1%
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.1")) / 100.0          # 0.1 -> 0.1%

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
if not WATCHLIST:
    raise Exception("Missing WATCHLIST")

# ================= FYERS =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

# ================= HELPERS =================
def send(msg: str):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=20,
        )
    except Exception:
        print(msg)

def now():
    return datetime.now(IST)

def today_str():
    return now().strftime("%Y-%m-%d")

def log(msg: str):
    print(f"[{now().strftime('%H:%M:%S')}] {msg}", flush=True)

def is_market_open():
    t = now().time()
    return t >= datetime.strptime("09:15", "%H:%M").time() and t <= datetime.strptime("15:30", "%H:%M").time()

def convert(sym: str):
    s = sym.strip().upper()
    if ":" in s:
        return s
    if s in ["NIFTY", "NIFTY50"]:
        return "NSE:NIFTY50-INDEX"
    if s == "BANKNIFTY":
        return "NSE:NIFTYBANK-INDEX"
    return f"NSE:{s}-EQ"

def short_name(symbol: str):
    return symbol.split(":")[1].replace("-EQ", "").replace("-INDEX", "")

def candle_time(ts: int):
    return datetime.fromtimestamp(ts, IST)

def pct_range(high: float, low: float, close: float):
    if close == 0:
        return 0.0
    return ((high - low) / close) * 100.0

SYMBOLS = [convert(s) for s in WATCHLIST.split(",") if s.strip()]

# ================= DATA =================
def get_history(symbol, res, days=5):
    payload = {
        "symbol": symbol,
        "resolution": str(res),
        "date_format": "1",
        "range_from": (now() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": now().strftime("%Y-%m-%d"),
        "cont_flag": "1",
    }
    try:
        data = fyers.history(data=payload)
    except TypeError:
        data = fyers.history(payload)
    return data.get("candles", [])

def get_today_candles(symbol, res, days=5):
    candles = get_history(symbol, res, days)
    out = []
    td = today_str()
    for c in candles:
        try:
            if candle_time(c[0]).strftime("%Y-%m-%d") == td:
                out.append(c)
        except Exception:
            pass
    out.sort(key=lambda x: x[0])
    return out

def get_previous_daily(symbol):
    daily = get_history(symbol, "D", 10)
    parsed = []
    td = today_str()
    for c in daily:
        try:
            dt = candle_time(c[0]).strftime("%Y-%m-%d")
            if dt < td:
                parsed.append(c)
        except Exception:
            pass
    parsed.sort(key=lambda x: x[0])
    return parsed[-1] if parsed else None

# ================= RESULT ENGINE =================
def evaluate_sell_result(candles, entry, target, stoploss):
    """
    candles: list of intraday candles AFTER entry candle
    Sell logic:
      target hit if Low <= target
      stoploss hit if High >= stoploss
      if both in same candle -> conservative: Stoploss first
    """
    for c in candles:
        high = float(c[2])
        low = float(c[3])

        # conservative assumption if both touched same candle
        if high >= stoploss and low <= target:
            return "Stoploss 🛑", stoploss

        if high >= stoploss:
            return "Stoploss 🛑", stoploss

        if low <= target:
            return "Target 🎯", target

    if candles:
        last_close = float(candles[-1][4])
        return "Day End", last_close

    return "No Data", entry

def evaluate_buy_result(candles, entry, target, stoploss):
    """
    Buy logic:
      target hit if High >= target
      stoploss hit if Low <= stoploss
      if both in same candle -> conservative: Stoploss first
    """
    for c in candles:
        high = float(c[2])
        low = float(c[3])

        if low <= stoploss and high >= target:
            return "Stoploss 🛑", stoploss

        if low <= stoploss:
            return "Stoploss 🛑", stoploss

        if high >= target:
            return "Target 🎯", target

    if candles:
        last_close = float(candles[-1][4])
        return "Day End", last_close

    return "No Data", entry

# ================= STRATEGY ANALYSIS =================
def analyze_gapup_sell_after_market(symbol):
    """
    Rule:
    - today's first 5m candle
    - open > previous day high
    - optional gap % filter
    - first 5m candle range < GAPUP_CANDLE_MAX_PCT
    - entry = first 5m low breakdown
    - target = entry - TARGET_PCT
    - sl = first 5m high + buffer
    - evaluate outcome from later 5m candles
    """
    prev_day = get_previous_daily(symbol)
    today_5m = get_today_candles(symbol, 5, 5)

    if prev_day is None or len(today_5m) < 1:
        return None

    first = today_5m[0]

    prev_high = float(prev_day[2])
    open_ = float(first[1])
    high = float(first[2])
    low = float(first[3])
    close = float(first[4])

    gap_vs_prev_high_pct = ((open_ - prev_high) / prev_high) * 100 if prev_high else 0.0
    first_range_pct = pct_range(high, low, close)

    valid = (
        open_ > prev_high
        and gap_vs_prev_high_pct >= GAPUP_MIN_PCT
        and first_range_pct <= GAPUP_CANDLE_MAX_PCT
    )

    if not valid:
        return None

    entry = low
    target = round(entry * (1 - TARGET_PCT), 2)
    stoploss = round(high * (1 + SL_BUFFER_PCT), 2)

    later_candles = today_5m[1:]
    result, exit_price = evaluate_sell_result(later_candles, entry, target, stoploss)

    pl = round(entry - exit_price, 2)

    return {
        "symbol": short_name(symbol),
        "strategy": "Gap-Up Breakdown",
        "type": "SELL",
        "gap_pct": round(gap_vs_prev_high_pct, 2),
        "entry": round(entry, 2),
        "target": target,
        "stoploss": stoploss,
        "result": result,
        "exit_price": round(exit_price, 2),
        "pl": pl,
    }

def analyze_15m_inside_after_market(symbol):
    """
    Rule:
    - first 15m candle range < INSIDE15_FIRST_CANDLE_MAX_PCT
    - second candle inside first
    - buy above first high
    - sell below first low
    - evaluate from later 15m candles
    """
    today_15m = get_today_candles(symbol, 15, 5)
    if len(today_15m) < 2:
        return None

    c1 = today_15m[0]
    c2 = today_15m[1]

    h1, l1, c1close = float(c1[2]), float(c1[3]), float(c1[4])
    h2, l2 = float(c2[2]), float(c2[3])

    range_pct = pct_range(h1, l1, c1close)
    inside = h2 < h1 and l2 > l1

    if not (range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    later = today_15m[2:]

    # BUY side
    buy_entry = h1
    buy_target = round(buy_entry * (1 + TARGET_PCT), 2)
    buy_sl = round(l1 * (1 - SL_BUFFER_PCT), 2)
    buy_result, buy_exit = evaluate_buy_result(later, buy_entry, buy_target, buy_sl)
    buy_pl = round(buy_exit - buy_entry, 2)

    # SELL side
    sell_entry = l1
    sell_target = round(sell_entry * (1 - TARGET_PCT), 2)
    sell_sl = round(h1 * (1 + SL_BUFFER_PCT), 2)
    sell_result, sell_exit = evaluate_sell_result(later, sell_entry, sell_target, sell_sl)
    sell_pl = round(sell_entry - sell_exit, 2)

    return {
        "symbol": short_name(symbol),
        "strategy_buy": "15 Min Inside Candle Breakout",
        "strategy_sell": "15 Min Inside Candle Breakdown",
        "buy": {
            "type": "BUY",
            "entry": round(buy_entry, 2),
            "target": buy_target,
            "stoploss": buy_sl,
            "result": buy_result,
            "exit_price": round(buy_exit, 2),
            "pl": buy_pl,
        },
        "sell": {
            "type": "SELL",
            "entry": round(sell_entry, 2),
            "target": sell_target,
            "stoploss": sell_sl,
            "result": sell_result,
            "exit_price": round(sell_exit, 2),
            "pl": sell_pl,
        }
    }

def analyze_pivot_rejection_after_market(symbol):
    """
    Simple historical pivot rejection summary:
    - use today's first two 30m candles
    - check rejection at P/R1/R2/R3
    - if sell entry triggered, evaluate result using later 30m candles
    """
    today_30m = get_today_candles(symbol, 30, 5)
    if len(today_30m) < 2:
        return None

    prev_day = get_previous_daily(symbol)
    if prev_day is None:
        return None

    ph = float(prev_day[2])
    pl = float(prev_day[3])
    pc = float(prev_day[4])

    pivot = (ph + pl + pc) / 3
    levels = {
        "P": pivot,
        "R1": (2 * pivot) - pl,
        "R2": pivot + (ph - pl),
        "R3": ph + 2 * (pivot - pl),
        "S1": (2 * pivot) - ph,
        "S2": pivot - (ph - pl),
        "S3": pl - 2 * (ph - pivot),
    }

    c1 = today_30m[0]
    c2 = today_30m[1]
    later = today_30m[2:]

    for level_name, level in levels.items():
        c1_green = float(c1[4]) > float(c1[1])
        c2_red = float(c2[4]) < float(c2[1])
        c1_touch = float(c1[3]) <= level <= float(c1[2])
        c2_touch = float(c2[3]) <= level <= float(c2[2])
        c2_below = float(c2[4]) < level

        if c1_green and c1_touch and c2_red and c2_touch and c2_below:
            entry = float(c2[3])
            target = round(entry * (1 - TARGET_PCT), 2)
            stoploss = round(max(float(c1[2]), float(c2[2])) * (1 + SL_BUFFER_PCT), 2)
            result, exit_price = evaluate_sell_result(later, entry, target, stoploss)
            pl_value = round(entry - exit_price, 2)

            return {
                "symbol": short_name(symbol),
                "strategy": f"Pivot Rejection ({level_name})",
                "type": "SELL",
                "entry": round(entry, 2),
                "target": target,
                "stoploss": stoploss,
                "result": result,
                "exit_price": round(exit_price, 2),
                "pl": pl_value,
            }

    return None

# ================= FORMATTERS =================
def format_gap_summary(items):
    if not items:
        return "⚡Gap up plus⚡\n\nNone"

    lines = ["⚡Gap up plus⚡", ""]
    for i, x in enumerate(items, 1):
        lines.append(f"{i}. {x['symbol']} ({x['gap_pct']}%)")
    return "\n".join(lines)

def format_inside_summary(items):
    if not items:
        return "🕯️15 Min Inside Candle🕯️\n\nNone"

    lines = ["🕯️15 Min Inside Candle🕯️", ""]
    for i, x in enumerate(items, 1):
        lines.append(f"{i}. {x['symbol']}")
    return "\n".join(lines)

def format_pivot_summary(items):
    if not items:
        return "⛔PIVOT Alert⛔\n\nNone"

    lines = ["⛔PIVOT Alert⛔", ""]
    for x in items:
        lines.append(x["symbol"])
    return "\n".join(lines)

def format_after_market_results(gap_results, inside_results, pivot_results):
    lines = ["📘 AFTER MARKET RESULTS", ""]

    if gap_results:
        lines.append("⚡ GAP UP SELL ⚡")
        for x in gap_results:
            sign = "+" if x["pl"] > 0 else ""
            lines += [
                x["symbol"],
                f"Entry    : {x['entry']}",
                f"Target   : {x['target']}",
                f"Stoploss : {x['stoploss']}",
                f"Result   : {x['result']}",
                f"Exit/LTP : {x['exit_price']}",
                f"P/L      : {sign}{x['pl']}",
                "",
            ]

    if inside_results:
        lines.append("🕯️ 15M INSIDE CANDLE")
        for x in inside_results:
            b = x["buy"]
            s = x["sell"]

            bsign = "+" if b["pl"] > 0 else ""
            ssign = "+" if s["pl"] > 0 else ""

            lines += [
                x["symbol"],
                f"BUY  -> Entry:{b['entry']} Target:{b['target']} SL:{b['stoploss']} Result:{b['result']} Exit:{b['exit_price']} P/L:{bsign}{b['pl']}",
                f"SELL -> Entry:{s['entry']} Target:{s['target']} SL:{s['stoploss']} Result:{s['result']} Exit:{s['exit_price']} P/L:{ssign}{s['pl']}",
                "",
            ]

    if pivot_results:
        lines.append("⛔ PIVOT REJECTION")
        for x in pivot_results:
            sign = "+" if x["pl"] > 0 else ""
            lines += [
                x["symbol"],
                f"Strategy : {x['strategy']}",
                f"Entry    : {x['entry']}",
                f"Target   : {x['target']}",
                f"Stoploss : {x['stoploss']}",
                f"Result   : {x['result']}",
                f"Exit/LTP : {x['exit_price']}",
                f"P/L      : {sign}{x['pl']}",
                "",
            ]

    if len(lines) == 2:
        lines.append("No valid historical setups found.")

    return "\n".join(lines).strip()

# ================= LIVE MODE =================
def run_live():
    send(
        "🚀 BOT STARTED\n"
        f"Watching: {', '.join([short_name(s) for s in SYMBOLS])}\n"
        f"Gap Candle % Max: {GAPUP_CANDLE_MAX_PCT}\n"
        f"15m Candle % Max: {INSIDE15_FIRST_CANDLE_MAX_PCT}\n"
        f"Target %: {TARGET_PCT * 100:.2f}\n"
        f"SL Buffer %: {SL_BUFFER_PCT * 100:.2f}"
    )

    gap_sent = False
    inside_sent = False

    while True:
        if not is_market_open():
            return

        t = now().time()

        if not gap_sent and t >= datetime.strptime("09:20", "%H:%M").time():
            gap_results = []
            for sym in SYMBOLS:
                r = analyze_gapup_sell_after_market(sym)
                if r:
                    gap_results.append(r)
            send(format_gap_summary(gap_results))
            gap_sent = True

        if not inside_sent and t >= datetime.strptime("09:45", "%H:%M").time():
            inside_results = []
            for sym in SYMBOLS:
                r = analyze_15m_inside_after_market(sym)
                if r:
                    inside_results.append(r)
            send(format_inside_summary(inside_results))
            inside_sent = True

        time.sleep(30)

# ================= AFTER MARKET MODE =================
def run_after_market():
    log("Running AFTER MARKET historical mode")

    gap_results = []
    inside_results = []
    pivot_results = []

    for sym in SYMBOLS:
        g = analyze_gapup_sell_after_market(sym)
        if g:
            gap_results.append(g)

        i = analyze_15m_inside_after_market(sym)
        if i:
            inside_results.append(i)

        p = analyze_pivot_rejection_after_market(sym)
        if p:
            pivot_results.append(p)

    send(format_gap_summary(gap_results))
    send(format_inside_summary(inside_results))
    send(format_pivot_summary(pivot_results))
    send(format_after_market_results(gap_results, inside_results, pivot_results))

# ================= MAIN =================
if __name__ == "__main__":
    if not is_market_open():
        if AFTER_MARKET_RUN:
            run_after_market()
        else:
            log("Market closed. AFTER_MARKET_RUN disabled.")
    else:
        run_live()

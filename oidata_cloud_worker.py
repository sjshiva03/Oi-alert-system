import os
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
WATCHLIST_RAW = (os.getenv("WATCHLIST") or "").strip()

AFTER_MARKET_RUN = (os.getenv("AFTER_MARKET_RUN", "false").strip().lower() == "true")

# Holiday list optional
NSE_HOLIDAYS_RAW = (os.getenv("NSE_HOLIDAYS") or "").strip()

# Variables from Railway
GAPUP_MIN_PCT = float(os.getenv("GAPUP_MIN_PCT", "0.0"))                       # extra filter on open vs prev high %
GAPUP_CANDLE_MAX_PCT = float(os.getenv("GAPUP_CANDLE_MAX_PCT", "1.5"))         # first 5m candle max range %
INSIDE15_FIRST_CANDLE_MAX_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MAX_PCT", "2.0"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "1.0")) / 100.0                     # 1.0 => 1%
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.1")) / 100.0               # 0.1 => 0.1%

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
if not WATCHLIST_RAW:
    raise Exception("Missing WATCHLIST")

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

def today_str():
    return now_ist().strftime("%Y-%m-%d")

def log(msg: str):
    print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)

def short_name(symbol: str) -> str:
    return symbol.split(":")[1].replace("-EQ", "").replace("-INDEX", "")

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

def candle_dt(ts: int):
    return datetime.fromtimestamp(ts, IST)

def pct_range(high: float, low: float, close: float) -> float:
    if close == 0:
        return 0.0
    return ((high - low) / close) * 100.0

def fmt2(x: float) -> str:
    return f"{x:.2f}"

# ================= MARKET SCHEDULER =================
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

    candidate = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    while not is_market_day(candidate):
        candidate = (candidate + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    return candidate

# ================= FYERS =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

def get_history(symbol, resolution, days=10):
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
        log(f"HISTORY ERROR {symbol} {resolution}: {e}")
        return []

    candles = data.get("candles", [])
    if not candles:
        log(f"HISTORY EMPTY {symbol} {resolution}")
    return candles

# ================= CANDLE SELECTION =================
def get_today_candles(symbol, resolution, days=10):
    candles = get_history(symbol, resolution, days)
    td = today_str()
    out = []

    for c in candles:
        try:
            if candle_dt(c[0]).strftime("%Y-%m-%d") == td:
                out.append(c)
        except Exception:
            pass

    out.sort(key=lambda x: x[0])
    return out

def get_previous_daily(symbol):
    daily = get_history(symbol, "D", 15)
    td = today_str()
    out = []
    for c in daily:
        try:
            ds = candle_dt(c[0]).strftime("%Y-%m-%d")
            if ds < td:
                out.append(c)
        except Exception:
            pass
    out.sort(key=lambda x: x[0])
    return out[-1] if out else None

def get_first_5m_candle_today(symbol):
    c = get_today_candles(symbol, 5, 5)
    return c[0] if len(c) >= 1 else None

def get_first_two_15m_candles_today(symbol):
    c = get_today_candles(symbol, 15, 5)
    if len(c) >= 2:
        return c[0], c[1]
    return None, None

# ================= RESULT ENGINE =================
def evaluate_sell_result(candles_after_entry, entry, target, stoploss):
    """
    Conservative rule:
    If both target and stoploss touched in same candle -> Stoploss first
    """
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
        return "Day End", float(candles_after_entry[-1][4])

    return "No Data", entry

def evaluate_buy_result(candles_after_entry, entry, target, stoploss):
    """
    Conservative rule:
    If both target and stoploss touched in same candle -> Stoploss first
    """
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
        return "Day End", float(candles_after_entry[-1][4])

    return "No Data", entry

# ================= GAP-UP ANALYSIS =================
def analyze_gapup_sell(symbol):
    """
    Rule:
    - Open > Previous Day High
    - gap % >= GAPUP_MIN_PCT
    - first 5m range % <= GAPUP_CANDLE_MAX_PCT
    - sell entry = first 5m low
    - target = entry - TARGET_PCT
    - sl = first 5m high + buffer
    """
    prev_day = get_previous_daily(symbol)
    first_5m = get_first_5m_candle_today(symbol)

    if prev_day is None or first_5m is None:
        return None

    prev_high = float(prev_day[2])
    o = float(first_5m[1])
    h = float(first_5m[2])
    l = float(first_5m[3])
    c = float(first_5m[4])

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

    later_5m = get_today_candles(symbol, 5, 5)[1:]
    result, exit_price = evaluate_sell_result(later_5m, entry, target, stoploss)
    pl = round(entry - exit_price, 2)

    return {
        "symbol": short_name(symbol),
        "strategy": "Gap-Up Breakdown",
        "type": "SELL",
        "gap_pct": round(gap_pct, 2),
        "candle_pct": round(candle_pct, 2),
        "entry": entry,
        "target": target,
        "stoploss": stoploss,
        "result": result,
        "exit_price": round(exit_price, 2),
        "pl": pl,
    }

# ================= 15M INSIDE ANALYSIS =================
def analyze_15m_inside(symbol):
    """
    Rule:
    - first 15m candle range % <= INSIDE15_FIRST_CANDLE_MAX_PCT
    - second 15m candle inside first
    - buy above first high
    - sell below first low
    - evaluate later 15m candles
    """
    c1, c2 = get_first_two_15m_candles_today(symbol)
    if c1 is None or c2 is None:
        return None

    h1 = float(c1[2])
    l1 = float(c1[3])
    c1_close = float(c1[4])

    h2 = float(c2[2])
    l2 = float(c2[3])

    if c1_close <= 0:
        return None

    first_range_pct = pct_range(h1, l1, c1_close)
    inside = h2 < h1 and l2 > l1

    log(f"15M {short_name(symbol)} | H1:{h1} L1:{l1} H2:{h2} L2:{l2} Range%:{first_range_pct:.2f} Inside:{inside}")

    if not (first_range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    later_15m = get_today_candles(symbol, 15, 5)[2:]

    buy_entry = round(h1, 2)
    buy_target = round(buy_entry * (1 + TARGET_PCT), 2)
    buy_sl = round(l1 * (1 - SL_BUFFER_PCT), 2)
    buy_result, buy_exit = evaluate_buy_result(later_15m, buy_entry, buy_target, buy_sl)
    buy_pl = round(buy_exit - buy_entry, 2)

    sell_entry = round(l1, 2)
    sell_target = round(sell_entry * (1 - TARGET_PCT), 2)
    sell_sl = round(h1 * (1 + SL_BUFFER_PCT), 2)
    sell_result, sell_exit = evaluate_sell_result(later_15m, sell_entry, sell_target, sell_sl)
    sell_pl = round(sell_entry - sell_exit, 2)

    return {
        "symbol": short_name(symbol),
        "range_pct": round(first_range_pct, 2),
        "first": {
            "high": round(h1, 2),
            "low": round(l1, 2),
        },
        "second": {
            "high": round(h2, 2),
            "low": round(l2, 2),
        },
        "buy": {
            "strategy": "15 Min Inside Candle Breakout",
            "type": "BUY",
            "entry": buy_entry,
            "target": buy_target,
            "stoploss": buy_sl,
            "result": buy_result,
            "exit_price": round(buy_exit, 2),
            "pl": buy_pl,
        },
        "sell": {
            "strategy": "15 Min Inside Candle Breakdown",
            "type": "SELL",
            "entry": sell_entry,
            "target": sell_target,
            "stoploss": sell_sl,
            "result": sell_result,
            "exit_price": round(sell_exit, 2),
            "pl": sell_pl,
        }
    }

# ================= FORMATTERS =================
def format_gap_summary(items):
    if not items:
        return "⚡Gap up plus⚡\n\nNone"

    lines = ["⚡Gap up plus⚡", ""]
    for i, x in enumerate(items, 1):
        lines.append(f"{i}. {x['symbol']} ({x['gap_pct']}%)")
    return "\n".join(lines)

def format_15m_summary(items):
    if not items:
        return "🕯️15 Min Inside Candle🕯️\n\nNone"

    lines = ["🕯️15 Min Inside Candle🕯️", ""]
    for i, x in enumerate(items, 1):
        lines += [
            f"{i}. {x['symbol']}",
            f"1st Candle  H:{x['first']['high']} L:{x['first']['low']} Range%:{x['range_pct']}",
            f"2nd Candle  H:{x['second']['high']} L:{x['second']['low']}",
            f"BUY Entry:{x['buy']['entry']} Target:{x['buy']['target']} SL:{x['buy']['stoploss']}",
            f"SELL Entry:{x['sell']['entry']} Target:{x['sell']['target']} SL:{x['sell']['stoploss']}",
            "",
        ]
    return "\n".join(lines).strip()

def format_after_market_results(gap_results, inside_results):
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
                f"1st Candle  H:{x['first']['high']} L:{x['first']['low']} Range%:{x['range_pct']}",
                f"2nd Candle  H:{x['second']['high']} L:{x['second']['low']}",
                f"BUY  -> Entry:{b['entry']} Target:{b['target']} SL:{b['stoploss']} Result:{b['result']} Exit:{b['exit_price']} P/L:{bsign}{b['pl']}",
                f"SELL -> Entry:{s['entry']} Target:{s['target']} SL:{s['stoploss']} Result:{s['result']} Exit:{s['exit_price']} P/L:{ssign}{s['pl']}",
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

        t = now_ist().time()

        if not gap_sent and t >= dtime(9, 20):
            gap_results = []
            for sym in SYMBOLS:
                r = analyze_gapup_sell(sym)
                if r:
                    gap_results.append(r)
            send(format_gap_summary(gap_results))
            gap_sent = True

        if not inside_sent and t >= dtime(9, 45):
            inside_results = []
            for sym in SYMBOLS:
                r = analyze_15m_inside(sym)
                if r:
                    inside_results.append(r)
            send(format_15m_summary(inside_results))
            inside_sent = True

        time.sleep(30)

# ================= AFTER MARKET MODE =================
def run_after_market():
    log("Running AFTER MARKET historical mode")

    gap_results = []
    inside_results = []

    for sym in SYMBOLS:
        g = analyze_gapup_sell(sym)
        if g:
            gap_results.append(g)

        i = analyze_15m_inside(sym)
        if i:
            inside_results.append(i)

    send(format_gap_summary(gap_results))
    send(format_15m_summary(inside_results))
    send(format_after_market_results(gap_results, inside_results))

# ================= MAIN =================
if __name__ == "__main__":
    if is_market_open():
        run_live()
    else:
        if AFTER_MARKET_RUN:
            run_after_market()
        else:
            nxt = next_market_open_datetime()
            log(f"Market closed. AFTER_MARKET_RUN disabled. Next open: {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")

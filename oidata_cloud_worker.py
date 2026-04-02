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

# ================= NIFTY 50 =================
SYMBOLS = [
    "NSE:ADANIENT-EQ", "NSE:ADANIPORTS-EQ", "NSE:APOLLOHOSP-EQ", "NSE:ASIANPAINT-EQ",
    "NSE:AXISBANK-EQ", "NSE:BAJAJ-AUTO-EQ", "NSE:BAJFINANCE-EQ", "NSE:BAJAJFINSV-EQ",
    "NSE:BEL-EQ", "NSE:BHARTIARTL-EQ", "NSE:BPCL-EQ", "NSE:BRITANNIA-EQ",
    "NSE:CIPLA-EQ", "NSE:COALINDIA-EQ", "NSE:DRREDDY-EQ", "NSE:EICHERMOT-EQ",
    "NSE:ETERNAL-EQ", "NSE:GRASIM-EQ", "NSE:HCLTECH-EQ", "NSE:HDFCBANK-EQ",
    "NSE:HDFCLIFE-EQ", "NSE:HEROMOTOCO-EQ", "NSE:HINDALCO-EQ", "NSE:HINDUNILVR-EQ",
    "NSE:ICICIBANK-EQ", "NSE:INDIGO-EQ", "NSE:INFY-EQ", "NSE:ITC-EQ",
    "NSE:JSWSTEEL-EQ", "NSE:KOTAKBANK-EQ", "NSE:LT-EQ", "NSE:M&M-EQ",
    "NSE:MARUTI-EQ", "NSE:NESTLEIND-EQ", "NSE:NTPC-EQ", "NSE:ONGC-EQ",
    "NSE:POWERGRID-EQ", "NSE:RELIANCE-EQ", "NSE:SBILIFE-EQ", "NSE:SHRIRAMFIN-EQ",
    "NSE:SBIN-EQ", "NSE:SUNPHARMA-EQ", "NSE:TATACONSUM-EQ", "NSE:TATAMOTORS-EQ",
    "NSE:TATASTEEL-EQ", "NSE:TCS-EQ", "NSE:TECHM-EQ", "NSE:TITAN-EQ",
    "NSE:TRENT-EQ", "NSE:WIPRO-EQ"
]

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

def short_name(symbol: str) -> str:
    return symbol.split(":")[1].replace("-EQ", "").replace("-INDEX", "")

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

# ================= HOLIDAYS / MARKET TIME =================
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

def get_history(symbol, resolution, days=10):
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
    candles = data.get("candles", [])
    return dedupe_candles_by_ts(candles)

# ================= CANDLE HELPERS =================
def get_analysis_day_candles(symbol, resolution, days=10):
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
    daily = get_history(symbol, "D", 20)
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
        return "Day End", float(candles_after_entry[-1][4])

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
        return "Day End", float(candles_after_entry[-1][4])

    return "No Data", entry

# ================= STRATEGIES =================
def analyze_gapup_sell(symbol):
    prev_day = get_previous_daily(symbol)
    day_5m = get_analysis_day_candles(symbol, 5, 5)

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

def analyze_15m_inside(symbol):
    day_15m = get_analysis_day_candles(symbol, 15, 5)
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

    log(
        f"15M {short_name(symbol)} | "
        f"C1 H:{h1} L:{l1} C:{c1_close} | "
        f"C2 H:{h2} L:{l2} | "
        f"Range%:{range_pct:.2f} | Inside:{inside}"
    )

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
        "first_high": round(h1, 2),
        "first_low": round(l1, 2),
        "second_high": round(h2, 2),
        "second_low": round(l2, 2),
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

# ================= FORMATTERS =================
def format_gapup_message(gap_items):
    if not gap_items:
        return "⚡ GAP UP PLUS STOCKS (%) ⚡\n\nNone"

    lines = ["⚡ GAP UP PLUS STOCKS (%) ⚡", ""]
    for i, x in enumerate(sorted(gap_items, key=lambda z: z["gap_pct"], reverse=True), 1):
        lines.append(f"{i}. {x['symbol']} ({x['gap_pct']}%)")
    return "\n".join(lines)

def format_inside_list_message(inside_items):
    if not inside_items:
        return "🕯️ 15 MIN INSIDE CANDLE STOCKS 🕯️\n\nNone"

    lines = ["🕯️ 15 MIN INSIDE CANDLE STOCKS 🕯️", ""]
    for i, x in enumerate(inside_items, 1):
        lines += [
            f"{i}. {x['symbol']}",
            f"1st Candle  H:{x['first_high']} L:{x['first_low']} Range%:{x['range_pct']}",
            f"2nd Candle  H:{x['second_high']} L:{x['second_low']}",
            ""
        ]
    return "\n".join(lines).strip()

def format_entry_outcome_message(gap_items, inside_items):
    lines = ["📘 IF ENTRY TAKEN, WHAT HAPPENED", ""]

    if gap_items:
        lines.append("⚡ GAP UP PLUS")
        for x in gap_items:
            sign = "+" if x["pl"] > 0 else ""
            lines += [
                x["symbol"],
                f"SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
                f"Result:{x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
                ""
            ]

    if inside_items:
        lines.append("🕯️ 15 MIN INSIDE CANDLE")
        for x in inside_items:
            b = x["buy"]
            s = x["sell"]
            bsign = "+" if b["pl"] > 0 else ""
            ssign = "+" if s["pl"] > 0 else ""

            lines += [
                x["symbol"],
                f"BUY  Entry:{b['entry']} Target:{b['target']} SL:{b['stoploss']}",
                f"     Result:{b['result']} Exit:{b['exit_price']} P/L:{bsign}{b['pl']}",
                f"SELL Entry:{s['entry']} Target:{s['target']} SL:{s['stoploss']}",
                f"     Result:{s['result']} Exit:{s['exit_price']} P/L:{ssign}{s['pl']}",
                ""
            ]

    if len(lines) == 2:
        lines.append("No valid setups found.")

    return "\n".join(lines).strip()

# ================= RUNNERS =================
def run_after_market_once():
    send("📡 Running after-market scan...")

    gap_items = []
    inside_items = []

    for sym in SYMBOLS:
        try:
            g = analyze_gapup_sell(sym)
            if g:
                gap_items.append(g)
        except Exception as e:
            log(f"GAP ERROR {sym}: {e}")

        try:
            i = analyze_15m_inside(sym)
            if i:
                inside_items.append(i)
        except Exception as e:
            log(f"15M ERROR {sym}: {e}")

    # Message 1
    send(format_gapup_message(gap_items))

    # Message 2
    send(format_inside_list_message(inside_items))

    # Message 3
    send(format_entry_outcome_message(gap_items, inside_items))

    nxt = next_market_open_datetime()
    send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")

def run_live_day():
    gap_sent = False
    inside_sent = False
    eod_sent = False

    while True:
        if not is_market_open():
            return

        t = now_ist().time()

        if not gap_sent and t >= dtime(9, 20):
            gap_items = []
            for sym in SYMBOLS:
                try:
                    g = analyze_gapup_sell(sym)
                    if g:
                        gap_items.append(g)
                except Exception as e:
                    log(f"LIVE GAP ERROR {sym}: {e}")
            send(format_gapup_message(gap_items))
            gap_sent = True

        if not inside_sent and t >= dtime(9, 45):
            inside_items = []
            for sym in SYMBOLS:
                try:
                    i = analyze_15m_inside(sym)
                    if i:
                        inside_items.append(i)
                except Exception as e:
                    log(f"LIVE 15M ERROR {sym}: {e}")
            send(format_inside_list_message(inside_items))
            inside_sent = True

        if not eod_sent and t >= dtime(15, 25):
            gap_items = []
            inside_items = []

            for sym in SYMBOLS:
                try:
                    g = analyze_gapup_sell(sym)
                    if g:
                        gap_items.append(g)
                except Exception:
                    pass

                try:
                    i = analyze_15m_inside(sym)
                    if i:
                        inside_items.append(i)
                except Exception:
                    pass

            send(format_entry_outcome_message(gap_items, inside_items))
            nxt = next_market_open_datetime()
            send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            eod_sent = True

        time.sleep(POLL_SECONDS)

# ================= MAIN =================
def main():
    profile = check_auth()
    send(
        f"🚀 BOT STARTED\n"
        f"Profile status: {profile.get('s')}\n"
        f"AFTER_MARKET_RUN={AFTER_MARKET_RUN}\n"
        f"Analysis day={analysis_date_str()}"
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

import os
import time
import math
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from collections import deque
from fyers_apiv3 import fyersModel

# ================= ENV =================
CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
WATCHLIST_RAW = (os.getenv("WATCHLIST") or "").strip()
NSE_HOLIDAYS_RAW = (os.getenv("NSE_HOLIDAYS") or "").strip()

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
if not WATCHLIST_RAW:
    raise Exception("Missing WATCHLIST")

# ================= SETTINGS =================
IST = timezone(timedelta(hours=5, minutes=30))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
OI_UPDATE_EVERY_SECONDS = int(os.getenv("OI_UPDATE_EVERY_SECONDS", "300"))
TARGET_PCT = float(os.getenv("TARGET_PCT", "0.01"))
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.001"))
DEBUG_OI = (os.getenv("DEBUG_OI", "false").strip().lower() == "true")

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

# ================= UTIL =================
def now_ist():
    return datetime.now(IST)

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def log(msg: str):
    print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)

def name(sym: str) -> str:
    right = sym.split(":")[1]
    return right.replace("-EQ", "").replace("-INDEX", "")

def human_num(v: float) -> str:
    sign = "+" if v > 0 else ""
    av = abs(v)
    if av >= 10000000:
        return f"{sign}{v/10000000:.2f}Cr".replace(".00", "")
    if av >= 100000:
        return f"{sign}{v/100000:.2f}L".replace(".00", "")
    if av >= 1000:
        return f"{sign}{v/1000:.2f}K".replace(".00", "")
    return f"{sign}{v:.0f}"

def arrow(v: float) -> str:
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ================= HOLIDAYS / SCHEDULER =================
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

_last_sleep_log = None

def sleep_until_next_market_open():
    global _last_sleep_log
    nxt = next_market_open_datetime()
    key = nxt.strftime("%Y-%m-%d %H:%M:%S")
    if _last_sleep_log != key:
        log(f"Market closed. Sleeping until {key} IST")
        _last_sleep_log = key
    while True:
        rem = (nxt - now_ist()).total_seconds()
        if rem <= 1:
            _last_sleep_log = None
            return
        time.sleep(min(60, max(1, int(rem))))

# ================= RATE LIMIT =================
class RateLimiter:
    def __init__(self, per_sec=8, per_min=150):
        self.per_sec = per_sec
        self.per_min = per_min
        self.sec = deque()
        self.min = deque()

    def wait(self):
        now = time.time()
        while self.sec and now - self.sec[0] > 1:
            self.sec.popleft()
        while self.min and now - self.min[0] > 60:
            self.min.popleft()

        if len(self.sec) >= self.per_sec:
            time.sleep(0.2)
            return self.wait()

        if len(self.min) >= self.per_min:
            time.sleep(1)
            return self.wait()

        t = time.time()
        self.sec.append(t)
        self.min.append(t)

rl = RateLimiter()

# ================= FYERS =================
fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN, log_path="")

def fyers_profile_check():
    try:
        resp = fyers.get_profile()
        log(f"FYERS profile check: {resp.get('s')}")
        return resp.get("s") == "ok"
    except Exception as e:
        log(f"FYERS profile check failed: {e}")
        return False

def get_quotes():
    rl.wait()
    payload = {"symbols": ",".join(SYMBOLS)}
    try:
        data = fyers.quotes(data=payload)
    except TypeError:
        data = fyers.quotes(payload)

    out = {}
    for d in data.get("d", []):
        try:
            out[d["n"]] = float(d["v"].get("lp", 0))
        except Exception:
            pass
    return out

def get_history(symbol, resolution, days=5):
    rl.wait()
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
    return data.get("candles", [])

def get_option_chain_rows(symbol):
    rl.wait()
    payload = {"symbol": symbol, "strikecount": 12}
    try:
        data = fyers.optionchain(data=payload)
    except TypeError:
        data = fyers.optionchain(payload)

    rows = data.get("data", {}).get("optionsChain", [])
    if DEBUG_OI:
        log(f"OPTIONCHAIN {symbol} rows={len(rows)}")
        if rows:
            log(f"OPTIONCHAIN sample keys {symbol}: {list(rows[0].keys())}")
    return rows

# ================= TELEGRAM =================
def send(msg):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        log(msg)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=20,
        )
    except Exception as e:
        log(f"Telegram error: {e}")

# ================= OPTION CHAIN / OI =================
def strike_step_for_symbol(symbol: str, ltp: float) -> int:
    s = name(symbol)
    if "NIFTYBANK" in s or s == "BANKNIFTY":
        return 100
    if "NIFTY50" in s or s == "NIFTY":
        return 50
    if ltp >= 5000:
        return 100
    if ltp >= 1000:
        return 20
    if ltp >= 300:
        return 10
    return 5

def nearest_strike(ltp: float, step: int) -> int:
    return int(round(ltp / step) * step)

def extract_oi_value(row: dict, cp: str) -> float:
    """
    Tries multiple likely keys from optionchain row.
    cp = 'CE' or 'PE'
    """
    candidates = []

    # direct row values
    candidates.extend([
        row.get("oi_change"),
        row.get("changeinopeninterest"),
        row.get("oiChange"),
        row.get("changeInOpenInterest"),
        row.get("oich"),
    ])

    # nested CE/PE block if present
    leg = row.get(cp) if isinstance(row.get(cp), dict) else None
    if leg:
        candidates.extend([
            leg.get("oi_change"),
            leg.get("changeinopeninterest"),
            leg.get("oiChange"),
            leg.get("changeInOpenInterest"),
            leg.get("oich"),
        ])

    for v in candidates:
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            continue
    return 0.0

def get_oi_snapshot(symbol: str, ltp: float):
    rows = get_option_chain_rows(symbol)
    if not rows:
        return []

    step = strike_step_for_symbol(symbol, ltp)
    atm = nearest_strike(ltp, step)
    wanted = [atm - step, atm, atm + step, atm + 2 * step]

    snapshot = []

    for strike in wanted:
        ce_row = None
        pe_row = None

        for r in rows:
            try:
                sp = r.get("strike_price") or r.get("strikePrice")
                typ = (r.get("type") or "").upper()

                if sp is None:
                    continue
                sp = int(float(sp))

                if sp == strike and typ == "CE":
                    ce_row = r
                elif sp == strike and typ == "PE":
                    pe_row = r
            except Exception:
                continue

        ce = extract_oi_value(ce_row, "CE") if ce_row else 0.0
        pe = extract_oi_value(pe_row, "PE") if pe_row else 0.0

        snapshot.append({
            "strike": strike,
            "ce": ce,
            "pe": pe,
            "ce_text": f"{human_num(ce)} {arrow(ce)}",
            "pe_text": f"{human_num(pe)} {arrow(pe)}",
        })

    if DEBUG_OI:
        log(f"OI SNAPSHOT {symbol} @ {ltp}: {snapshot}")

    return snapshot

def classify_bias(snapshot, side):
    if not snapshot:
        return "NO OI", "WAIT"

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
        return ("⚠️ SELL WEAKENING", "EXIT SELL") if side == "SELL" else ("⚠️ BUY WEAKENING", "EXIT BUY")
    return ("⚪ SIDEWAYS", "WAIT")

# ================= TRADES =================
active_trades = {}
closed_trades = []
eod_sent_for_day = None

def trade_key(symbol, strategy, side):
    return f"{today_ist_str()}|{symbol}|{strategy}|{side}"

def register_trade(symbol, strategy, side, entry, target, stoploss):
    key = trade_key(symbol, strategy, side)
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
        "last_oi_update_ts": 0.0,
        "closed": False,
    }

def close_trade(trade, result, exit_price):
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
        "exit_price": round(exit_price, 2),
        "pl": pl,
    })
    active_trades.pop(trade["key"], None)

def manage_trade_by_price(trade, ltp):
    if trade.get("closed"):
        return

    if trade["side"] == "SELL":
        if ltp <= trade["target"]:
            trade["closed"] = True
            send(
                f"🎯 TARGET HIT\n\n"
                f"{name(trade['symbol'])}\n\n"
                f"Strategy : {trade['strategy']}\n"
                f"Entry    : {trade['entry']}\n"
                f"Target   : {trade['target']}\n"
                f"P/L      : +{round(trade['entry'] - trade['target'], 2)}"
            )
            close_trade(trade, "Target 🎯", trade["target"])
            return

        if ltp >= trade["stoploss"]:
            trade["closed"] = True
            send(
                f"🛑 STOPLOSS HIT\n\n"
                f"{name(trade['symbol'])}\n\n"
                f"Strategy : {trade['strategy']}\n"
                f"Entry    : {trade['entry']}\n"
                f"Stoploss : {trade['stoploss']}\n"
                f"P/L      : -{round(trade['stoploss'] - trade['entry'], 2)}"
            )
            close_trade(trade, "Stoploss 🛑", trade["stoploss"])
            return

    else:
        if ltp >= trade["target"]:
            trade["closed"] = True
            send(
                f"🎯 TARGET HIT\n\n"
                f"{name(trade['symbol'])}\n\n"
                f"Strategy : {trade['strategy']}\n"
                f"Entry    : {trade['entry']}\n"
                f"Target   : {trade['target']}\n"
                f"P/L      : +{round(trade['target'] - trade['entry'], 2)}"
            )
            close_trade(trade, "Target 🎯", trade["target"])
            return

        if ltp <= trade["stoploss"]:
            trade["closed"] = True
            send(
                f"🛑 STOPLOSS HIT\n\n"
                f"{name(trade['symbol'])}\n\n"
                f"Strategy : {trade['strategy']}\n"
                f"Entry    : {trade['entry']}\n"
                f"Stoploss : {trade['stoploss']}\n"
                f"P/L      : -{round(trade['entry'] - trade['stoploss'], 2)}"
            )
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
        f"Time   : {now_ist().strftime('%H:%M:%S')}",
    ]

    send("\n".join(lines))
    trade["last_oi_update_ts"] = time.time()

# ================= SAMPLE STRATEGY =================
# Replace this with your full gap/15m/pivot logic.
strategy_seen = set()

def sample_strategy(symbol, ltp):
    key = f"{today_ist_str()}|{symbol}"
    if key in strategy_seen:
        return

    candles = get_history(symbol, 5)
    if len(candles) < 2:
        return

    # Very simple sample sell setup using last completed 5m candle
    c = candles[-1]
    high = float(c[2])
    low = float(c[3])

    entry = low
    target = entry * (1 - TARGET_PCT)
    stoploss = high * (1 + SL_BUFFER_PCT)

    register_trade(symbol, "Sample Breakdown", "SELL", entry, target, stoploss)

    send(
        f"⚡ SELL ALERT ⚡\n\n"
        f"{name(symbol)}\n\n"
        f"Strategy : Sample Breakdown\n"
        f"Type     : SELL\n"
        f"Entry    : {round(entry, 2)}\n"
        f"Target   : {round(target, 2)}\n"
        f"SL       : {round(stoploss, 2)}"
    )

    strategy_seen.add(key)

# ================= EOD =================
def build_eod_summary():
    if not closed_trades:
        return "📘 EOD SUMMARY\n\nNo closed trades today."

    lines = ["📘 EOD SUMMARY", ""]
    for t in closed_trades:
        sign = "+" if t["pl"] > 0 else ""
        lines += [
            t["symbol"],
            f"Strategy : {t['strategy']}",
            f"Type     : {t['side']}",
            f"Entry    : {t['entry']}",
            f"Exit     : {t['exit_price']}",
            f"Result   : {t['result']}",
            f"P/L      : {sign}{t['pl']}",
            "",
        ]
    return "\n".join(lines).strip()

def reset_next_day_state():
    global eod_sent_for_day
    active_trades.clear()
    closed_trades.clear()
    strategy_seen.clear()
    eod_sent_for_day = None

# ================= MAIN =================
def main():
    global eod_sent_for_day

    log(f"Loaded watchlist: {SYMBOLS}")
    if not fyers_profile_check():
        raise Exception("FYERS authentication failed")

    send("🚀 BOT STARTED")

    while True:
        today = today_ist_str()

        if not is_market_open():
            if now_ist().time() >= dtime(15, 25) and eod_sent_for_day != today:
                try:
                    quotes = get_quotes()
                    for key in list(active_trades.keys()):
                        trade = active_trades.get(key)
                        if not trade:
                            continue
                        ltp = float(quotes.get(trade["symbol"], trade["entry"]))
                        close_trade(trade, "Day End", ltp)
                except Exception as e:
                    log(f"EOD close error: {e}")

                send(build_eod_summary())
                eod_sent_for_day = today
                sleep_until_next_market_open()
                reset_next_day_state()
                continue

            sleep_until_next_market_open()
            reset_next_day_state()
            continue

        try:
            quotes = get_quotes()

            # sample entries
            for sym, ltp in quotes.items():
                if ltp and ltp > 0:
                    sample_strategy(sym, float(ltp))

            # active trade management + OI only for active trades
            for key in list(active_trades.keys()):
                trade = active_trades.get(key)
                if not trade:
                    continue

                ltp = float(quotes.get(trade["symbol"], trade["entry"]))
                manage_trade_by_price(trade, ltp)

                if key not in active_trades:
                    continue

                maybe_send_oi_update(trade, ltp)

            # EOD while still running
            if now_ist().time() >= dtime(15, 25) and eod_sent_for_day != today:
                send(build_eod_summary())
                eod_sent_for_day = today

            time.sleep(POLL_SECONDS)

        except Exception as e:
            log(f"MAIN LOOP ERROR: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()

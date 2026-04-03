import os
import time
import math
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from fyers_apiv3 import fyersModel
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from bs4 import BeautifulSoup

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
CHAT_ID = (os.getenv("CHAT_ID") or "").strip()

WATCHLIST_RAW = (os.getenv("WATCHLIST") or "").strip()

AFTER_MARKET_RUN = (os.getenv("AFTER_MARKET_RUN", "true").strip().lower() == "true")

# Pattern filters
GAPUP_MIN_PCT = float(os.getenv("GAPUP_MIN_PCT", "0.0"))
GAPUP_CANDLE_MAX_PCT = float(os.getenv("GAPUP_CANDLE_MAX_PCT", "1.5"))
INSIDE15_FIRST_CANDLE_MIN_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MIN_PCT", "0.0"))
INSIDE15_FIRST_CANDLE_MAX_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MAX_PCT", "2.0"))

# Trade / risk
RISK_AMOUNT = float(os.getenv("RISK_AMOUNT", "500"))
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TARGET_RR = float(os.getenv("TARGET_RR", "1.0"))
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.1")) / 100.0
MAX_QTY = int(os.getenv("MAX_QTY", "100000"))

# Live tracking
LTP_INTERVAL_PER_STOCK = int(os.getenv("LTP_INTERVAL_PER_STOCK", "2"))
OI_INTERVAL_SECONDS = int(os.getenv("OI_INTERVAL_SECONDS", "180"))
OI_STOCK_GAP_SECONDS = int(os.getenv("OI_STOCK_GAP_SECONDS", "10"))
ALERT_GAP_SECONDS = int(os.getenv("ALERT_GAP_SECONDS", "300"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))

# Pivot filter
PIVOT_LTP_FILTER_PCT = float(os.getenv("PIVOT_LTP_FILTER_PCT", "3.0")) / 100.0
PIVOT_MIN_YDAY_TURNOVER = float(os.getenv("PIVOT_MIN_YDAY_TURNOVER", "0"))

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

# ================= GLOBAL STATE =================
watch_candidates = {}
active_trades = {}
closed_for_day = set()
blocked_entries = []
pattern_summary = {
    "gapup": [],
    "inside15": [],
    "pivot30": []
}
eod_stats = {
    "entries": [],
    "targets": [],
    "stoplosses": [],
    "dayend": [],
    "blocked": [],
    "closed": []
}
last_alert_time = {}
pivot_scan_done_keys = set()

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def now_epoch():
    return time.time()

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


def text_to_image_bytes(text, width=1200, padding=30, line_gap=12, font_size=26):
    lines = text.split("\n")

    try:
        font = ImageFont.truetype("DejaVuSans.ttf", font_size)
    except Exception:
        font = ImageFont.load_default()

    dummy = Image.new("RGB", (width, 100), "white")
    draw = ImageDraw.Draw(dummy)

    line_heights = []
    max_width = 0

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        line_heights.append(h)
        max_width = max(max_width, w)

    img_width = max(width, max_width + padding * 2)
    img_height = padding * 2 + sum(line_heights) + line_gap * max(0, len(lines) - 1)

    img = Image.new("RGB", (img_width, img_height), "white")
    draw = ImageDraw.Draw(img)

    y = padding
    for i, line in enumerate(lines):
        draw.text((padding, y), line, fill="black", font=font)
        y += line_heights[i] + line_gap

    bio = BytesIO()
    bio.name = "report.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def send_photo_from_text(text, caption=""):
    print(text, flush=True)

    if not TELEGRAM_TOKEN or not CHAT_ID:
        return

    try:
        img_bytes = text_to_image_bytes(text)

        files = {
            "photo": ("report.png", img_bytes, "image/png")
        }
        data = {
            "chat_id": CHAT_ID,
            "caption": caption[:1024] if caption else ""
        }

        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data=data,
            files=files,
            timeout=60
        )
    except Exception as e:
        log(f"Telegram photo error: {e}")

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

def throttle_ok(key: str) -> bool:
    t = last_alert_time.get(key, 0)
    if now_epoch() - t >= ALERT_GAP_SECONDS:
        last_alert_time[key] = now_epoch()
        return True
    return False

def short_name(symbol: str) -> str:
    right = symbol.split(":")[1]
    return right.replace("-EQ", "").replace("-INDEX", "")

def candle_dt(ts: int):
    return datetime.fromtimestamp(ts, IST)

def pct_range(high: float, low: float, close: float) -> float:
    if close == 0:
        return 0.0
    return ((high - low) / close) * 100.0

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def human_format(n):
    n = safe_float(n, 0.0)
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 10000000:
        return f"{sign}{(n/10000000):.2f}".rstrip("0").rstrip(".") + "Cr"
    if n >= 100000:
        return f"{sign}{(n/100000):.2f}".rstrip("0").rstrip(".") + "L"
    if n >= 1000:
        return f"{sign}{(n/1000):.2f}".rstrip("0").rstrip(".") + "K"
    if float(n).is_integer():
        return f"{sign}{int(n)}"
    return f"{sign}{n:.2f}".rstrip("0").rstrip(".")

def arrow(v):
    v = safe_float(v, 0.0)
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

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
def fetch_nse_holidays_from_web(year=None):
    if year is None:
        year = now_ist().year

    url = "https://www.nseindia.com/resources/exchange-communication-holidays"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.headers.update(headers)

    try:
        session.get("https://www.nseindia.com", timeout=20)
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"Holiday fetch failed: {e}")
        return set()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)

    holidays = set()
    for token in text.replace(",", " ").split():
        token = token.strip()
        try:
            dt = datetime.strptime(token, "%d-%b-%Y")
            if dt.year == year:
                holidays.add(dt.strftime("%Y-%m-%d"))
        except Exception:
            pass

    return holidays


def get_holiday_set():
    env_holidays = set()
    for part in NSE_HOLIDAYS_RAW.replace(";", ",").split(","):
        p = part.strip()
        if p:
            env_holidays.add(p)

    web_holidays = fetch_nse_holidays_from_web(now_ist().year)
    merged = env_holidays | web_holidays

    log(f"NSE holidays loaded: {sorted(list(merged))}")
    return merged


try:
    HOLIDAYS = get_holiday_set()
except Exception as e:
    log(f"Holiday init failed: {e}")
    HOLIDAYS = set()

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

def get_reference_symbol():
    # prefer index if present, else first stock in watchlist
    for sym in SYMBOLS:
        if sym.endswith("-INDEX"):
            return sym
    return SYMBOLS[0]


def get_last_available_session_date():
    ref_symbol = get_reference_symbol()

    # use intraday data itself to decide latest valid session date
    candles = get_history(ref_symbol, 5, 10)

    if not candles:
        # fallback only if fyers returned nothing
        now = now_ist()
        return now.strftime("%Y-%m-%d")

    # latest candle date present in fyers
    last_dt = candle_dt(candles[-1])
    return last_dt.strftime("%Y-%m-%d")


def analysis_date_str():
    return get_last_available_session_date()


def log_analysis_date_debug():
    ref_symbol = get_reference_symbol()
    candles = get_history(ref_symbol, 5, 10)

    if candles and isinstance(candles[-1], list) and len(candles[-1]) > 0:
        try:
            ts = int(candles[-1][0])
            last_dt = candle_dt(ts).strftime("%Y-%m-%d %H:%M")

            log(f"Reference symbol: {ref_symbol}")
            log(f"Latest candle from FYERS: {last_dt}")
            log(f"Analysis date selected: {analysis_date_str()}")

        except Exception as e:
            log(f"Debug parse error: {e}")
    else:
        log("No valid candles returned from FYERS")

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
    daily = get_history(symbol, "D", 40)
    today_str = now_ist().strftime("%Y-%m-%d")

    prev = []
    for c in daily:
        try:
            c_day = candle_dt(c[0]).strftime("%Y-%m-%d")
            if c_day < today_str:
                prev.append(c)
        except Exception:
            pass

    prev.sort(key=lambda x: x[0])
    return prev[-1] if prev else None


def get_previous_weekly(symbol):
    weekly = get_history(symbol, "W", 80)
    today_str = now_ist().strftime("%Y-%m-%d")

    prev = []
    for c in weekly:
        try:
            c_day = candle_dt(c[0]).strftime("%Y-%m-%d")
            if c_day < today_str:
                prev.append(c)
        except Exception:
            pass

    prev.sort(key=lambda x: x[0])
    return prev[-1] if prev else None


def fetch_quotes(symbol):
    payload = {"symbols": symbol}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception as e:
        log(f"QUOTES ERROR {symbol}: {e}")
        return {}

    items = resp.get("d") or []
    if not items:
        return {}

    item = items[0] if isinstance(items[0], dict) else {}
    vals = item.get("v") or {}

    return {  
    "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),  
    "open": safe_float(vals.get("open_price") or vals.get("open") or vals.get("openPrice"), 0.0),  
    "high": safe_float(vals.get("high_price") or vals.get("high") or vals.get("highPrice"), 0.0),  
    "low": safe_float(vals.get("low_price") or vals.get("low") or vals.get("lowPrice"), 0.0),  
    "prev_close": safe_float(vals.get("prev_close_price") or vals.get("prev_close") or vals.get("prevClose"), 0.0),  
    }

def fetch_option_chain(symbol, strikecount=10, timestamp=""):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": timestamp}
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)
    except Exception as e:
        log(f"OPTIONCHAIN ERROR {symbol}: {e}")
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

def normalize_chain_fast(options_list):
    call_map = {}
    put_map = {}

    for x in options_list:
        if not isinstance(x, dict):
            continue

        strike = (
            x.get("strike_price")
            or x.get("strikePrice")
            or x.get("strike")
            or x.get("sp")
        )
        if strike is None:
            continue
        strike = safe_float(strike, None)
        if strike is None:
            continue

        option_type = str(
            x.get("option_type")
            or x.get("optionType")
            or x.get("type")
            or x.get("otype")
            or ""
        ).upper().strip()

        sym = str(x.get("symbol", "")).upper()

        row = {
            "ltp": safe_float(x.get("ltp") or x.get("last_price") or x.get("lastPrice"), 0.0),
            "chg": safe_float(x.get("chg") or x.get("change") or x.get("ch"), 0.0),
            "iv": safe_float(x.get("iv") or x.get("implied_volatility") or x.get("impliedVolatility"), 0.0),
            "oi": safe_float(x.get("oi") or x.get("open_interest") or x.get("openInterest"), 0.0),
            "oi_change": safe_float(x.get("oich") or x.get("oi_change") or x.get("oiChange"), 0.0),
            "volume": safe_float(x.get("volume") or x.get("vol") or x.get("tradedVolume") or x.get("tot_vol"), 0.0),
        }

        if option_type in ("CE", "CALL", "C") or sym.endswith("CE"):
            call_map[int(strike)] = row
        elif option_type in ("PE", "PUT", "P") or sym.endswith("PE"):
            put_map[int(strike)] = row

    strikes = sorted(set(call_map.keys()) | set(put_map.keys()))
    rows = []
    for strike in strikes:
        c = call_map.get(strike, {})
        p = put_map.get(strike, {})
        rows.append({
            "strike": int(strike),
            "call_oi": c.get("oi", 0.0),
            "call_oich": c.get("oi_change", 0.0),
            "put_oi": p.get("oi", 0.0),
            "put_oich": p.get("oi_change", 0.0),
        })
    return rows

# ================= OI =================
def get_oi_snapshot(symbol, ltp):
    resp = fetch_option_chain(symbol, strikecount=10, timestamp="")
    option_rows = extract_options_chain_list(resp)
    parsed = normalize_chain_fast(option_rows)

    if not parsed:
        return [], "NEUTRAL"

    strikes = [r["strike"] for r in parsed]
    atm = min(strikes, key=lambda x: abs(x - ltp))
    atm_idx = strikes.index(atm)

    start = max(0, atm_idx - 1)
    end = min(len(parsed), start + 4)
    selected = parsed[start:end]

    ce_total = sum(r["call_oich"] for r in selected)
    pe_total = sum(r["put_oich"] for r in selected)

    bias = "NEUTRAL"
    if pe_total > ce_total:
        bias = "BULLISH"
    elif ce_total > pe_total:
        bias = "BEARISH"

    return selected, bias

def format_oi_snapshot(rows):
    if not rows:
        return "Strike    PE       | CE"
    out = ["Strike    PE       | CE"]
    for r in rows:
        out.append(
            f"{r['strike']:<8} {human_format(r['put_oich'])}{arrow(r['put_oich'])} | "
            f"{human_format(r['call_oich'])}{arrow(r['call_oich'])}"
        )
    return "\n".join(out)

def hold_status(side, bias):
    if side == "BUY":
        return "Buy Hold 🟢" if bias == "BULLISH" else "Exit ⚪"
    if side == "SELL":
        return "Sell Hold 🔴" if bias == "BEARISH" else "Exit ⚪"
    return "Exit ⚪"

# ================= POSITION SIZE =================
def calc_position(entry, stoploss):
    risk_per_share = abs(entry - stoploss)
    if risk_per_share <= 0:
        return 0, 0.0, 0.0, 0.0

    qty = max(1, int(round(RISK_AMOUNT / risk_per_share)))
    qty = min(qty, MAX_QTY)

    exposure = entry * qty
    margin = exposure / LEVERAGE if LEVERAGE > 0 else exposure

    return qty, exposure, margin, risk_per_share

# ================= RESULT ENGINE =================
def evaluate_sell_result(candles_after_entry, entry, target, stoploss):
    for c in candles_after_entry:
        high = float(c[2]); low = float(c[3])

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
        high = float(c[2]); low = float(c[3])

        if low <= stoploss and high >= target:
            return "Stoploss 🛑", stoploss
        if low <= stoploss:
            return "Stoploss 🛑", stoploss
        if high >= target:
            return "Target 🎯", target

    if candles_after_entry:
        return "Day End ⚪", float(candles_after_entry[-1][4])

    return "No Data", entry

# ================= PATTERN SCANNERS =================
def scan_gapup_pattern(symbol):
    if symbol in closed_for_day:
        return None

    prev_day = get_previous_daily(symbol)
    day_5m = get_analysis_day_candles(symbol, 5, 7)
    if prev_day is None or len(day_5m) < 1:
        return None

    first = day_5m[0]
    prev_high = float(prev_day[2])
    o = float(first[1]); h = float(first[2]); l = float(first[3]); c = float(first[4])

    gap_pct = ((o - prev_high) / prev_high) * 100 if prev_high else 0.0
    candle_pct = pct_range(h, l, c)

    if not (o > prev_high and gap_pct >= GAPUP_MIN_PCT and candle_pct <= GAPUP_CANDLE_MAX_PCT):
        return None

    entry = round(l, 2)
    sl = round(h * (1 + SL_BUFFER_PCT), 2)
    target = round(entry - (sl - entry) * TARGET_RR, 2)

    return {
        "symbol": symbol,
        "strategy": "GAPUP_PLUS",
        "side": "SELL",
        "gap_pct": round(gap_pct, 2),
        "entry": entry,
        "stoploss": sl,
        "target": target,
        "pattern_time": candle_dt(first[0]).strftime("%H:%M")
    }

def scan_15m_inside_pattern(symbol):
    if symbol in closed_for_day:
        return None

    day_15m = get_analysis_day_candles(symbol, 15, 7)
    if len(day_15m) < 2:
        return None

    c1 = day_15m[0]
    c2 = day_15m[1]

    h1 = float(c1[2]); l1 = float(c1[3]); c1_close = float(c1[4])
    h2 = float(c2[2]); l2 = float(c2[3])

    if c1_close <= 0:
        return None

    range_pct = pct_range(h1, l1, c1_close)
    inside = h2 <= h1 and l2 >= l1

    if not (INSIDE15_FIRST_CANDLE_MIN_PCT <= range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    return {
        "symbol": symbol,
        "strategy": "INSIDE_15M",
        "range_pct": round(range_pct, 2),
        "buy_entry": round(h1, 2),
        "buy_stoploss": round(l1 * (1 - SL_BUFFER_PCT), 2),
        "buy_target": round(h1 + (h1 - (l1 * (1 - SL_BUFFER_PCT))) * TARGET_RR, 2),
        "sell_entry": round(l1, 2),
        "sell_stoploss": round(h1 * (1 + SL_BUFFER_PCT), 2),
        "sell_target": round(l1 - ((h1 * (1 + SL_BUFFER_PCT)) - l1) * TARGET_RR, 2),
        "pattern_time": candle_dt(c2[0]).strftime("%H:%M")
    }

def compute_weekly_r_levels(prev_week):
    h = float(prev_week[2]); l = float(prev_week[3]); c = float(prev_week[4])

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
    high = float(candle[2]); low = float(candle[3])
    return low <= level <= high

def eligible_for_pivot_scan(symbol):
    prev_day = get_previous_daily(symbol)
    q = fetch_quotes(symbol)
    if prev_day is None or not q:
        return False

    yesterday_close = float(prev_day[4])
    today_ltp = q.get("ltp", 0.0)
    prev_turnover = safe_float(prev_day[5], 0.0) * yesterday_close if len(prev_day) > 5 else 0.0

    if PIVOT_MIN_YDAY_TURNOVER > 0 and prev_turnover < PIVOT_MIN_YDAY_TURNOVER:
        return False

    return today_ltp >= yesterday_close * (1 + PIVOT_LTP_FILTER_PCT)

def scan_30m_pivot_sell(symbol):
    if symbol in closed_for_day:
        return None

    if not eligible_for_pivot_scan(symbol):
        return None

    prev_week = get_previous_weekly(symbol)
    day_30m = get_analysis_day_candles(symbol, 30, 21)

    if prev_week is None or len(day_30m) < 3:
        return None

    c1 = day_30m[0]; c2 = day_30m[1]
    c1_open = float(c1[1]); c1_close = float(c1[4])
    c2_open = float(c2[1]); c2_close = float(c2[4])
    c2_high = float(c2[2]); c2_low = float(c2[3])

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

    entry = round(c2_low, 2)
    stoploss = round(c2_high, 2)
    if stoploss <= entry:
        return None
    target = round(entry - (stoploss - entry) * TARGET_RR, 2)

    return {
        "symbol": symbol,
        "strategy": "PIVOT_30M_WEEKLY_SELL",
        "side": "SELL",
        "pivot_name": pivot_name,
        "pivot_value": pivot_value,
        "entry": entry,
        "stoploss": stoploss,
        "target": target,
        "pattern_time": candle_dt(c2[0]).strftime("%H:%M")
    }

# ================= SUMMARY FORMATTERS =================
def format_gapup_summary(items):
    if not items:
        return "⚡ GAP UP PLUS STOCKS (%) ⚡\n\nNone"
    lines = ["⚡ GAP UP PLUS STOCKS (%) ⚡", ""]
    for i, x in enumerate(sorted(items, key=lambda z: z["gap_pct"], reverse=True), 1):
        lines.append(f"{i}. {short_name(x['symbol'])} ({x['gap_pct']}%)")
    return "\n".join(lines)

def format_inside_summary(items):
    if not items:
        return "🕯️ 15 MIN INSIDE CANDLE STOCKS (%) 🕯️\n\nNone"
    lines = ["🕯️ 15 MIN INSIDE CANDLE STOCKS (%) 🕯️", ""]
    for i, x in enumerate(sorted(items, key=lambda z: z["range_pct"]), 1):
        lines.append(f"{i}. {short_name(x['symbol'])} ({x['range_pct']}%)")
    return "\n".join(lines)

def format_pivot_summary(items):
    if not items:
        return "📍 30 MIN WEEKLY PIVOT SELL STOCKS\n\nNone"
    lines = ["📍 30 MIN WEEKLY PIVOT SELL STOCKS", ""]
    for i, x in enumerate(items, 1):
        lines.append(f"{i}. {short_name(x['symbol'])} ({x['pivot_name']}={x['pivot_value']})")
    return "\n".join(lines)

# ================= WATCH / ENTRY =================
def add_watch_candidate(symbol, payload):
    if symbol in closed_for_day:
        return
    if symbol in active_trades:
        return

    prev = watch_candidates.get(symbol)
    if prev and prev.get("strategy") == payload.get("strategy"):
        return

    watch_candidates[symbol] = payload

def block_trade(symbol, strategy, side, reason):
    blocked_entries.append({
        "symbol": short_name(symbol),
        "strategy": strategy,
        "side": side,
        "reason": reason,
        "time": now_ist().strftime("%H:%M:%S")
    })
    eod_stats["blocked"].append({
        "symbol": short_name(symbol),
        "strategy": strategy,
        "side": side,
        "reason": reason
    })

def send_entry_alert(symbol, trade, oi_rows, oi_bias):
    qty, exposure, margin, risk_per_share = calc_position(trade["entry"], trade["stoploss"])
    trade["qty"] = qty
    trade["exposure"] = round(exposure, 2)
    trade["margin"] = round(margin, 2)
    trade["risk_per_share"] = round(risk_per_share, 2)

    side_icon = "🟢" if trade["side"] == "BUY" else "🔴"
    strategy_name = trade["strategy"]

    msg = (
        f"{side_icon} ENTRY CONFIRMED\n\n"
        f"Stock: {short_name(symbol)}\n"
        f"Strategy: {strategy_name}\n"
        f"Side: {trade['side']}\n\n"
        f"Entry: {trade['entry']}\n"
        f"Target: {trade['target']}\n"
        f"Stoploss: {trade['stoploss']}\n\n"
        f"Risk: ₹{int(RISK_AMOUNT)}\n"
        f"Risk/Share: {trade['risk_per_share']}\n"
        f"Qty: {qty}\n"
        f"Exposure: {round(exposure)}\n"
        f"Margin(~{int(LEVERAGE)}X): {round(margin)}\n\n"
        f"OI: {oi_bias} ✅\n"
        f"{format_oi_snapshot(oi_rows)}"
    )
    send_long_message(msg)

def try_entry_for_candidate(symbol):
    if symbol in closed_for_day:
        return
    if symbol in active_trades:
        return
    if symbol not in watch_candidates:
        return

    q = fetch_quotes(symbol)
    ltp = q.get("ltp", 0.0)
    if ltp <= 0:
        return

    c = watch_candidates[symbol]
    strategy = c["strategy"]

    if strategy == "GAPUP_PLUS":
        if ltp <= c["entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BEARISH":
                if throttle_ok(f"{symbol}|blocked|SELL"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nSELL trigger hit, but OI is against SELL")
                block_trade(symbol, strategy, "SELL", "OI against SELL")
                del watch_candidates[symbol]
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "SELL",
                "entry": c["entry"],
                "target": c["target"],
                "stoploss": c["stoploss"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "SELL"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]
        return

    if strategy == "INSIDE_15M":
        if ltp >= c["buy_entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BULLISH":
                if throttle_ok(f"{symbol}|blocked|BUY"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nBUY trigger hit, but OI is against BUY")
                block_trade(symbol, strategy, "BUY", "OI against BUY")
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "BUY",
                "entry": c["buy_entry"],
                "target": c["buy_target"],
                "stoploss": c["buy_stoploss"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "BUY"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]
            return

        if ltp <= c["sell_entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BEARISH":
                if throttle_ok(f"{symbol}|blocked|SELL"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nSELL trigger hit, but OI is against SELL")
                block_trade(symbol, strategy, "SELL", "OI against SELL")
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "SELL",
                "entry": c["sell_entry"],
                "target": c["sell_target"],
                "stoploss": c["sell_stoploss"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "SELL"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]
            return

    if strategy == "PIVOT_30M_WEEKLY_SELL":
        if ltp <= c["entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BEARISH":
                if throttle_ok(f"{symbol}|blocked|PIVOTSELL"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nSELL trigger hit, but OI is against SELL")
                block_trade(symbol, strategy, "SELL", "OI against SELL")
                del watch_candidates[symbol]
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "SELL",
                "entry": c["entry"],
                "target": c["target"],
                "stoploss": c["stoploss"],
                "pivot_name": c["pivot_name"],
                "pivot_value": c["pivot_value"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "SELL"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]

# ================= LIVE TRADE TRACKING =================
def close_trade(symbol, reason, exit_price):
    if symbol not in active_trades:
        return
    trade = active_trades.pop(symbol)
    trade["close_reason"] = reason
    trade["exit_price"] = round(exit_price, 2)
    trade["close_time"] = now_ist().strftime("%H:%M:%S")
    closed_for_day.add(symbol)

    qty = trade.get("qty", 0)
    if trade["side"] == "BUY":
        pnl = round((exit_price - trade["entry"]) * qty, 2)
    else:
        pnl = round((trade["entry"] - exit_price) * qty, 2)
    trade["pnl"] = pnl

    if reason.startswith("Target"):
        eod_stats["targets"].append({"symbol": short_name(symbol), "strategy": trade["strategy"], "pnl": pnl})
        icon = "🎯"
    elif reason.startswith("Stoploss"):
        eod_stats["stoplosses"].append({"symbol": short_name(symbol), "strategy": trade["strategy"], "pnl": pnl})
        icon = "🛑"
    else:
        eod_stats["dayend"].append({"symbol": short_name(symbol), "strategy": trade["strategy"], "pnl": pnl})
        icon = "⚪"

    eod_stats["closed"].append({
        "symbol": short_name(symbol),
        "strategy": trade["strategy"],
        "side": trade["side"],
        "entry": trade["entry"],
        "exit": trade["exit_price"],
        "pnl": pnl,
        "reason": reason
    })

    side_icon = "🟢" if trade["side"] == "BUY" else "🔴"
    send(
        f"{icon} TRADE CLOSED\n\n"
        f"Stock: {short_name(symbol)}\n"
        f"Strategy: {trade['strategy']}\n"
        f"Side: {trade['side']} {side_icon}\n"
        f"Entry: {trade['entry']}\n"
        f"Exit: {trade['exit_price']}\n"
        f"Qty: {qty}\n"
        f"P/L: {pnl}\n"
        f"Reason: {reason}"
    )

def track_active_trade(symbol):
    if symbol not in active_trades:
        return

    trade = active_trades[symbol]
    q = fetch_quotes(symbol)
    ltp = q.get("ltp", 0.0)
    if ltp <= 0:
        return

    if trade["side"] == "BUY":
        if ltp <= trade["stoploss"]:
            close_trade(symbol, "Stoploss 🛑", trade["stoploss"])
            return
        if ltp >= trade["target"]:
            close_trade(symbol, "Target 🎯", trade["target"])
            return
    else:
        if ltp >= trade["stoploss"]:
            close_trade(symbol, "Stoploss 🛑", trade["stoploss"])
            return
        if ltp <= trade["target"]:
            close_trade(symbol, "Target 🎯", trade["target"])
            return

    if now_epoch() - trade.get("last_oi_check", 0) >= OI_INTERVAL_SECONDS:
        oi_rows, bias = get_oi_snapshot(symbol, ltp)
        status = hold_status(trade["side"], bias)
        trade["last_oi_check"] = now_epoch()

        if throttle_ok(f"{symbol}|live_oi"):
            side_icon = "🟢" if trade["side"] == "BUY" else "🔴"
            send_long_message(
                f"{side_icon} LIVE TRADE TRACKING\n\n"
                f"Stock: {short_name(symbol)}\n"
                f"Side: {trade['side']}\n"
                f"Strategy: {trade['strategy']}\n"
                f"Entry: {trade['entry']}\n"
                f"SL: {trade['stoploss']}\n"
                f"Target: {trade['target']}\n"
                f"LTP: {ltp}\n\n"
                f"Status: {status}\n\n"
                f"{format_oi_snapshot(oi_rows)}"
            )

        if status == "Exit ⚪" and throttle_ok(f"{symbol}|oi_exit"):
            send(
                f"⚪ OI EXIT SIGNAL\n\n"
                f"Stock: {short_name(symbol)}\n"
                f"Side: {trade['side']}\n"
                f"Strategy: {trade['strategy']}\n"
                f"LTP: {ltp}\n"
                f"Reason: OI turned against trade"
            )

# ================= SCAN SCHEDULERS =================
def scan_gapup_once():
    items = []
    for sym in SYMBOLS:
        try:
            r = scan_gapup_pattern(sym)
            if r:
                items.append(r)
                add_watch_candidate(sym, r)
        except Exception as e:
            log(f"GAP SCAN ERROR {sym}: {e}")
    pattern_summary["gapup"] = items
    send_photo_from_text(format_gapup_summary(items), "Gap Up Plus")

def scan_inside15_once():
    items = []
    for sym in SYMBOLS:
        try:
            r = scan_15m_inside_pattern(sym)
            if r:
                items.append(r)
                add_watch_candidate(sym, r)
        except Exception as e:
            log(f"15M SCAN ERROR {sym}: {e}")
    pattern_summary["inside15"] = items
    send_photo_from_text(format_inside_summary(items), "15 Min Inside")

def pivot_scan_key():
    now = now_ist()
    return f"{now.strftime('%Y-%m-%d')}-{now.hour:02d}-{now.minute:02d}"

def should_run_pivot_scan():
    now = now_ist().time()
    valid_times = {
        dtime(9,45), dtime(10,15), dtime(10,45), dtime(11,15), dtime(11,45),
        dtime(12,15), dtime(12,45), dtime(13,15), dtime(13,45), dtime(14,15),
        dtime(14,45), dtime(15,15)
    }
    current = dtime(now.hour, now.minute)
    return current in valid_times

def scan_pivot_30m_once():
    key = pivot_scan_key()
    if key in pivot_scan_done_keys:
        return
    pivot_scan_done_keys.add(key)

    items = []
    for sym in SYMBOLS:
        try:
            r = scan_30m_pivot_sell(sym)
            if r:
                items.append(r)
                add_watch_candidate(sym, r)
        except Exception as e:
            log(f"PIVOT SCAN ERROR {sym}: {e}")
    pattern_summary["pivot30"] = items
    send_photo_from_text(format_pivot_summary(items), "30 Min Weekly Pivot Sell")

# ================= LIVE LOOP =================
def run_live_day():
    gap_summary_sent = False
    inside_summary_sent = False
    eod_sent = False

    while True:
        if not is_market_open():
            return

        nowt = now_ist().time()

        if not gap_summary_sent and nowt >= dtime(9, 20):
            scan_gapup_once()
            gap_summary_sent = True

        if not inside_summary_sent and nowt >= dtime(9, 45):
            scan_inside15_once()
            inside_summary_sent = True

        if should_run_pivot_scan():
            scan_pivot_30m_once()

        for sym in list(watch_candidates.keys()):
            if sym in closed_for_day or sym in active_trades:
                continue
            try:
                try_entry_for_candidate(sym)
            except Exception as e:
                log(f"ENTRY ERROR {sym}: {e}")
            time.sleep(1)

        for sym in list(active_trades.keys()):
            try:
                track_active_trade(sym)
            except Exception as e:
                log(f"TRACK ERROR {sym}: {e}")
            time.sleep(LTP_INTERVAL_PER_STOCK)

        if not eod_sent and nowt >= dtime(15, 28):
            send_photo_from_text(build_eod_report(), "End of Day Report")
            nxt = next_market_open_datetime()
            send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            eod_sent = True

        time.sleep(POLL_SECONDS)

# ================= AFTER MARKET SUMMARY =================
def evaluate_gapup_after_market(symbol):
    prev_day = get_previous_daily(symbol)
    day_5m = get_analysis_day_candles(symbol, 5, 7)
    if prev_day is None or len(day_5m) < 1:
        return None

    first = day_5m[0]
    prev_high = float(prev_day[2])

    o = float(first[1]); h = float(first[2]); l = float(first[3]); c = float(first[4])
    gap_pct = ((o - prev_high) / prev_high) * 100 if prev_high else 0.0
    candle_pct = pct_range(h, l, c)

    if not (o > prev_high and gap_pct >= GAPUP_MIN_PCT and candle_pct <= GAPUP_CANDLE_MAX_PCT):
        return None

    entry = round(l, 2)
    stoploss = round(h * (1 + SL_BUFFER_PCT), 2)
    target = round(entry - (stoploss - entry) * TARGET_RR, 2)
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

def evaluate_inside_after_market(symbol):
    day_15m = get_analysis_day_candles(symbol, 15, 7)
    if len(day_15m) < 2:
        return None

    c1 = day_15m[0]; c2 = day_15m[1]
    h1 = float(c1[2]); l1 = float(c1[3]); c1_close = float(c1[4])
    h2 = float(c2[2]); l2 = float(c2[3])

    if c1_close <= 0:
        return None

    range_pct = pct_range(h1, l1, c1_close)
    inside = h2 <= h1 and l2 >= l1
    if not (INSIDE15_FIRST_CANDLE_MIN_PCT <= range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    later = day_15m[2:]

    buy_entry = round(h1, 2)
    buy_sl = round(l1 * (1 - SL_BUFFER_PCT), 2)
    buy_target = round(buy_entry + (buy_entry - buy_sl) * TARGET_RR, 2)
    buy_result, buy_exit = evaluate_buy_result(later, buy_entry, buy_target, buy_sl)
    buy_pl = round(buy_exit - buy_entry, 2)

    sell_entry = round(l1, 2)
    sell_sl = round(h1 * (1 + SL_BUFFER_PCT), 2)
    sell_target = round(sell_entry - (sell_sl - sell_entry) * TARGET_RR, 2)
    sell_result, sell_exit = evaluate_sell_result(later, sell_entry, sell_target, sell_sl)
    sell_pl = round(sell_entry - sell_exit, 2)

    return {
        "symbol": short_name(symbol),
        "range_pct": round(range_pct, 2),
        "buy": {"entry": buy_entry, "target": buy_target, "stoploss": buy_sl, "result": buy_result, "exit_price": round(buy_exit, 2), "pl": buy_pl},
        "sell": {"entry": sell_entry, "target": sell_target, "stoploss": sell_sl, "result": sell_result, "exit_price": round(sell_exit, 2), "pl": sell_pl},
    }

def evaluate_pivot_after_market(symbol):
    prev_week = get_previous_weekly(symbol)
    day_30m = get_analysis_day_candles(symbol, 30, 21)
    if prev_week is None or len(day_30m) < 3:
        return None

    c1 = day_30m[0]; c2 = day_30m[1]; c3 = day_30m[2]
    c1_open = float(c1[1]); c1_close = float(c1[4])
    c2_open = float(c2[1]); c2_close = float(c2[4])
    c2_high = float(c2[2]); c2_low = float(c2[3])
    c3_low = float(c3[3]); c3_high = float(c3[2])

    if not (c1_close > c1_open and c2_close < c2_open):
        return None

    r_levels = compute_weekly_r_levels(prev_week)
    touched = []
    for name, value in r_levels.items():
        if candle_touches_level(c1, value) and candle_touches_level(c2, value):
            touched.append((name, value))
    if not touched:
        return None

    pivot_name, pivot_value = touched[-1]
    entry = round(c2_low, 2)
    stoploss = round(c2_high, 2)
    if stoploss <= entry:
        return None
    target = round(entry - (stoploss - entry) * TARGET_RR, 2)

    if c3_low > entry:
        result = "No Entry"
        exit_price = entry
        pl = 0.0
    elif c3_high >= stoploss and c3_low <= target:
        result = "Stoploss 🛑"
        exit_price = stoploss
        pl = round(entry - exit_price, 2)
    elif c3_high >= stoploss:
        result = "Stoploss 🛑"
        exit_price = stoploss
        pl = round(entry - exit_price, 2)
    elif c3_low <= target:
        result = "Target 🎯"
        exit_price = target
        pl = round(entry - exit_price, 2)
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

def format_gapup_results(items):
    if not items:
        return "📘 GAP UP PLUS - IF ENTRY TAKEN\n\nNone"
    lines = ["📘 GAP UP PLUS - IF ENTRY TAKEN", ""]
    for x in items:
        sign = "+" if x["pl"] > 0 else ""
        lines += [
            x["symbol"],
            f"SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
            f"Result:{x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

def format_inside_results(items):
    if not items:
        return "📘 15 MIN INSIDE CANDLE - IF ENTRY TAKEN\n\nNone"
    lines = ["📘 15 MIN INSIDE CANDLE - IF ENTRY TAKEN", ""]
    for x in items:
        b = x["buy"]; s = x["sell"]
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

def format_pivot_results(items):
    if not items:
        return "📘 30 MIN WEEKLY PIVOT SELL - IF ENTRY TAKEN\n\nNone"
    lines = ["📘 30 MIN WEEKLY PIVOT SELL - IF ENTRY TAKEN", ""]
    for x in items:
        sign = "+" if x["pl"] > 0 else ""
        lines += [
            x["symbol"],
            f"Level:{x['pivot_name']} ({x['pivot_value']})",
            f"🔴 SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
            f"      {x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

def run_after_market_once():
    send("📡 Running after-market scan...")

    gap_items = []
    inside_items = []
    pivot_items = []

    for sym in SYMBOLS:
        try:
            g = evaluate_gapup_after_market(sym)
            if g:
                gap_items.append(g)
        except Exception as e:
            log(f"GAP AFTER ERROR {sym}: {e}")

        try:
            i = evaluate_inside_after_market(sym)
            if i:
                inside_items.append(i)
        except Exception as e:
            log(f"15M AFTER ERROR {sym}: {e}")

        try:
            p = evaluate_pivot_after_market(sym)
            if p:
                pivot_items.append(p)
        except Exception as e:
            log(f"PIVOT AFTER ERROR {sym}: {e}")

    send_long_message(format_gapup_summary([{"symbol": f"NSE:{x['symbol']}-EQ", "gap_pct": x["gap_pct"]} for x in gap_items]))
    send_long_message(format_inside_summary([{"symbol": f"NSE:{x['symbol']}-EQ", "range_pct": x["range_pct"]} for x in inside_items]))
    send_long_message(format_pivot_summary([{"symbol": f"NSE:{x['symbol']}-EQ", "pivot_name": x["pivot_name"], "pivot_value": x["pivot_value"]} for x in pivot_items]))

    send_photo_from_text(format_gapup_results(gap_items), "Gap Up Result")
    send_photo_from_text(format_inside_results(inside_items), "15 Min Inside Result")
    send_photo_from_text(format_pivot_results(pivot_items), "Pivot Result")

    nxt = next_market_open_datetime()
    send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")

# ================= EOD REPORT =================
def build_eod_report():
    total_pnl = round(sum(x.get("pnl", 0.0) for x in eod_stats["closed"]), 2)

    lines = ["📊 END OF DAY REPORT", ""]

    lines += [
        f"Patterns Found:",
        f"Gap Up Plus: {len(pattern_summary['gapup'])}",
        f"15 Min Inside: {len(pattern_summary['inside15'])}",
        f"30 Min Weekly Pivot Sell: {len(pattern_summary['pivot30'])}",
        "",
        f"Entries Triggered: {len(eod_stats['entries'])}",
        f"Blocked by OI: {len(eod_stats['blocked'])}",
        f"Target Hit: {len(eod_stats['targets'])}",
        f"Stoploss Hit: {len(eod_stats['stoplosses'])}",
        f"Day End / Others: {len(eod_stats['dayend'])}",
        "",
        f"Total Closed Trades: {len(eod_stats['closed'])}",
        f"Net P/L: {total_pnl}",
        ""
    ]

    if eod_stats["closed"]:
        lines.append("Closed Trades:")
        for x in eod_stats["closed"]:
            sign = "+" if x["pnl"] > 0 else ""
            lines.append(
                f"{x['symbol']} | {x['strategy']} | {x['side']} | "
                f"Entry:{x['entry']} Exit:{x['exit']} | {x['reason']} | P/L:{sign}{x['pnl']}"
            )
        lines.append("")

    if eod_stats["blocked"]:
        lines.append("Blocked Entries:")
        for x in eod_stats["blocked"]:
            lines.append(f"{x['symbol']} | {x['strategy']} | {x['side']} | {x['reason']}")

    return "\n".join(lines).strip()

# ================= MAIN =================
def main():
    profile = check_auth()
    log_analysis_date_debug()
    send(
        f"🚀 BOT STARTED\n"
        f"Profile status: {profile.get('s')}\n"
        f"AFTER_MARKET_RUN={AFTER_MARKET_RUN}\n"
        f"Analysis day={analysis_date_str()}\n"
        f"WATCHLIST={WATCHLIST_RAW}\n"
        f"Risk={RISK_AMOUNT} | Leverage={LEVERAGE}X"
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

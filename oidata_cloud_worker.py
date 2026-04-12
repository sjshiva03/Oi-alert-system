
import os
import io
import json
import time
import threading
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from fyers_apiv3 import fyersModel

try:
    from fyers_apiv3.FyersWebsocket import data_ws
except Exception:
    data_ws = None

IST = timezone(timedelta(hours=5, minutes=30))

JSON_FILE = os.getenv("JSON_FILE", "npattern_selected.json")
GITHUB_JSON_URL = (os.getenv("GITHUB_JSON_URL") or "").strip()
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
ENTRY_ZONE_PERCENT = float(os.getenv("ENTRY_ZONE_PERCENT", "2.0"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "1.2"))
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").strip().lower() == "true"
MARKET_START = (9, 15)
MARKET_END = (15, 30)
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "6"))
OI_CONFIRMATION_REQUIRED = os.getenv("OI_CONFIRMATION_REQUIRED", "false").strip().lower() == "true"
TELEGRAM_SEND_MODE = os.getenv("TELEGRAM_SEND_MODE", "photo").strip().lower() or "photo"
IMAGE_WIDTH = int(os.getenv("IMAGE_WIDTH", "1600"))
IMAGE_HEIGHT = int(os.getenv("IMAGE_HEIGHT", "940"))
SHOW_STARTUP_SAMPLE = os.getenv("SHOW_STARTUP_SAMPLE", "true").strip().lower() == "true"
STARTUP_SAMPLE_SENT_FILE = os.getenv("STARTUP_SAMPLE_SENT_FILE", ".startup_sample_sent")
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", str(6 * 60 * 60)))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
FYERS_RETRY_COUNT = int(os.getenv("FYERS_RETRY_COUNT", "3"))
MANUAL_HOLIDAYS_RAW = (os.getenv("MANUAL_HOLIDAYS") or "").strip()
USE_WEBSOCKET = os.getenv("USE_WEBSOCKET", "true").strip().lower() == "true"
SEND_MULTI_CARD_SUMMARY = os.getenv("SEND_MULTI_CARD_SUMMARY", "true").strip().lower() == "true"
MULTI_CARD_SIZE = int(os.getenv("MULTI_CARD_SIZE", "6"))
TRACK_SL_TARGET = os.getenv("TRACK_SL_TARGET", "true").strip().lower() == "true"
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.0")) / 100.0
MARKET_WAIT_LOG_INTERVAL_SECONDS = int(os.getenv("MARKET_WAIT_LOG_INTERVAL_SECONDS", "900"))
SEND_JSON_READ_SAMPLE = os.getenv("SEND_JSON_READ_SAMPLE", "true").strip().lower() == "true"
JSON_READ_SAMPLE_SENT_FILE = os.getenv("JSON_READ_SAMPLE_SENT_FILE", ".json_read_sample_sent")

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

MANUAL_HOLIDAYS = {x.strip() for x in MANUAL_HOLIDAYS_RAW.split(",") if x.strip()}

LATEST_QUOTES: Dict[str, Dict[str, Any]] = {}
WS_INSTANCE = None
WS_RUNNING = False
WS_SYMBOLS: List[str] = []
LAST_MARKET_WAIT_LOG_TS = 0.0

# =========================
# Utilities
# =========================
def now_ist() -> datetime:
    return datetime.now(IST)


def log(msg: str) -> None:
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] {msg}", flush=True)


def short_symbol(symbol: str) -> str:
    s = symbol.replace("NSE:", "").replace("BSE:", "")
    for suf in ["-EQ", "-BE", "-BZ", "-BL", "-SM", "-INDEX"]:
        s = s.replace(suf, "")
    return s.strip()


def in_market_hours() -> bool:
    if not ONLY_MARKET_HOURS:
        return True
    n = now_ist()
    if n.weekday() >= 5:
        return False
    if n.strftime("%Y-%m-%d") in MANUAL_HOLIDAYS:
        return False
    start = n.replace(hour=MARKET_START[0], minute=MARKET_START[1], second=0, microsecond=0)
    end = n.replace(hour=MARKET_END[0], minute=MARKET_END[1], second=0, microsecond=0)
    return start <= n <= end


def fmt_ist(ts: Any, fmt: str = "%d-%m-%Y %H:%M IST") -> str:
    if ts is None or ts == "":
        return ""
    try:
        t = pd.to_datetime(ts)
        if t.tzinfo is None:
            t = t.tz_localize("UTC").tz_convert(IST)
        else:
            t = t.tz_convert(IST)
        return t.strftime(fmt)
    except Exception:
        return str(ts)


def compact_num(v: Any) -> str:
    try:
        n = float(v)
    except Exception:
        return "-"
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e7:
        return f"{sign}{n/1e7:.2f}Cr"
    if n >= 1e5:
        return f"{sign}{n/1e5:.2f}L"
    if n >= 1e3:
        return f"{sign}{n/1e3:.1f}K"
    return f"{sign}{n:.0f}"


def ensure_float(x: Any, default: float = 0.0) -> float:
    try:
        if x in (None, ""):
            return default
        return float(x)
    except Exception:
        return default


def market_start_dt(ref: Optional[datetime] = None) -> datetime:
    n = ref or now_ist()
    return n.replace(hour=9, minute=5, second=0, microsecond=0)


def market_end_dt(ref: Optional[datetime] = None) -> datetime:
    n = ref or now_ist()
    return n.replace(hour=MARKET_END[0], minute=MARKET_END[1], second=0, microsecond=0)


def is_market_day(ref: Optional[datetime] = None) -> bool:
    n = ref or now_ist()
    if n.weekday() >= 5:
        return False
    if n.strftime("%Y-%m-%d") in MANUAL_HOLIDAYS:
        return False
    return True


def wait_until_market_start() -> None:
    global LAST_MARKET_WAIT_LOG_TS
    while True:
        n = now_ist()
        if is_market_day(n):
            start = market_start_dt(n)
            if n >= start:
                return
            remaining = int((start - n).total_seconds())
            if time.time() - LAST_MARKET_WAIT_LOG_TS >= MARKET_WAIT_LOG_INTERVAL_SECONDS:
                log(f"Waiting for market start 09:05 IST | {max(remaining,0)//60} min left")
                LAST_MARKET_WAIT_LOG_TS = time.time()
            time.sleep(min(60, max(5, remaining)))
        else:
            tomorrow = n + timedelta(days=1)
            while not is_market_day(tomorrow):
                tomorrow += timedelta(days=1)
            next_start = market_start_dt(tomorrow)
            remaining = int((next_start - n).total_seconds())
            if time.time() - LAST_MARKET_WAIT_LOG_TS >= MARKET_WAIT_LOG_INTERVAL_SECONDS:
                log(f"Non-market day; next session at {next_start.strftime('%d-%m-%Y %H:%M IST')}")
                LAST_MARKET_WAIT_LOG_TS = time.time()
            time.sleep(min(900, max(60, remaining)))


def confidence_score(setup: Dict[str, Any], ltp: float, bias: str) -> int:
    score = 50
    d_price = ensure_float(setup.get("touched_d_price") or setup.get("fib_d") or setup.get("trend_d"))
    if d_price > 0:
        diff_pct = abs(ltp - d_price) / d_price * 100.0
        if diff_pct <= 0.25:
            score += 20
        elif diff_pct <= 0.50:
            score += 12
        elif diff_pct <= ENTRY_ZONE_PERCENT:
            score += 6
    pattern = str(setup.get("pattern","")).lower()
    b = bias.lower()
    if pattern == "bullish":
        if b == "bullish":
            score += 20
        elif "bullish" in b:
            score += 12
        elif b == "neutral":
            score += 4
    else:
        if b == "bearish":
            score += 20
        elif "bearish" in b:
            score += 12
        elif b == "neutral":
            score += 4
    if str(setup.get("touched_d_source","")).lower().startswith("fib"):
        score += 5
    return max(1, min(99, int(score)))



def _ws_symbol_payload(symbols: List[str]) -> List[str]:
    return [s for s in symbols if s]


def _ws_onmessage(message: Any) -> None:
    try:
        msg = message if isinstance(message, dict) else {}
        symbol = msg.get("symbol") or msg.get("n") or msg.get("s") or msg.get("fyToken")
        ltp = msg.get("ltp") or msg.get("lp") or msg.get("last_price") or msg.get("v", {}).get("lp") if isinstance(msg.get("v"), dict) else msg.get("ltp")
        if symbol and ltp is not None:
            LATEST_QUOTES[str(symbol)] = {"ltp": float(ltp), "ts": now_ist().strftime("%Y-%m-%d %H:%M:%S")}
    except Exception:
        pass


def _ws_onerror(message: Any) -> None:
    log(f"WebSocket error: {message}")


def _ws_onclose(message: Any) -> None:
    global WS_RUNNING
    WS_RUNNING = False
    log(f"WebSocket closed: {message}")


def _ws_onopen() -> None:
    log(f"WebSocket opened for {len(WS_SYMBOLS)} symbols")


def ensure_websocket(symbols: List[str]) -> None:
    global WS_INSTANCE, WS_RUNNING, WS_SYMBOLS
    if not USE_WEBSOCKET or data_ws is None:
        return
    symbols = sorted(set(_ws_symbol_payload(symbols)))
    if not symbols:
        return
    if WS_RUNNING and symbols == WS_SYMBOLS:
        return
    try:
        if WS_INSTANCE is not None:
            try:
                WS_INSTANCE.close_connection()
            except Exception:
                pass
    except Exception:
        pass
    WS_SYMBOLS = symbols
    try:
        access_token = f"{CLIENT_ID}:{ACCESS_TOKEN}"
        ws = data_ws.FyersDataSocket(
            access_token=access_token,
            log_path="",
            litemode=True,
            write_to_file=False,
            reconnect=True,
            on_connect=_ws_onopen,
            on_close=_ws_onclose,
            on_error=_ws_onerror,
            on_message=_ws_onmessage,
        )
        WS_INSTANCE = ws
        def _runner():
            global WS_RUNNING
            try:
                WS_RUNNING = True
                ws.connect()
                try:
                    ws.subscribe(symbols=WS_SYMBOLS, data_type="SymbolUpdate")
                    ws.keep_running()
                except Exception as e:
                    log(f"WebSocket subscribe/run error: {e}")
                    WS_RUNNING = False
            except Exception as e:
                log(f"WebSocket start failed: {e}")
                WS_RUNNING = False
        threading.Thread(target=_runner, daemon=True).start()
        log(f"WebSocket initialization requested for {len(symbols)} symbols")
    except Exception as e:
        log(f"WebSocket init failed: {e}")
        WS_RUNNING = False

# =========================
# Fyers
# =========================
def create_fyers() -> fyersModel.FyersModel:
    if not CLIENT_ID or not ACCESS_TOKEN:
        raise RuntimeError("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
    return fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        is_async=False,
        log_path="",
    )


FYERS = create_fyers()


def call_with_retry(fn, payload: Dict[str, Any], label: str) -> Optional[Dict[str, Any]]:
    last_error = None
    for attempt in range(1, FYERS_RETRY_COUNT + 1):
        try:
            try:
                resp = fn(data=payload)
            except TypeError:
                resp = fn(payload)
            if isinstance(resp, dict):
                return resp
            last_error = f"unexpected response type {type(resp)}"
        except Exception as e:
            last_error = str(e)
            log(f"{label} retry {attempt}/{FYERS_RETRY_COUNT} failed: {e}")
            time.sleep(min(2.0 * attempt, 5.0))
    log(f"{label} failed after retries: {last_error}")
    return None


def fetch_history(symbol: str, resolution: str = "15", days: int = 5) -> Optional[pd.DataFrame]:
    to_date = now_ist().date()
    from_date = to_date - timedelta(days=days)
    payload = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": from_date.strftime("%Y-%m-%d"),
        "range_to": to_date.strftime("%Y-%m-%d"),
        "cont_flag": "1",
    }
    resp = call_with_retry(FYERS.history, payload, f"History {symbol}")
    if not resp:
        return None
    candles = resp.get("candles", [])
    if not candles:
        return None
    df = pd.DataFrame(candles, columns=["ts", "o", "h", "l", "c", "v"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert(IST)
    return df


def get_ltp(symbol: str) -> Optional[float]:
    cached = LATEST_QUOTES.get(symbol)
    if cached and cached.get("ltp") is not None:
        return ensure_float(cached.get("ltp"), None)
    payload = {"symbols": symbol}
    resp = call_with_retry(FYERS.quotes, payload, f"Quote {symbol}")
    if not resp:
        return None
    try:
        for item in resp.get("d", []):
            values = item.get("v", {})
            ltp = values.get("lp") or values.get("ltp") or values.get("last_price")
            if ltp is not None:
                ltpf = float(ltp)
                LATEST_QUOTES[symbol] = {"ltp": ltpf, "ts": now_ist().strftime("%Y-%m-%d %H:%M:%S")}
                return ltpf
    except Exception as e:
        log(f"Quote parse error for {symbol}: {e}")
    return None


def fetch_option_chain_snapshot(symbol: str, strikecount: int = STRIKECOUNT) -> Dict[str, Any]:
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": ""}
    resp = call_with_retry(FYERS.optionchain, payload, f"OptionChain {symbol}")
    if not resp:
        return {"ok": False, "reason": "optionchain unavailable", "rows": [], "bias": "NA", "underlying_ltp": None}

    raw = resp.get("data") if isinstance(resp.get("data"), dict) else resp
    chain = []
    if isinstance(raw, dict):
        chain = raw.get("optionsChain") or raw.get("chain") or raw.get("options_chain") or raw.get("d") or []
        underlying_ltp = raw.get("ltp") or raw.get("underlying_ltp") or raw.get("underlyingPrice")
    else:
        underlying_ltp = None

    rows_by_strike: Dict[float, Dict[str, Any]] = {}
    for item in chain or []:
        try:
            strike = float(item.get("strike_price") or item.get("strike") or 0)
        except Exception:
            continue
        if strike <= 0:
            continue
        side = str(item.get("option_type") or "").upper()
        sym = str(item.get("symbol") or "")
        if side not in {"CE", "PE"}:
            if sym.endswith("CE"):
                side = "CE"
            elif sym.endswith("PE"):
                side = "PE"
            else:
                continue
        row = rows_by_strike.setdefault(strike, {"strike": strike})
        row[f"{side.lower()}_ltp"] = ensure_float(item.get("ltp"))
        row[f"{side.lower()}_oi"] = ensure_float(item.get("oi"))
        row[f"{side.lower()}_oich"] = ensure_float(item.get("oich") or item.get("oi_change"))
        row[f"{side.lower()}_vol"] = ensure_float(item.get("volume"))

    rows = sorted(rows_by_strike.values(), key=lambda x: x["strike"])
    if not rows:
        return {"ok": False, "reason": "empty option chain", "rows": [], "bias": "NA", "underlying_ltp": underlying_ltp}

    if underlying_ltp is None:
        underlying_ltp = get_ltp(symbol)
    underlying_ltp = ensure_float(underlying_ltp, 0.0) or None
    atm_index = 0
    if underlying_ltp is not None:
        atm_index = min(range(len(rows)), key=lambda i: abs(rows[i]["strike"] - underlying_ltp))

    start = max(0, atm_index - 5)
    end = min(len(rows), atm_index + 6)
    view_rows = rows[start:end]
    bias = derive_oi_bias(view_rows, underlying_ltp)
    return {
        "ok": True,
        "reason": "ok",
        "rows": view_rows,
        "bias": bias,
        "underlying_ltp": underlying_ltp,
    }


def derive_oi_bias(rows: List[Dict[str, Any]], underlying_ltp: Optional[float]) -> str:
    if not rows:
        return "NA"
    if underlying_ltp is None:
        underlying_ltp = rows[len(rows) // 2]["strike"]
    atm = min(rows, key=lambda r: abs(r["strike"] - underlying_ltp))
    ce = ensure_float(atm.get("ce_oich"))
    pe = ensure_float(atm.get("pe_oich"))
    if pe > 0 and ce <= 0:
        return "Bullish"
    if ce > 0 and pe <= 0:
        return "Bearish"
    if pe > ce:
        return "Bullish Weak"
    if ce > pe:
        return "Bearish Weak"
    return "Neutral"

# =========================
# HA entry logic
# =========================
def to_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    ha = df.copy().reset_index(drop=True)
    ha["ha_close"] = (ha["o"] + ha["h"] + ha["l"] + ha["c"]) / 4.0
    ha["ha_open"] = 0.0
    ha.loc[0, "ha_open"] = (ha.loc[0, "o"] + ha.loc[0, "c"]) / 2.0
    for i in range(1, len(ha)):
        ha.loc[i, "ha_open"] = (ha.loc[i - 1, "ha_open"] + ha.loc[i - 1, "ha_close"]) / 2.0
    ha["ha_high"] = ha[["h", "ha_open", "ha_close"]].max(axis=1)
    ha["ha_low"] = ha[["l", "ha_open", "ha_close"]].min(axis=1)
    return ha


def is_doji(row: pd.Series, max_body_ratio: float = 0.20) -> bool:
    rng = float(row["ha_high"] - row["ha_low"])
    if rng <= 0:
        return False
    body = abs(float(row["ha_close"] - row["ha_open"]))
    return (body / rng) <= max_body_ratio


def check_entry_after_touch(df: pd.DataFrame, d_price: float, pattern: str, max_entry_percent: float) -> Tuple[Optional[float], Optional[datetime], Dict[str, Any]]:
    ha = to_heikin_ashi(df)
    allowed = abs(d_price) * (max_entry_percent / 100.0)
    debug: Dict[str, Any] = {}

    for i in range(len(ha) - 1):
        first = ha.iloc[i]
        second = ha.iloc[i + 1]
        if pattern == "Bullish":
            nearest_scan_price = min(float(first["ha_open"]), float(first["ha_close"]), float(first["ha_low"]))
            entry_ref_price = min(float(second["ha_open"]), float(second["ha_close"]), float(second["ha_low"]))
        else:
            nearest_scan_price = max(float(first["ha_open"]), float(first["ha_close"]), float(first["ha_high"]))
            entry_ref_price = max(float(second["ha_open"]), float(second["ha_close"]), float(second["ha_high"]))

        if abs(nearest_scan_price - d_price) > allowed:
            debug["reason"] = f"Moved beyond {max_entry_percent}% from D"
            break

        if not is_doji(first):
            continue

        debug.update({
            "doji_time": first["datetime"],
            "next_time": second["datetime"],
        })

        if pattern == "Bullish":
            confirm_ok = abs(float(second["ha_open"]) - float(second["ha_low"])) <= 0.2
            if confirm_ok:
                entry_price = float(second["ha_high"])
                if abs(entry_ref_price - d_price) <= allowed:
                    return entry_price, pd.to_datetime(second["datetime"]).to_pydatetime(), debug
        else:
            confirm_ok = abs(float(second["ha_open"]) - float(second["ha_high"])) <= 0.2
            if confirm_ok:
                entry_price = float(second["ha_low"])
                if abs(entry_ref_price - d_price) <= allowed:
                    return entry_price, pd.to_datetime(second["datetime"]).to_pydatetime(), debug

    return None, None, debug

# =========================
# Telegram
# =========================
def send_telegram_text(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram credentials missing; skipping text alert")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        log(f"Telegram text error: {e}")


def send_telegram_photo(image_bytes: bytes, caption: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram credentials missing; skipping image alert")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        files = {"photo": ("npattern_entry.png", image_bytes, "image/png")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
        r = requests.post(url, data=data, files=files, timeout=max(HTTP_TIMEOUT, 30))
        r.raise_for_status()
    except Exception as e:
        log(f"Telegram photo error: {e}")
        send_telegram_text(caption)

# =========================
# Dashboard image
# =========================
def _load_font(size: int, bold: bool = False):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    fonts_dir = os.path.join(base_dir, "fonts")
    custom_paths = []
    if bold:
        custom_paths = [os.path.join(fonts_dir, "DejaVuSans-Bold.ttf")]
    else:
        custom_paths = [os.path.join(fonts_dir, "DejaVuSans.ttf")]

    for p in custom_paths:
        try:
            if os.path.exists(p):
                return ImageFont.truetype(p, size)
        except Exception as e:
            log(f"Custom font load failed {p}: {e}")

    fallback = []
    if bold:
        fallback += [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        ]
    else:
        fallback += [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ]
    for p in fallback:
        try:
            if os.path.exists(p):
                return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()


def draw_cell(draw: ImageDraw.ImageDraw, x1: int, y1: int, x2: int, y2: int, text: str, font, fill=(255,255,255), outline=(210,210,210), text_fill=(0,0,0), align="center"):
    draw.rectangle((x1, y1, x2, y2), fill=fill, outline=outline, width=1)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    if align == "left":
        tx = x1 + 12
    else:
        tx = x1 + (x2 - x1 - tw) / 2
    ty = y1 + (y2 - y1 - th) / 2 - 1
    draw.text((tx, ty), text, font=font, fill=text_fill)


def build_dashboard_image(setup: Dict[str, Any], entry_price: float, entry_time: datetime, oisnap: Dict[str, Any], startup_preview: bool = False) -> bytes:
    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (245, 245, 245))
    draw = ImageDraw.Draw(img)

    black = (18, 18, 18)
    green = (28, 148, 82)
    red = (198, 58, 58)
    blue = (48, 97, 184)
    white = (255, 255, 255)
    soft = (255, 255, 255)
    border = (210, 210, 210)
    table_head_fill = (236, 240, 247)

    f_title = _load_font(50, True)
    f_header = _load_font(30, True)
    f_label = _load_font(26, True)
    f_value = _load_font(30, False)
    f_table_h = _load_font(30, True)
    f_table_b = _load_font(30, False)
    f_stamp = _load_font(30, False)
    f_footer = _load_font(30, False)

    # Header
    draw.rounded_rectangle((28, 22, IMAGE_WIDTH - 28, 105), radius=24, fill=white, outline=border, width=2)
    draw.text((58, 45), "N PATTERN ENTRY ALERT", font=f_title, fill=blue)
    stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
    stamp_bbox = draw.textbbox((0, 0), stamp, font=f_stamp)
    draw.text((IMAGE_WIDTH - 55 - (stamp_bbox[2] - stamp_bbox[0]), 48), stamp, font=f_stamp, fill=black)

    # Info panel, 5 left + 5 right
    box_top = 132
    box_bottom = 420
    draw.rounded_rectangle((28, box_top, IMAGE_WIDTH - 28, box_bottom), radius=24, fill=soft, outline=border, width=2)

    touched_d = ensure_float(setup.get("touched_d_price") or setup.get("fib_d"))
    active_d = ensure_float(setup.get("active_d"))
    ltp_alert = ensure_float(setup.get("ltp_at_alert") or setup.get("ltp"))
    pattern = str(setup.get("pattern", ""))
    target_px = entry_price * 1.02 if pattern.lower() == "bullish" else entry_price * 0.98
    entry_time_str = entry_time.astimezone(IST).strftime("%d-%m-%Y %H:%M")
    oi_bias = str(oisnap.get("bias", "NA"))
    d_source = str(setup.get("touched_d_source") or "")

    left_items = [
        ("Stock", short_symbol(setup.get("symbol", "")), black),
        ("Pattern", pattern, black),
        ("D Source", d_source, black),
        ("Touched D", f"{touched_d:.2f}", black),
        ("Entry", f"{entry_price:.2f}", green),
    ]
    right_items = [
        ("Active D / Stop Basis", f"{active_d:.2f}", black),
        ("Target (+2%)", f"{target_px:.2f}", green),
        ("Entry Time", entry_time_str, black),
        ("OI Bias", oi_bias, green if "Bullish" in oi_bias else red if "Bearish" in oi_bias else black),
        ("LTP", f"{ltp_alert:.2f}" if ltp_alert else "-", black),
    ]

    y0 = box_top + 26
    row_gap = 50
    left_label_x, left_value_x = 55, 275
    right_label_x, right_value_x = 820, 1140
    for i, (lab, val, col) in enumerate(left_items):
        y = y0 + i * row_gap
        draw.text((left_label_x, y), f"{lab}:", font=f_label, fill=black)
        draw.text((left_value_x, y), val, font=f_value, fill=col)
    for i, (lab, val, col) in enumerate(right_items):
        y = y0 + i * row_gap
        draw.text((right_label_x, y), f"{lab}:", font=f_label, fill=black)
        draw.text((right_value_x, y), val, font=f_value, fill=col)

    # OI section
    oi_top = 432
    oi_bottom = IMAGE_HEIGHT - 38
    draw.rounded_rectangle((28, oi_top, IMAGE_WIDTH - 28, oi_bottom), radius=24, fill=white, outline=border, width=2)
    draw.text((55, oi_top + 18), "OI SNAPSHOT", font=f_header, fill=black)

    rows = oisnap.get("rows", [])[:11]
    tx1 = 52
    ty1 = oi_top + 62
    table_width = IMAGE_WIDTH - 104
    row_h = 72
    headers = ["Strike", "CE", "CE OI Chg", "PE", "PE OI Chg", "CE Vol", "PE Vol"]
    widths = [170, 170, 240, 170, 240, 180, 180]
    xs = [tx1]
    for w in widths:
        xs.append(xs[-1] + w)

    # Header row
    for j, h in enumerate(headers):
        draw_cell(draw, xs[j], ty1, xs[j+1], ty1 + row_h, h, f_table_h, fill=table_head_fill, outline=border, text_fill=blue)

    # Data rows
    base_y = ty1 + row_h
    for idx, r in enumerate(rows):
        y1 = base_y + idx * row_h
        y2 = y1 + row_h
        ce_oich = ensure_float(r.get("ce_oich"))
        pe_oich = ensure_float(r.get("pe_oich"))
        ce_arrow = "▲" if ce_oich > 0 else "▼" if ce_oich < 0 else "•"
        pe_arrow = "▲" if pe_oich > 0 else "▼" if pe_oich < 0 else "•"
        vals = [
            f"{ensure_float(r.get('strike')):.0f}",
            f"{ensure_float(r.get('ce_ltp')):.2f}",
            f"{ce_arrow} {compact_num(abs(ce_oich))}",
            f"{ensure_float(r.get('pe_ltp')):.2f}",
            f"{pe_arrow} {compact_num(abs(pe_oich))}",
            compact_num(r.get("ce_vol")),
            compact_num(r.get("pe_vol")),
        ]
        colors = [black, black, green if ce_oich > 0 else red if ce_oich < 0 else black, black, green if pe_oich > 0 else red if pe_oich < 0 else black, black, black]
        fill = (252, 252, 252) if idx % 2 == 0 else white
        for j, v in enumerate(vals):
            draw_cell(draw, xs[j], y1, xs[j+1], y2, v, f_table_b, fill=fill, outline=border, text_fill=colors[j])

    if startup_preview:
        footer = "This is only a startup preview image."
        bbox = draw.textbbox((0, 0), footer, font=f_footer)
        draw.text(((IMAGE_WIDTH - (bbox[2]-bbox[0]))/2, IMAGE_HEIGHT - 24), footer, font=f_footer, fill=(100,100,100))

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()



def build_multi_card_dashboard(alert_items: List[Dict[str, Any]]) -> bytes:
    card_cols = 2
    card_rows = 3
    width = max(IMAGE_WIDTH, 1700)
    height = max(IMAGE_HEIGHT, 1500)
    img = Image.new("RGB", (width, height), (245, 245, 245))
    draw = ImageDraw.Draw(img)
    black = (18,18,18)
    blue = (48,97,184)
    green = (28,148,82)
    red = (198,58,58)
    white = (255,255,255)
    border = (210,210,210)
    title_f = _load_font(44, True)
    head_f = _load_font(26, True)
    body_f = _load_font(22, False)
    small_f = _load_font(20, False)
    draw.rounded_rectangle((24,18,width-24,92), radius=22, fill=white, outline=border, width=2)
    draw.text((48,38), "N PATTERN LIVE ALERTS", font=title_f, fill=blue)
    stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
    sb = draw.textbbox((0,0), stamp, font=small_f)
    draw.text((width-48-(sb[2]-sb[0]),40), stamp, font=small_f, fill=black)
    margin_x, margin_y = 28, 120
    gap_x, gap_y = 26, 24
    card_w = (width - 2*margin_x - gap_x) // 2
    card_h = (height - margin_y - 36 - 2*gap_y) // 3
    for idx, item in enumerate(alert_items[:card_cols*card_rows]):
        row = idx // 2
        col = idx % 2
        x1 = margin_x + col * (card_w + gap_x)
        y1 = margin_y + row * (card_h + gap_y)
        x2 = x1 + card_w
        y2 = y1 + card_h
        draw.rounded_rectangle((x1, y1, x2, y2), radius=22, fill=white, outline=border, width=2)
        draw.text((x1+24, y1+20), short_symbol(item.get("symbol","")), font=head_f, fill=blue)
        y = y1 + 64
        lines = [
            ("Pattern", str(item.get("pattern","")), black),
            ("D", f"{item.get('d_source','')} {ensure_float(item.get('d_price')):.2f}", black),
            ("Entry", f"{ensure_float(item.get('entry_price')):.2f}", green),
            ("SL", f"{ensure_float(item.get('sl_price')):.2f}", red),
            ("Target", f"{ensure_float(item.get('target_price')):.2f}", green),
            ("OI", str(item.get("oi_bias","NA")), green if "Bullish" in str(item.get("oi_bias","")) else red if "Bearish" in str(item.get("oi_bias","")) else black),
            ("Confidence", f"{int(item.get('confidence_score',0))}%", blue),
        ]
        for lab, val, colr in lines:
            draw.text((x1+24, y), f"{lab}:", font=body_f, fill=black)
            draw.text((x1+200, y), val, font=body_f, fill=colr)
            y += 34
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


def send_multi_card_summary(alert_items: List[Dict[str, Any]]) -> None:
    if not alert_items:
        return
    caption = f"🔥 N PATTERN LIVE ALERTS | {len(alert_items)} setup(s)"
    image_bytes = build_multi_card_dashboard(alert_items)
    send_telegram_photo(image_bytes, caption)

# =========================
# State and setup loading
# =========================
def load_setups() -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    if GITHUB_JSON_URL:
        try:
            r = requests.get(GITHUB_JSON_URL, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            log(f"Loaded {len(data)} setups from GitHub JSON")
            return data
        except Exception as e:
            log(f"GitHub JSON refresh failed: {e}; falling back to local file")

    if not os.path.exists(JSON_FILE):
        log(f"JSON file not found: {JSON_FILE}")
        return []
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    log(f"Loaded {len(data)} setups from local JSON")
    return data


def save_setups(items: List[Dict[str, Any]]) -> None:
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


def _state_key(item: Dict[str, Any]) -> Tuple[str, str, str, str]:
    pattern_id = str(item.get("pattern_id") or "").strip()
    if pattern_id:
        return ("pattern_id", pattern_id, "", "")
    return (
        str(item.get("symbol", "") or ""),
        str(item.get("pattern", "") or ""),
        str(item.get("a_time", "") or ""),
        str(item.get("c_time", "") or ""),
    )


def merge_state(old_items: List[Dict[str, Any]], new_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    state_by_key: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for item in old_items:
        state_by_key[_state_key(item)] = item

    preserve_fields = [
        "alert_sent", "entry_found", "entry_price", "entry_time", "status",
        "ltp_at_alert", "oi_bias", "touched_d_source", "touched_d_price",
        "last_alert_at", "cooldown_seconds", "last_checked_at", "last_debug_reason",
        "sl_price", "target_price", "confidence_score", "target_hit", "sl_hit",
        "pattern_id", "a_price", "b_price", "c_price", "a_time", "b_time", "c_time",
        "active_d", "which_extension_active", "ext_path", "d_hit", "entry_reason",
        "target_status", "pattern_active", "final_ext_value", "projection", "ac_ratio",
    ]
    completed_statuses = {"target_hit", "sl_hit", "completed_without_entry"}

    merged: List[Dict[str, Any]] = []
    for item in new_items:
        key = _state_key(item)
        prev = state_by_key.get(key, {})
        combined = dict(item)

        # Always preserve previous completed trade state and live runtime values when available.
        prev_status = str(prev.get("status") or "").strip().lower()
        if prev_status in completed_statuses:
            combined.update(prev)
            merged.append(combined)
            continue

        for k in preserve_fields:
            if k not in prev:
                continue
            new_val = combined.get(k)
            old_val = prev.get(k)

            # runtime/live values should survive GitHub refreshes unless the new export explicitly has a real value
            if k in {"last_checked_at", "last_debug_reason", "last_alert_at", "oi_bias", "ltp_at_alert", "confidence_score"}:
                if not new_val:
                    combined[k] = old_val
                continue

            if k in {"alert_sent", "entry_found", "target_hit", "sl_hit"}:
                if not bool(new_val) and bool(old_val):
                    combined[k] = old_val
                continue

            if k in {"entry_price", "entry_time", "sl_price", "target_price", "touched_d_price", "touched_d_source"}:
                if new_val in [None, "", 0, 0.0, False] and old_val not in [None, "", 0, 0.0, False]:
                    combined[k] = old_val
                continue

            if k == "status":
                if str(new_val or "").strip().lower() in {"", "waiting"} and str(old_val or "").strip():
                    combined[k] = old_val
                continue

            if new_val in [None, "", False] and old_val not in [None, "", False]:
                combined[k] = old_val

        merged.append(combined)
    return merged

def send_json_read_sample(setups: List[Dict[str, Any]]) -> None:
    if not SEND_JSON_READ_SAMPLE:
        return
    if os.path.exists(JSON_READ_SAMPLE_SENT_FILE):
        return
    if not setups:
        return
    try:
        lines = [f"JSON READ TEST | Total setups: {len(setups)}"]
        for i, s in enumerate(setups[:5], start=1):
            lines.append(
                f"{i}. {short_symbol(str(s.get('symbol') or ''))} | "
                f"status={str(s.get('status') or 'waiting')} | "
                f"active_d={ensure_float(s.get('active_d')):.2f} | "
                f"entry_found={bool(s.get('entry_found'))} | "
                f"alert_sent={bool(s.get('alert_sent'))}"
            )
        send_telegram_text("\n".join(lines))
        with open(JSON_READ_SAMPLE_SENT_FILE, 'w', encoding='utf-8') as f:
            f.write(now_ist().strftime('%Y-%m-%d %H:%M:%S'))
        log('JSON read sample sent once')
    except Exception as e:
        log(f'JSON read sample failed: {e}')

# =========================
# Startup sample
# =========================
def maybe_send_startup_sample() -> None:
    if not SHOW_STARTUP_SAMPLE:
        return
    if os.path.exists(STARTUP_SAMPLE_SENT_FILE):
        return
    try:
        sample_setup = {
            "symbol": "NSE:HDFCLIFE-EQ",
            "pattern": "Bullish",
            "fib_d": 558.40,
            "active_d": 552.10,
            "touched_d_source": "Fib",
            "touched_d_price": 558.40,
            "ltp_at_alert": 560.25,
        }
        sample_oi = {
            "bias": "Bullish",
            "rows": [
                {"strike": 550, "ce_ltp": 14.2, "ce_oich": -1200, "pe_ltp": 4.1, "pe_oich": 1800, "ce_vol": 4200, "pe_vol": 6300},
                {"strike": 555, "ce_ltp": 10.8, "ce_oich": -900,  "pe_ltp": 5.6, "pe_oich": 2200, "ce_vol": 5100, "pe_vol": 7400},
                {"strike": 560, "ce_ltp": 7.4,  "ce_oich": -450,  "pe_ltp": 7.9, "pe_oich": 2600, "ce_vol": 6800, "pe_vol": 8900},
                {"strike": 565, "ce_ltp": 4.8,  "ce_oich": 200,   "pe_ltp": 11.7,"pe_oich": 1500, "ce_vol": 3500, "pe_vol": 5600},
                {"strike": 570, "ce_ltp": 3.1,  "ce_oich": 600,   "pe_ltp": 16.4,"pe_oich": 900,  "ce_vol": 2100, "pe_vol": 3900},
            ],
        }
        entry_time = now_ist()
        image_bytes = build_dashboard_image(sample_setup, 560.25, entry_time, sample_oi, startup_preview=True)
        caption = "Startup preview | N Pattern live monitor"
        if TELEGRAM_SEND_MODE == "text":
            send_telegram_text(caption)
        else:
            send_telegram_photo(image_bytes, caption)
        with open(STARTUP_SAMPLE_SENT_FILE, "w", encoding="utf-8") as f:
            f.write(now_ist().strftime("%Y-%m-%d %H:%M:%S"))
        log("Startup preview sent once")
    except Exception as e:
        log(f"Startup sample failed: {e}")

# =========================
# Main monitor
# =========================
def is_completed_without_entry(setup: Dict[str, Any]) -> bool:
    status = str(setup.get("status") or "").strip().lower()
    target_status = str(setup.get("target_status") or "").strip().lower()
    pattern_active = str(setup.get("pattern_active") or "").strip().lower()
    return (
        status == "completed_without_entry"
        or "completed without entry" in target_status
        or (status == "touched" and pattern_active == "no" and not setup.get("entry_found"))
    )


def should_process_setup(setup: Dict[str, Any]) -> bool:
    status = str(setup.get("status") or "waiting").strip().lower()
    if status in {"target_hit", "sl_hit", "completed_without_entry"}:
        return False
    if is_completed_without_entry(setup):
        setup["status"] = "completed_without_entry"
        return False
    # Entry already exists: handled only by trade tracker
    if bool(setup.get("entry_found")) or status in {"entry_found", "open"}:
        return False
    return True


def detect_touch(setup: Dict[str, Any], ltp: float) -> Tuple[Optional[float], str]:
    active_d = ensure_float(setup.get("active_d"))
    fib_d = ensure_float(setup.get("fib_d"))
    trend_d = ensure_float(setup.get("trend_d"))

    if active_d:
        allowed_active = abs(active_d) * (ENTRY_ZONE_PERCENT / 100.0)
        if abs(ltp - active_d) <= allowed_active:
            label = str(setup.get("which_extension_active") or "Active D")
            return active_d, label

    allowed_fib = abs(fib_d) * (ENTRY_ZONE_PERCENT / 100.0) if fib_d else 0
    allowed_trend = abs(trend_d) * (ENTRY_ZONE_PERCENT / 100.0) if trend_d else 0

    if fib_d and abs(ltp - fib_d) <= allowed_fib:
        return fib_d, "Fib"
    if trend_d and abs(ltp - trend_d) <= allowed_trend:
        return trend_d, "Trend"
    return None, ""


def oi_allows_entry(pattern: str, bias: str) -> bool:
    if not OI_CONFIRMATION_REQUIRED:
        return True
    p = pattern.lower()
    b = bias.lower()
    if p == "bullish":
        return "bullish" in b or b == "neutral"
    return "bearish" in b or b == "neutral"


def in_cooldown(setup: Dict[str, Any]) -> bool:
    last_alert_at = str(setup.get("last_alert_at") or "").strip()
    if not last_alert_at:
        return False
    try:
        dt = pd.to_datetime(last_alert_at)
        if dt.tzinfo is None:
            dt = dt.tz_localize(IST)
        else:
            dt = dt.tz_convert(IST)
        seconds = (now_ist() - dt.to_pydatetime()).total_seconds()
        return seconds < ALERT_COOLDOWN_SECONDS
    except Exception:
        return False


def process_setup(setup: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    symbol = str(setup.get("symbol") or "")
    if not symbol:
        return setup, None

    setup["last_checked_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    if not should_process_setup(setup):
        setup["last_debug_reason"] = setup.get("last_debug_reason") or f"Skipped due to status={setup.get('status','')}"
        return setup, None
    if setup.get("alert_sent") and in_cooldown(setup):
        setup["last_debug_reason"] = "Cooldown active"
        return setup, None

    ltp = get_ltp(symbol)
    if ltp is None:
        setup["last_debug_reason"] = "LTP missing"
        log(f"LTP missing for {symbol}")
        return setup, None

    d_price, source = detect_touch(setup, ltp)
    if d_price is None:
        setup["last_debug_reason"] = f"No D touch | LTP={ltp}"
        log(f"{symbol}: no D touch | LTP={ltp}")
        return setup, None

    log(f"{symbol}: touched {source} D @ {d_price} | LTP={ltp}")

    oisnap = fetch_option_chain_snapshot(symbol)
    bias = str(oisnap.get("bias", "NA"))
    log(f"{symbol}: OI bias={bias}")
    if not oi_allows_entry(str(setup.get("pattern", "")), bias):
        log(f"{symbol}: OI rejected entry")
        setup["oi_bias"] = bias
        setup["last_debug_reason"] = "OI rejected entry"
        return setup, None

    df = fetch_history(symbol, resolution=str(setup.get("entry_tf") or "15"), days=5)
    if df is None or df.empty:
        log(f"{symbol}: lower timeframe history unavailable")
        setup["last_debug_reason"] = "Lower timeframe history unavailable"
        return setup, None

    entry_price, entry_time, debug = check_entry_after_touch(df, d_price, str(setup.get("pattern", "Bullish")), ENTRY_ZONE_PERCENT)
    setup["oi_bias"] = bias
    setup["last_debug_reason"] = debug.get("reason", "")

    if not entry_price or not entry_time:
        setup["status"] = "touched"
        setup["touched_d_source"] = source
        setup["touched_d_price"] = round(float(d_price), 2)
        setup["last_debug_reason"] = debug.get("reason", "No entry found after touch")
        log(f"{symbol}: no entry found")
        return setup, None

    setup["alert_sent"] = True
    setup["entry_found"] = True
    setup["entry_price"] = round(float(entry_price), 2)
    setup["entry_time"] = entry_time.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
    setup["status"] = "entry_found"
    setup["ltp_at_alert"] = round(float(ltp), 2)
    setup["oi_bias"] = bias
    setup["touched_d_source"] = source
    setup["touched_d_price"] = round(float(d_price), 2)
    setup["last_alert_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    setup["cooldown_seconds"] = ALERT_COOLDOWN_SECONDS
    setup["confidence_score"] = confidence_score(setup, float(ltp), bias)
    if str(setup.get("pattern","")).lower() == "bullish":
        sl_price = ensure_float(setup.get("active_d")) * (1.0 - SL_BUFFER_PCT)
        target_price = float(entry_price) * 1.02
    else:
        sl_price = ensure_float(setup.get("active_d")) * (1.0 + SL_BUFFER_PCT)
        target_price = float(entry_price) * 0.98
    setup["sl_price"] = round(sl_price, 2)
    setup["target_price"] = round(target_price, 2)
    setup["target_hit"] = False
    setup["sl_hit"] = False

    caption = (
        f"🔥 N PATTERN ENTRY\n"
        f"Stock: {short_symbol(symbol)}\n"
        f"Pattern: {setup.get('pattern', '')}\n"
        f"D: {source} ({d_price:.2f})\n"
        f"Entry: {entry_price:.2f}\n"
        f"SL: {sl_price:.2f}\n"
        f"Target: {target_price:.2f}\n"
        f"Confidence: {setup['confidence_score']}%\n"
        f"Time: {entry_time.astimezone(IST).strftime('%d-%m-%Y %H:%M IST')}\n"
        f"OI Bias: {bias}"
    )

    alert_item = {
        "symbol": symbol,
        "pattern": str(setup.get("pattern", "")),
        "d_source": source,
        "d_price": round(float(d_price), 2),
        "entry_price": round(float(entry_price), 2),
        "sl_price": round(sl_price, 2),
        "target_price": round(target_price, 2),
        "confidence_score": int(setup["confidence_score"]),
        "oi_bias": bias,
    }

    if TELEGRAM_SEND_MODE == "text":
        send_telegram_text(caption)
    else:
        image_bytes = build_dashboard_image(setup, float(entry_price), entry_time, oisnap)
        send_telegram_photo(image_bytes, caption)

    log(f"{symbol}: alert sent")
    return setup, alert_item


def track_open_positions(setups: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not TRACK_SL_TARGET:
        return setups
    updated = []
    for setup in setups:
        try:
            if is_completed_without_entry(setup):
                setup["status"] = "completed_without_entry"
                updated.append(setup)
                continue
            if not setup.get("entry_found") or str(setup.get("status","")) not in {"entry_found","open"}:
                updated.append(setup)
                continue
            if setup.get("target_hit") or setup.get("sl_hit"):
                updated.append(setup)
                continue
            symbol = str(setup.get("symbol") or "")
            ltp = get_ltp(symbol)
            if ltp is None:
                updated.append(setup)
                continue
            pattern = str(setup.get("pattern","")).lower()
            sl_price = ensure_float(setup.get("sl_price"))
            target_price = ensure_float(setup.get("target_price"))
            hit_text = None
            if pattern == "bullish":
                if ltp >= target_price > 0:
                    setup["target_hit"] = True
                    setup["status"] = "target_hit"
                    hit_text = f"✅ TARGET HIT\nStock: {short_symbol(symbol)}\nLTP: {ltp:.2f}\nTarget: {target_price:.2f}"
                elif ltp <= sl_price and sl_price > 0:
                    setup["sl_hit"] = True
                    setup["status"] = "sl_hit"
                    hit_text = f"❌ STOPLOSS HIT\nStock: {short_symbol(symbol)}\nLTP: {ltp:.2f}\nSL: {sl_price:.2f}"
            else:
                if ltp <= target_price and target_price > 0:
                    setup["target_hit"] = True
                    setup["status"] = "target_hit"
                    hit_text = f"✅ TARGET HIT\nStock: {short_symbol(symbol)}\nLTP: {ltp:.2f}\nTarget: {target_price:.2f}"
                elif ltp >= sl_price > 0:
                    setup["sl_hit"] = True
                    setup["status"] = "sl_hit"
                    hit_text = f"❌ STOPLOSS HIT\nStock: {short_symbol(symbol)}\nLTP: {ltp:.2f}\nSL: {sl_price:.2f}"
            if hit_text:
                send_telegram_text(hit_text)
                setup["last_alert_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
                log(f"{symbol}: {setup['status']}")
            updated.append(setup)
            time.sleep(RATE_LIMIT_SLEEP)
        except Exception as e:
            log(f"Track trade error: {e}")
            updated.append(setup)
    return updated


def main() -> None:
    log("N Pattern Railway live monitor with OI + dashboard image started")
    maybe_send_startup_sample()

    # Read JSON and send one summary message immediately, even on weekends/off-market hours.
    try:
        local_items = []
        if os.path.exists(JSON_FILE):
            try:
                with open(JSON_FILE, "r", encoding="utf-8") as f:
                    local_items = json.load(f)
            except Exception:
                local_items = []
        incoming = load_setups()
        initial_setups = merge_state(local_items, incoming) if incoming else local_items
        for s in initial_setups:
            if is_completed_without_entry(s):
                s["status"] = "completed_without_entry"
        if initial_setups:
            send_json_read_sample(initial_setups)
    except Exception as e:
        log(f"Initial JSON read sample failed: {e}")

    wait_until_market_start()
    last_symbols: List[str] = []
    while True:
        try:
            if ONLY_MARKET_HOURS and not in_market_hours():
                wait_until_market_start()
                continue

            local_items = []
            if os.path.exists(JSON_FILE):
                try:
                    with open(JSON_FILE, "r", encoding="utf-8") as f:
                        local_items = json.load(f)
                except Exception:
                    local_items = []
            incoming = load_setups()
            setups = merge_state(local_items, incoming) if incoming else local_items
            for s in setups:
                if is_completed_without_entry(s):
                    s["status"] = "completed_without_entry"
            if not setups:
                log("No setups found; sleeping")
                time.sleep(max(POLL_SECONDS, 60))
                continue

            send_json_read_sample(setups)

            symbols = [str(x.get("symbol") or "") for x in setups if str(x.get("symbol") or "")]
            if USE_WEBSOCKET and symbols != last_symbols:
                ensure_websocket(symbols)
                last_symbols = sorted(set(symbols))

            updated: List[Dict[str, Any]] = []
            new_alerts: List[Dict[str, Any]] = []
            for setup in setups:
                if should_process_setup(setup):
                    updated_setup, alert_item = process_setup(setup)
                else:
                    updated_setup, alert_item = setup, None
                updated.append(updated_setup)
                if alert_item:
                    new_alerts.append(alert_item)
                time.sleep(RATE_LIMIT_SLEEP)

            if SEND_MULTI_CARD_SUMMARY and new_alerts:
                for i in range(0, len(new_alerts), max(1, MULTI_CARD_SIZE)):
                    send_multi_card_summary(new_alerts[i:i + max(1, MULTI_CARD_SIZE)])
                    time.sleep(1.0)

            updated = track_open_positions(updated)
            save_setups(updated)
        except Exception as e:
            log(f"Main loop error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

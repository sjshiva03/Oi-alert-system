import os
import io
import json
import time
from math import floor
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from fyers_apiv3 import fyersModel

IST = timezone(timedelta(hours=5, minutes=30))

JSON_FILE = os.getenv("JSON_FILE", "npattern_selected.json")
GITHUB_JSON_URL = (os.getenv("GITHUB_JSON_URL") or "").strip()
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
ENTRY_ZONE_PERCENT = float(os.getenv("ENTRY_ZONE_PERCENT", "2.0"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "1.2"))
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").strip().lower() == "true"
MARKET_START = tuple(int(x) for x in os.getenv("MARKET_START", "9,5").split(","))
MARKET_END = tuple(int(x) for x in os.getenv("MARKET_END", "15,30").split(","))
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
MULTI_CARD_SIZE = int(os.getenv("MULTI_CARD_SIZE", "6"))
SEND_MULTI_CARD_SUMMARY = os.getenv("SEND_MULTI_CARD_SUMMARY", "true").strip().lower() == "true"
TRACK_SL_TARGET = os.getenv("TRACK_SL_TARGET", "true").strip().lower() == "true"
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.0")) / 100.0
USE_STRONGER_DOJI = os.getenv("USE_STRONGER_DOJI", "true").strip().lower() == "true"

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

MANUAL_HOLIDAYS = {x.strip() for x in MANUAL_HOLIDAYS_RAW.split(",") if x.strip()}


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


def market_window_for_day(day: datetime) -> Tuple[datetime, datetime]:
    start = day.replace(hour=MARKET_START[0], minute=MARKET_START[1], second=0, microsecond=0)
    end = day.replace(hour=MARKET_END[0], minute=MARKET_END[1], second=0, microsecond=0)
    return start, end


def is_trading_day(day: datetime) -> bool:
    return day.weekday() < 5 and day.strftime("%Y-%m-%d") not in MANUAL_HOLIDAYS


def in_market_hours() -> bool:
    if not ONLY_MARKET_HOURS:
        return True
    n = now_ist()
    if not is_trading_day(n):
        return False
    start, end = market_window_for_day(n)
    return start <= n <= end


def next_market_start(after: Optional[datetime] = None) -> datetime:
    n = after or now_ist()
    check = n
    while True:
        if is_trading_day(check):
            start, end = market_window_for_day(check)
            if check <= start:
                return start
            if start <= check <= end:
                return check
        check = (check + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def wait_until_market_start() -> None:
    if not ONLY_MARKET_HOURS:
        return
    while True:
        n = now_ist()
        if in_market_hours():
            return
        nxt = next_market_start(n)
        seconds = max(1, int((nxt - n).total_seconds()))
        mins = max(1, floor(seconds / 60))
        log(f"Waiting for next market session @ {nxt.strftime('%d-%m-%Y %H:%M IST')} ({mins} min)")
        time.sleep(min(seconds, 300))


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
    payload = {"symbols": symbol}
    resp = call_with_retry(FYERS.quotes, payload, f"Quote {symbol}")
    if not resp:
        return None
    try:
        for item in resp.get("d", []):
            values = item.get("v", {})
            ltp = values.get("lp") or values.get("ltp") or values.get("last_price")
            if ltp is not None:
                return float(ltp)
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
# Entry logic
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
    if not USE_STRONGER_DOJI:
        max_body_ratio = 0.35
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
    custom_paths = [os.path.join(fonts_dir, "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf")]
    for p in custom_paths:
        try:
            if os.path.exists(p):
                return ImageFont.truetype(p, size)
        except Exception as e:
            log(f"Custom font load failed {p}: {e}")
    fallback = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
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
    tx = x1 + 12 if align == "left" else x1 + (x2 - x1 - tw) / 2
    ty = y1 + (y2 - y1 - th) / 2 - 1
    draw.text((tx, ty), text, font=font, fill=text_fill)


def calculate_confidence(setup: Dict[str, Any], d_price: float, entry_price: float, oi_bias: str) -> int:
    score = 50
    diff_pct = abs(entry_price - d_price) / d_price * 100 if d_price else 100
    if diff_pct <= 0.25:
        score += 15
    elif diff_pct <= 0.50:
        score += 10
    elif diff_pct <= 1.00:
        score += 6
    if str(setup.get("touched_d_source", "")).lower().startswith("fib"):
        score += 5
    p = str(setup.get("pattern", "")).lower()
    b = oi_bias.lower()
    if p == "bullish" and "bullish" in b:
        score += 20 if b == "bullish" else 12
    elif p == "bearish" and "bearish" in b:
        score += 20 if b == "bearish" else 12
    elif b == "neutral":
        score += 4
    return max(0, min(95, score))


def build_dashboard_image(setup: Dict[str, Any], entry_price: float, entry_time: datetime, oisnap: Dict[str, Any], startup_preview: bool = False) -> bytes:
    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (245, 245, 245))
    draw = ImageDraw.Draw(img)

    black = (18, 18, 18)
    green = (28, 148, 82)
    red = (198, 58, 58)
    blue = (48, 97, 184)
    white = (255, 255, 255)
    border = (210, 210, 210)
    table_head_fill = (236, 240, 247)

    f_title = _load_font(40, True)
    f_header = _load_font(21, True)
    f_label = _load_font(19, True)
    f_value = _load_font(18, False)
    f_table_h = _load_font(20, True)
    f_table_b = _load_font(19, False)
    f_stamp = _load_font(18, False)
    f_footer = _load_font(16, False)

    draw.rounded_rectangle((28, 22, IMAGE_WIDTH - 28, 105), radius=24, fill=white, outline=border, width=2)
    draw.text((58, 45), "N PATTERN ENTRY ALERT", font=f_title, fill=blue)
    stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
    stamp_bbox = draw.textbbox((0, 0), stamp, font=f_stamp)
    draw.text((IMAGE_WIDTH - 55 - (stamp_bbox[2] - stamp_bbox[0]), 48), stamp, font=f_stamp, fill=black)

    box_top = 132
    box_bottom = 400
    draw.rounded_rectangle((28, box_top, IMAGE_WIDTH - 28, box_bottom), radius=24, fill=white, outline=border, width=2)

    touched_d = ensure_float(setup.get("touched_d_price") or setup.get("fib_d"))
    active_d = ensure_float(setup.get("active_d"))
    ltp_alert = ensure_float(setup.get("ltp_at_alert") or setup.get("ltp"))
    pattern = str(setup.get("pattern", ""))
    target_px = ensure_float(setup.get("target_price") or (entry_price * (1.02 if pattern.lower() == "bullish" else 0.98)))
    sl_px = ensure_float(setup.get("sl_price") or active_d)
    entry_time_str = entry_time.astimezone(IST).strftime("%d-%m-%Y %H:%M")
    oi_bias = str(oisnap.get("bias", "NA"))
    d_source = str(setup.get("touched_d_source") or "")
    confidence = calculate_confidence(setup, touched_d, entry_price, oi_bias)

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
        ("Confidence", f"{confidence}%", blue),
    ]

    y0 = box_top + 26
    row_gap = 41
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

    oi_top = 432
    oi_bottom = IMAGE_HEIGHT - 38
    draw.rounded_rectangle((28, oi_top, IMAGE_WIDTH - 28, oi_bottom), radius=24, fill=white, outline=border, width=2)
    draw.text((55, oi_top + 18), "OI SNAPSHOT", font=f_header, fill=black)

    rows = oisnap.get("rows", [])[:11]
    tx1 = 52
    ty1 = oi_top + 62
    row_h = 62
    headers = ["Strike", "CE", "CE OI Chg", "PE", "PE OI Chg", "CE Vol", "PE Vol"]
    widths = [170, 170, 240, 170, 240, 180, 180]
    xs = [tx1]
    for w in widths:
        xs.append(xs[-1] + w)

    for j, h in enumerate(headers):
        draw_cell(draw, xs[j], ty1, xs[j+1], ty1 + row_h, h, f_table_h, fill=table_head_fill, outline=border, text_fill=blue)

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


def build_multi_card_image(cards: List[Dict[str, Any]]) -> bytes:
    width = IMAGE_WIDTH
    height = max(980, 280 + ((len(cards[:MULTI_CARD_SIZE]) + 2) // 3) * 330)
    img = Image.new("RGB", (width, height), (245, 245, 245))
    draw = ImageDraw.Draw(img)
    white = (255, 255, 255)
    border = (210, 210, 210)
    black = (18, 18, 18)
    green = (28, 148, 82)
    red = (198, 58, 58)
    blue = (48, 97, 184)

    f_title = _load_font(34, True)
    f_card_title = _load_font(24, True)
    f_label = _load_font(18, True)
    f_value = _load_font(18, False)

    draw.rounded_rectangle((24, 18, width - 24, 96), radius=24, fill=white, outline=border, width=2)
    draw.text((50, 42), "N PATTERN LIVE ENTRIES", font=f_title, fill=blue)
    stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
    sb = draw.textbbox((0, 0), stamp, font=f_value)
    draw.text((width - 50 - (sb[2]-sb[0]), 45), stamp, font=f_value, fill=black)

    cards = cards[:MULTI_CARD_SIZE]
    left = 28
    top = 122
    gap_x = 24
    gap_y = 24
    card_w = int((width - left * 2 - gap_x * 2) / 3)
    card_h = 300

    for idx, c in enumerate(cards):
        row = idx // 3
        col = idx % 3
        x1 = left + col * (card_w + gap_x)
        y1 = top + row * (card_h + gap_y)
        x2 = x1 + card_w
        y2 = y1 + card_h
        draw.rounded_rectangle((x1, y1, x2, y2), radius=20, fill=white, outline=border, width=2)
        draw.text((x1 + 20, y1 + 18), short_symbol(c.get("symbol", "")), font=f_card_title, fill=black)
        draw.text((x1 + 20, y1 + 55), str(c.get("pattern", "")), font=f_value, fill=blue)
        y = y1 + 92
        items = [
            ("Entry", f"{ensure_float(c.get('entry_price')):.2f}", green),
            ("SL", f"{ensure_float(c.get('sl_price')):.2f}", red),
            ("Target", f"{ensure_float(c.get('target_price')):.2f}", green),
            ("OI", str(c.get('oi_bias', 'NA')), green if 'Bullish' in str(c.get('oi_bias','')) else red if 'Bearish' in str(c.get('oi_bias','')) else black),
            ("Conf", f"{int(c.get('confidence_score', 0))}%", blue),
        ]
        for lab, val, color in items:
            draw.text((x1 + 20, y), f"{lab}:", font=f_label, fill=black)
            draw.text((x1 + 150, y), val, font=f_value, fill=color)
            y += 36

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


# =========================
# Setup state
# =========================
def load_setups() -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    if GITHUB_JSON_URL:
        try:
            r = requests.get(GITHUB_JSON_URL, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            # merge lightweight github data with local runtime state
            local_map = {}
            if os.path.exists(JSON_FILE):
                try:
                    with open(JSON_FILE, "r", encoding="utf-8") as f:
                        local_items = json.load(f)
                    for item in local_items:
                        key = f"{item.get('symbol')}|{item.get('pattern')}|{item.get('fib_d')}|{item.get('trend_d')}"
                        local_map[key] = item
                except Exception:
                    pass
            merged = []
            for item in data:
                key = f"{item.get('symbol')}|{item.get('pattern')}|{item.get('fib_d')}|{item.get('trend_d')}"
                if key in local_map:
                    merged_item = dict(local_map[key])
                    merged_item.update(item)
                    merged.append(merged_item)
                else:
                    merged.append(item)
            with open(JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(merged, f, indent=2, ensure_ascii=False)
            log(f"Loaded {len(merged)} setups from GitHub JSON")
            return merged
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
                {"strike": 565, "ce_ltp": 4.8,  "ce_oich": 200,   "pe_ltp": 11.7, "pe_oich": 1500, "ce_vol": 3500, "pe_vol": 5600},
                {"strike": 570, "ce_ltp": 3.1,  "ce_oich": 600,   "pe_ltp": 16.4, "pe_oich": 900,  "ce_vol": 2100, "pe_vol": 3900},
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
def detect_touch(setup: Dict[str, Any], ltp: float) -> Tuple[Optional[float], str]:
    fib_d = ensure_float(setup.get("fib_d"))
    trend_d = ensure_float(setup.get("trend_d"))
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


def compute_sl_target(setup: Dict[str, Any], entry_price: float) -> Tuple[float, float]:
    active_d = ensure_float(setup.get("active_d") or setup.get("touched_d_price") or setup.get("fib_d"))
    pattern = str(setup.get("pattern", "Bullish")).lower()
    if pattern == "bullish":
        sl = active_d * (1.0 - SL_BUFFER_PCT)
        target = entry_price * 1.02
    else:
        sl = active_d * (1.0 + SL_BUFFER_PCT)
        target = entry_price * 0.98
    return round(sl, 2), round(target, 2)


def process_setup(setup: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    symbol = str(setup.get("symbol") or "")
    if not symbol:
        return setup, None

    setup["last_checked_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    if setup.get("entry_found") and TRACK_SL_TARGET:
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
        return setup, None

    log(f"{symbol}: touched {source} D @ {d_price} | LTP={ltp}")

    oisnap = fetch_option_chain_snapshot(symbol)
    bias = str(oisnap.get("bias", "NA"))
    if not oi_allows_entry(str(setup.get("pattern", "")), bias):
        setup["oi_bias"] = bias
        setup["last_debug_reason"] = "OI rejected entry"
        return setup, None

    df = fetch_history(symbol, resolution=str(setup.get("entry_tf") or "15"), days=5)
    if df is None or df.empty:
        setup["last_debug_reason"] = "Lower timeframe history unavailable"
        return setup, None

    entry_price, entry_time, debug = check_entry_after_touch(df, d_price, str(setup.get("pattern", "Bullish")), ENTRY_ZONE_PERCENT)
    setup["oi_bias"] = bias
    setup["last_debug_reason"] = debug.get("reason", "")

    if not entry_price or not entry_time:
        return setup, None

    sl_price, target_price = compute_sl_target(setup, float(entry_price))
    confidence = calculate_confidence(setup, d_price, float(entry_price), bias)

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
    setup["sl_price"] = sl_price
    setup["target_price"] = target_price
    setup["confidence_score"] = confidence
    setup["sl_alert_sent"] = False
    setup["target_alert_sent"] = False

    entry_card = {
        "symbol": symbol,
        "pattern": setup.get("pattern", ""),
        "entry_price": setup["entry_price"],
        "sl_price": sl_price,
        "target_price": target_price,
        "oi_bias": bias,
        "confidence_score": confidence,
    }
    return setup, entry_card


def track_open_trade(setup: Dict[str, Any]) -> Dict[str, Any]:
    if not TRACK_SL_TARGET:
        return setup
    if not setup.get("entry_found"):
        return setup
    if setup.get("target_alert_sent") or setup.get("sl_alert_sent"):
        return setup

    symbol = str(setup.get("symbol") or "")
    pattern = str(setup.get("pattern") or "Bullish")
    entry_price = ensure_float(setup.get("entry_price"))
    target_price = ensure_float(setup.get("target_price"))
    sl_price = ensure_float(setup.get("sl_price"))
    if not symbol or not entry_price or not target_price or not sl_price:
        return setup

    ltp = get_ltp(symbol)
    if ltp is None:
        return setup

    bullish = pattern.lower() == "bullish"
    target_hit = ltp >= target_price if bullish else ltp <= target_price
    sl_hit = ltp <= sl_price if bullish else ltp >= sl_price

    if target_hit:
        msg = (
            f"✅ TARGET HIT\n"
            f"Stock: {short_symbol(symbol)}\n"
            f"Pattern: {pattern}\n"
            f"Entry: {entry_price:.2f}\n"
            f"Target: {target_price:.2f}\n"
            f"LTP: {ltp:.2f}"
        )
        send_telegram_text(msg)
        setup["target_alert_sent"] = True
        setup["status"] = "target_hit"
        setup["exit_price"] = round(float(ltp), 2)
        setup["exit_time"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        log(f"{symbol}: target alert sent")
    elif sl_hit:
        msg = (
            f"❌ STOPLOSS HIT\n"
            f"Stock: {short_symbol(symbol)}\n"
            f"Pattern: {pattern}\n"
            f"Entry: {entry_price:.2f}\n"
            f"SL: {sl_price:.2f}\n"
            f"LTP: {ltp:.2f}"
        )
        send_telegram_text(msg)
        setup["sl_alert_sent"] = True
        setup["status"] = "sl_hit"
        setup["exit_price"] = round(float(ltp), 2)
        setup["exit_time"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        log(f"{symbol}: stoploss alert sent")

    return setup


def maybe_send_entry_alerts(entry_cards: List[Dict[str, Any]]) -> None:
    if not entry_cards:
        return
    if SEND_MULTI_CARD_SUMMARY and len(entry_cards) > 1 and TELEGRAM_SEND_MODE != "text":
        image_bytes = build_multi_card_image(entry_cards)
        caption = f"🔥 N PATTERN LIVE ENTRIES | Count: {len(entry_cards[:MULTI_CARD_SIZE])}"
        send_telegram_photo(image_bytes, caption)
        return

    for c in entry_cards:
        caption = (
            f"🔥 N PATTERN ENTRY\n"
            f"Stock: {short_symbol(c['symbol'])}\n"
            f"Pattern: {c['pattern']}\n"
            f"Entry: {c['entry_price']:.2f}\n"
            f"SL: {c['sl_price']:.2f}\n"
            f"Target: {c['target_price']:.2f}\n"
            f"OI Bias: {c['oi_bias']}\n"
            f"Confidence: {int(c['confidence_score'])}%"
        )
        send_telegram_text(caption)


def main() -> None:
    log("N Pattern Railway live monitor upgraded started")
    maybe_send_startup_sample()
    wait_until_market_start()
    while True:
        try:
            if not in_market_hours():
                wait_until_market_start()
                continue

            setups = load_setups()
            if not setups:
                log("No setups found")
                time.sleep(max(POLL_SECONDS, 60))
                continue

            updated = []
            entry_cards: List[Dict[str, Any]] = []
            for setup in setups:
                setup, card = process_setup(setup)
                setup = track_open_trade(setup)
                updated.append(setup)
                if card:
                    entry_cards.append(card)
                time.sleep(RATE_LIMIT_SLEEP)

            maybe_send_entry_alerts(entry_cards)
            save_setups(updated)
        except Exception as e:
            log(f"Main loop error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

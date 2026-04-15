import os
import io
import gzip
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont

IST = timezone(timedelta(hours=5, minutes=30))

# =========================
# ENV / SETTINGS
# =========================
UPSTOX_ACCESS_TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or "").strip()
JSON_FILE = os.getenv("JSON_FILE", "npattern_selected.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").strip().lower() == "true"
DE_BUG = os.getenv("DE_BUG", "false").strip().lower() == "true"

ENTRY_LIMIT_PERCENT = float(os.getenv("ENTRY_LIMIT_PERCENT", os.getenv("ENTRY_DISTANCE_PERCENT", "2.0")))
ENTRY_BUFFER_PERCENT = float(os.getenv("ENTRY_BUFFER_PERCENT", "0.2"))
HA_BODY_PERCENT = float(os.getenv("HA_BODY_PERCENT", "30"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", str(6 * 60 * 60)))
HISTORICAL_RUN_ONCE = os.getenv("HISTORICAL_RUN_ONCE", "true").strip().lower() == "true"
LOOKBACK_DAYS_MINUTE = int(os.getenv("LOOKBACK_DAYS_MINUTE", "60"))
LOOKBACK_DAYS_DAILY = int(os.getenv("LOOKBACK_DAYS_DAILY", "365"))
IMAGE_WIDTH = int(os.getenv("IMAGE_WIDTH", "1500"))
IMAGE_HEIGHT = int(os.getenv("IMAGE_HEIGHT", "1100"))
CARDS_PER_IMAGE = int(os.getenv("CARDS_PER_IMAGE", "6"))
TARGET_PERCENT = float(os.getenv("TARGET_PERCENT", "2.0"))

TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
}

INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
MARKET_START = (9, 15)
MARKET_END = (15, 30)

_INSTRUMENT_MAP: Dict[str, Dict[str, Any]] = {}

# =========================
# BASICS
# =========================
def now_ist() -> datetime:
    return datetime.now(IST)


def log(msg: str) -> None:
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] {msg}", flush=True)


def debug(msg: str) -> None:
    if DE_BUG:
        log(f"[DE_BUG] {msg}")


def ensure_float(x: Any, default: float = 0.0) -> float:
    try:
        if x in (None, ""):
            return default
        return float(x)
    except Exception:
        return default


def short_symbol(symbol: str) -> str:
    s = str(symbol or "").replace("NSE:", "").replace("BSE:", "")
    for suf in ["-EQ", "-BE", "-BZ", "-BL", "-SM", "-INDEX"]:
        s = s.replace(suf, "")
    return s.strip()


def compact_num(v: Any) -> str:
    n = ensure_float(v, 0.0)
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e7:
        return f"{sign}{n/1e7:.2f}Cr"
    if n >= 1e5:
        return f"{sign}{n/1e5:.2f}L"
    if n >= 1e3:
        return f"{sign}{n/1e3:.1f}K"
    return f"{sign}{n:.2f}"


def parse_any_datetime(x: Any) -> Optional[datetime]:
    if x in (None, ""):
        return None
    if isinstance(x, datetime):
        return x.astimezone(IST) if x.tzinfo else x.replace(tzinfo=IST)
    s = str(x).strip()
    if not s:
        return None
    formats = [
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=IST) if dt.tzinfo is None else dt.astimezone(IST)
        except Exception:
            pass
    try:
        ts = pd.to_datetime(s, utc=False, errors="coerce")
        if pd.isna(ts):
            return None
        if ts.tzinfo is None:
            return ts.to_pydatetime().replace(tzinfo=IST)
        return ts.tz_convert("Asia/Kolkata").to_pydatetime()
    except Exception:
        return None


def format_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def is_market_hours() -> bool:
    if not ONLY_MARKET_HOURS:
        return True
    n = now_ist()
    if n.weekday() >= 5:
        return False
    start = n.replace(hour=MARKET_START[0], minute=MARKET_START[1], second=0, microsecond=0)
    end = n.replace(hour=MARKET_END[0], minute=MARKET_END[1], second=0, microsecond=0)
    return start <= n <= end


def wait_until_market_start() -> None:
    while True:
        if is_market_hours():
            return
        n = now_ist()
        if n.weekday() >= 5:
            log("Weekend; waiting for next market session")
            time.sleep(600)
            continue
        start = n.replace(hour=MARKET_START[0], minute=MARKET_START[1], second=0, microsecond=0)
        if n < start:
            mins = int((start - n).total_seconds() // 60)
            log(f"Waiting for market start; {mins} min left")
            time.sleep(min(300, max(30, mins * 30)))
        else:
            time.sleep(60)


# =========================
# TELEGRAM
# =========================
def send_telegram_text(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram credentials missing; skipping text")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        log(f"Telegram text failed: {e}")


def send_telegram_photo(image_bytes: bytes, caption: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram credentials missing; skipping image")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        files = {"photo": ("status.png", image_bytes, "image/png")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1024]}
        resp = requests.post(url, files=files, data=data, timeout=max(HTTP_TIMEOUT, 30))
        resp.raise_for_status()
    except Exception as e:
        log(f"Telegram photo failed: {e}")
        send_telegram_text(caption)


# =========================
# HTTP / MASTER
# =========================
def upstox_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            debug(f"GET retry {attempt}/{MAX_RETRIES} failed | url={url} | params={params} | error={e}")
            time.sleep(min(2.0 * attempt, 5.0))
    raise RuntimeError(f"Upstox GET failed: {url} | {last_err}")


def load_instrument_master() -> Dict[str, Dict[str, Any]]:
    global _INSTRUMENT_MAP
    if _INSTRUMENT_MAP:
        return _INSTRUMENT_MAP
    try:
        resp = requests.get(INSTRUMENTS_URL, timeout=60)
        resp.raise_for_status()
        content = gzip.decompress(resp.content)
        rows = json.loads(content.decode("utf-8"))
    except Exception as e:
        log(f"Instrument master load failed: {e}")
        _INSTRUMENT_MAP = {}
        return _INSTRUMENT_MAP

    mapping: Dict[str, Dict[str, Any]] = {}
    for rec in rows:
        trading_symbol = str(rec.get("trading_symbol", "")).strip().upper()
        instrument_key = str(rec.get("instrument_key", "")).strip()
        segment = str(rec.get("segment", "")).strip()
        instrument_type = str(rec.get("instrument_type", "")).strip()
        if not trading_symbol or not instrument_key:
            continue
        if segment == "NSE_EQ" and instrument_type in {"EQ", "BE", "BZ", "BL", "SM"}:
            mapping[trading_symbol] = rec
    _INSTRUMENT_MAP = mapping
    log(f"Loaded {len(mapping)} NSE_EQ instruments")
    return _INSTRUMENT_MAP


def symbol_to_instrument_key(symbol: str) -> Optional[str]:
    rec = load_instrument_master().get(short_symbol(symbol).upper())
    return str(rec.get("instrument_key", "")).strip() if rec else None


def load_setups() -> List[Dict[str, Any]]:
    if not os.path.exists(JSON_FILE):
        log(f"JSON file not found: {JSON_FILE}")
        return []
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def save_setups(items: List[Dict[str, Any]]) -> None:
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


# =========================
# PRICE / HISTORY
# =========================
def batch_fetch_ltps(setups: List[Dict[str, Any]]) -> Dict[str, float]:
    key_to_symbol: Dict[str, str] = {}
    keys: List[str] = []
    for s in setups:
        sym = str(s.get("symbol") or "")
        key = symbol_to_instrument_key(sym)
        if key:
            key_to_symbol[key] = sym
            keys.append(key)
    joined = ",".join(sorted(set(keys)))
    if not joined:
        return {}

    data = upstox_get("https://api.upstox.com/v3/market-quote/ltp", params={"instrument_key": joined})
    out: Dict[str, float] = {}
    payload = data.get("data", {}) if isinstance(data, dict) else {}
    if not isinstance(payload, dict):
        return out

    for outer_key, row in payload.items():
        if not isinstance(row, dict):
            continue
        sym = key_to_symbol.get(str(outer_key))
        if not sym:
            row_key = row.get("instrument_token") or row.get("instrument_key") or row.get("symbol")
            if row_key:
                sym = key_to_symbol.get(str(row_key))
        if not sym:
            continue
        ltp = row.get("last_price")
        if ltp is None:
            ltp = row.get("ltp")
        try:
            out[sym] = float(ltp)
        except Exception:
            pass
    debug(f"batch_fetch_ltps done | returned={len(out)} | symbols={list(out.keys())[:20]}")
    return out


def _parse_candles(candles: List[Any]) -> pd.DataFrame:
    rows = []
    for row in candles:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        rows.append({
            "ts": row[0], "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5]
        })
    if not rows:
        return pd.DataFrame(columns=["datetime", "o", "h", "l", "c", "v"])
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.tz_convert("Asia/Kolkata")
    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[["datetime", "o", "h", "l", "c", "v"]].dropna(subset=["datetime", "o", "h", "l", "c"]).reset_index(drop=True)


def fetch_history(instrument_key: str, resolution_minutes: int, lookback_days: Optional[int] = None) -> pd.DataFrame:
    today = now_ist().strftime("%Y-%m-%d")
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS_DAILY if resolution_minutes >= 1440 else LOOKBACK_DAYS_MINUTE
    from_date = (now_ist() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    urls: List[str] = []
    if resolution_minutes >= 1440:
        urls.extend([
            f"https://api.upstox.com/v3/historical-candle/{instrument_key}/days/1/{today}/{from_date}",
            f"https://api.upstox.com/v2/historical-candle/{instrument_key}/day/{today}/{from_date}",
        ])
    else:
        res = str(int(resolution_minutes))
        urls.extend([
            f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{res}/{today}/{from_date}",
            f"https://api.upstox.com/v2/historical-candle/{instrument_key}/{res}minute/{today}/{from_date}",
        ])
    for url in urls:
        try:
            data = upstox_get(url)
            candles = ((data.get("data") or {}).get("candles")) or data.get("candles") or []
            df = _parse_candles(candles)
            if not df.empty:
                debug(f"fetch_history success | key={instrument_key} | tf={resolution_minutes} | candles={len(df)} | url={url}")
                return df
        except Exception as e:
            debug(f"fetch_history failed | key={instrument_key} | tf={resolution_minutes} | url={url} | error={e}")
    return pd.DataFrame(columns=["datetime", "o", "h", "l", "c", "v"])


# =========================
# TF / PATTERN HELPERS
# =========================
def infer_pattern_tf_minutes(setup: Dict[str, Any]) -> int:
    a = parse_any_datetime(setup.get("a_time"))
    b = parse_any_datetime(setup.get("b_time"))
    c = parse_any_datetime(setup.get("c_time"))
    diffs = []
    for x, y in [(a, b), (b, c)]:
        if x and y:
            mins = abs((y - x).total_seconds()) / 60.0
            if mins >= 1:
                diffs.append(mins)
    if not diffs:
        return 240
    avg = sum(diffs) / len(diffs)
    known = [15, 30, 45, 60, 120, 240, 1440]
    return min(known, key=lambda k: abs(k - avg))


def tf_delta(minutes: int) -> timedelta:
    return timedelta(minutes=minutes)


def current_d_candidates(setup: Dict[str, Any]) -> List[Tuple[str, float]]:
    vals: List[Tuple[str, float]] = []
    seen = set()
    for item in (setup.get("d_levels") or []):
        label = str(item.get("level") or "")
        value = ensure_float(item.get("value"), 0.0)
        if value > 0 and value not in seen:
            vals.append((label or f"D{len(vals)+1}", value))
            seen.add(value)
    for label, field in [("Fib", "fib_d"), ("Trend", "trend_d"), ("Active D", "active_d")]:
        value = ensure_float(setup.get(field), 0.0)
        if value > 0 and value not in seen:
            vals.append((label, value))
            seen.add(value)
    vals = sorted(vals, key=lambda x: x[1])
    return vals


def lowest_d(setup: Dict[str, Any]) -> float:
    vals = [v for _, v in current_d_candidates(setup)]
    return min(vals) if vals else 0.0


def highest_d(setup: Dict[str, Any]) -> float:
    vals = [v for _, v in current_d_candidates(setup)]
    return max(vals) if vals else 0.0


def touch_match_from_candle(setup: Dict[str, Any], row: pd.Series) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    pattern = str(setup.get("pattern") or "").strip().lower()
    levels = current_d_candidates(setup)
    if not levels:
        return None, None, None
    low = ensure_float(row.get("l"))
    high = ensure_float(row.get("h"))
    touched = [(lab, val) for lab, val in levels if low <= val <= high]
    if not touched:
        return None, None, None
    if pattern == "bullish":
        lab, val = max(touched, key=lambda x: x[1])
    else:
        lab, val = min(touched, key=lambda x: x[1])
    return lab, val, low if pattern == "bullish" else high


def touch_match_from_ltp(setup: Dict[str, Any], ltp: float) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    pattern = str(setup.get("pattern") or "").strip().lower()
    levels = current_d_candidates(setup)
    if not levels:
        return None, None, None
    if pattern == "bullish":
        touched = [(lab, val) for lab, val in levels if ltp <= val]
        if not touched:
            return None, None, None
        lab, val = max(touched, key=lambda x: x[1])
        return lab, val, ltp
    else:
        touched = [(lab, val) for lab, val in levels if ltp >= val]
        if not touched:
            return None, None, None
        lab, val = min(touched, key=lambda x: x[1])
        return lab, val, ltp


def to_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    ha = df.copy().reset_index(drop=True)
    if ha.empty:
        return ha
    ha["ha_close"] = (ha["o"] + ha["h"] + ha["l"] + ha["c"]) / 4.0
    ha["ha_open"] = 0.0
    ha.loc[0, "ha_open"] = (ha.loc[0, "o"] + ha.loc[0, "c"]) / 2.0
    for i in range(1, len(ha)):
        ha.loc[i, "ha_open"] = (ha.loc[i - 1, "ha_open"] + ha.loc[i - 1, "ha_close"]) / 2.0
    ha["ha_high"] = ha[["h", "ha_open", "ha_close"]].max(axis=1)
    ha["ha_low"] = ha[["l", "ha_open", "ha_close"]].min(axis=1)
    return ha


def is_doji(row: pd.Series) -> bool:
    rng = float(row["ha_high"] - row["ha_low"])
    if rng <= 0:
        return False
    body = abs(float(row["ha_close"] - row["ha_open"]))
    return (body / rng) <= (HA_BODY_PERCENT / 100.0)


def is_open_low_pct(open_p: float, low_p: float) -> bool:
    if low_p == 0:
        return False
    return (abs(open_p - low_p) / abs(low_p) * 100.0) <= ENTRY_BUFFER_PERCENT


def is_open_high_pct(open_p: float, high_p: float) -> bool:
    if high_p == 0:
        return False
    return (abs(open_p - high_p) / abs(high_p) * 100.0) <= ENTRY_BUFFER_PERCENT


def within_entry_limit(price: float, d_ref: float) -> bool:
    if d_ref <= 0:
        return False
    return abs(price - d_ref) / abs(d_ref) * 100.0 <= ENTRY_LIMIT_PERCENT


# =========================
# STATUS / IMAGE
# =========================
def load_font(size: int, bold: bool = False):
    name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    candidates = [
        os.path.join(os.getcwd(), "fonts", name),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "fonts", name),
        f"/usr/share/fonts/truetype/dejavu/{name}",
    ]
    for path in candidates:
        try:
            if os.path.exists(path):
                return ImageFont.truetype(path, size)
        except Exception:
            pass
    return ImageFont.load_default()


def build_status_board(title: str, records: List[Dict[str, Any]]) -> List[bytes]:
    if not records:
        return []
    pages: List[bytes] = []
    per_page = max(1, CARDS_PER_IMAGE)
    font_title = load_font(48, True)
    font_head = load_font(24, True)
    font_body = load_font(22, False)
    for start in range(0, len(records), per_page):
        chunk = records[start:start+per_page]
        img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (243, 246, 250))
        draw = ImageDraw.Draw(img)
        draw.text((40, 22), title, font=font_title, fill=(20, 40, 90))
        stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
        draw.text((IMAGE_WIDTH - 340, 34), stamp, font=font_body, fill=(60, 70, 80))
        cols = 2
        rows = max(1, (per_page + cols - 1) // cols)
        card_w = (IMAGE_WIDTH - 60) // cols - 20
        card_h = (IMAGE_HEIGHT - 120) // rows - 20
        for i, rec in enumerate(chunk):
            r = i // cols
            c = i % cols
            x1 = 30 + c * (card_w + 20)
            y1 = 100 + r * (card_h + 20)
            x2 = x1 + card_w
            y2 = y1 + card_h
            draw.rounded_rectangle((x1, y1, x2, y2), radius=24, fill=(255, 255, 255), outline=(216, 224, 232), width=2)
            sym = rec.get("display_symbol") or short_symbol(rec.get("symbol"))
            draw.text((x1 + 18, y1 + 14), str(sym), font=font_head, fill=(18, 18, 18))
            status = str(rec.get("status") or "")
            draw.text((x1 + 18, y1 + 48), status, font=font_body, fill=(30, 100, 180))
            lines = [
                f"Pattern: {rec.get('pattern', '')}",
                f"D: {rec.get('touched_d_source', '')} {ensure_float(rec.get('touched_d_price'), 0.0):.2f}" if ensure_float(rec.get('touched_d_price'), 0.0) else f"D: {ensure_float(rec.get('active_d'), 0.0):.2f}",
                f"Touch Px: {ensure_float(rec.get('touched_price'), 0.0):.2f}" if ensure_float(rec.get('touched_price'), 0.0) else "Touch Px: -",
                f"Touch Time: {str(rec.get('touched_at') or '')[:19]}",
                f"Entry: {ensure_float(rec.get('entry_price'), 0.0):.2f}" if rec.get('entry_price') not in (None, '') else "Entry: -",
                f"Entry Time: {str(rec.get('entry_time') or '')[:19]}",
                f"SL: {ensure_float(rec.get('sl_price'), 0.0):.2f}" if rec.get('sl_price') not in (None, '') else "SL: -",
                f"Target: {ensure_float(rec.get('target_price'), 0.0):.2f}" if rec.get('target_price') not in (None, '') else "Target: -",
                f"LTP: {ensure_float(rec.get('ltp'), 0.0):.2f}" if rec.get('ltp') not in (None, '') else "LTP: -",
                f"Reason: {str(rec.get('entry_reason') or rec.get('last_debug_reason') or '')[:60]}",
            ]
            yy = y1 + 84
            for line in lines:
                draw.text((x1 + 18, yy), line, font=font_body, fill=(65, 74, 84))
                yy += 32
        bio = io.BytesIO()
        img.save(bio, format="PNG")
        pages.append(bio.getvalue())
    return pages


def send_status_boards(events: Dict[str, List[Dict[str, Any]]]) -> None:
    mapping = {
        "touched": "D TOUCHED",
        "entry": "ENTRY TRIGGERED",
        "status": "STATUS DATA",
    }
    for key, title in mapping.items():
        items = events.get(key) or []
        if not items:
            continue
        for idx, page in enumerate(build_status_board(title, items), 1):
            caption = f"{title} ({idx}) | Count: {len(items)}"
            send_telegram_photo(page, caption)


# =========================
# STATE MACHINE
# =========================
def reset_runtime_state(setup: Dict[str, Any]) -> Dict[str, Any]:
    setup = dict(setup)
    setup["d_hit"] = "No"
    setup["touched_d_price"] = 0.0
    setup["touched_d_source"] = ""
    setup["touched_price"] = 0.0
    setup["touched_at"] = ""
    setup["entry_found"] = False
    setup["entry_price"] = None
    setup["entry_time"] = ""
    setup["entry_reason"] = ""
    setup["sl_price"] = None
    setup["target_price"] = None
    setup["target_hit"] = False
    setup["sl_hit"] = False
    setup["status"] = "waiting"
    setup["alert_sent"] = False
    setup["ltp_at_alert"] = None
    setup["last_alert_at"] = ""
    setup["pattern_active"] = "Yes"
    setup["target_status"] = ""
    return setup


def update_touch_state(setup: Dict[str, Any], source: str, d_price: float, touch_price: float, touch_dt: datetime) -> None:
    setup["d_hit"] = "Yes"
    setup["touched_d_source"] = source
    setup["touched_d_price"] = round(float(d_price), 2)
    setup["touched_price"] = round(float(touch_price), 2)
    setup["touched_at"] = format_dt(touch_dt)
    setup["status"] = "touched"
    setup["active_d"] = round(float(d_price), 2)


def mark_pattern_failure(setup: Dict[str, Any], fail_dt: datetime, reason: str) -> None:
    setup["status"] = "pattern_failed"
    setup["pattern_active"] = "No"
    setup["target_status"] = reason
    setup["last_debug_reason"] = reason
    setup["failure_time"] = format_dt(fail_dt)


def mark_entry(setup: Dict[str, Any], entry_price: float, entry_time: datetime, reason: str) -> None:
    pattern = str(setup.get("pattern") or "").strip().lower()
    sl_ref = lowest_d(setup) if pattern == "bullish" else highest_d(setup)
    setup["entry_found"] = True
    setup["entry_price"] = round(float(entry_price), 2)
    setup["entry_time"] = format_dt(entry_time)
    setup["entry_reason"] = reason
    setup["status"] = "entry_found"
    setup["sl_price"] = round(float(sl_ref), 2) if sl_ref > 0 else None
    if pattern == "bullish":
        setup["target_price"] = round(float(entry_price) * (1 + TARGET_PERCENT / 100.0), 2)
    else:
        setup["target_price"] = round(float(entry_price) * (1 - TARGET_PERCENT / 100.0), 2)


def mark_stoploss(setup: Dict[str, Any], hit_dt: datetime, reason: str) -> None:
    setup["sl_hit"] = True
    setup["status"] = "sl_hit"
    setup["target_status"] = reason
    setup["sl_time"] = format_dt(hit_dt)


def maybe_mark_target(setup: Dict[str, Any], pattern_df: pd.DataFrame, entry_dt: datetime) -> None:
    if pattern_df.empty or not setup.get("entry_found"):
        return
    target = ensure_float(setup.get("target_price"), 0.0)
    if target <= 0:
        return
    pattern = str(setup.get("pattern") or "").strip().lower()
    later = pattern_df[pattern_df["datetime"] >= entry_dt]
    for _, row in later.iterrows():
        high = ensure_float(row["h"])
        low = ensure_float(row["l"])
        dt = row["datetime"].to_pydatetime() if hasattr(row["datetime"], 'to_pydatetime') else row["datetime"]
        if pattern == "bullish" and high >= target:
            setup["target_hit"] = True
            setup["status"] = "target_hit"
            setup["target_status"] = "Target hit"
            setup["target_time"] = format_dt(dt)
            return
        if pattern == "bearish" and low <= target:
            setup["target_hit"] = True
            setup["status"] = "target_hit"
            setup["target_status"] = "Target hit"
            setup["target_time"] = format_dt(dt)
            return


def find_failure_before_entry(setup: Dict[str, Any], pattern_df: pd.DataFrame, start_dt: datetime, tf_minutes: int) -> Optional[datetime]:
    pattern = str(setup.get("pattern") or "").strip().lower()
    c_price = ensure_float(setup.get("c_price"), 0.0)
    if c_price <= 0 or pattern_df.empty:
        return None
    window = pattern_df[pattern_df["datetime"] >= start_dt]
    for _, row in window.iterrows():
        close = ensure_float(row["c"], 0.0)
        row_start = row["datetime"].to_pydatetime() if hasattr(row["datetime"], 'to_pydatetime') else row["datetime"]
        row_close_dt = row_start + tf_delta(tf_minutes)
        if pattern == "bullish" and close > c_price:
            return row_close_dt
        if pattern == "bearish" and close < c_price:
            return row_close_dt
    return None


def find_touch_historical(setup: Dict[str, Any], pattern_df: pd.DataFrame, start_dt: datetime, tf_minutes: int) -> Tuple[Optional[str], Optional[float], Optional[float], Optional[datetime]]:
    window = pattern_df[pattern_df["datetime"] >= start_dt]
    for _, row in window.iterrows():
        source, d_price, touch_px = touch_match_from_candle(setup, row)
        if source and d_price is not None and touch_px is not None:
            row_start = row["datetime"].to_pydatetime() if hasattr(row["datetime"], 'to_pydatetime') else row["datetime"]
            touch_dt = row_start + tf_delta(tf_minutes)
            return source, d_price, touch_px, touch_dt
    return None, None, None, None


def find_entry_15m(setup: Dict[str, Any], df15: pd.DataFrame, touch_dt: datetime, cutoff_dt: Optional[datetime]) -> Tuple[Optional[float], Optional[datetime], str]:
    if df15.empty or len(df15) < 2:
        return None, None, "No 15m candles"
    work = df15.copy()
    work = work[work["datetime"] >= touch_dt].sort_values("datetime").reset_index(drop=True)
    if cutoff_dt is not None:
        work = work[work["datetime"] < cutoff_dt].reset_index(drop=True)
    if len(work) < 2:
        return None, None, "No post-touch 15m candles"
    ha = to_heikin_ashi(work)
    pattern = str(setup.get("pattern") or "").strip().lower()
    d_ref = lowest_d(setup) if pattern == "bullish" else highest_d(setup)
    for i in range(len(ha) - 1):
        doji = ha.iloc[i]
        nxt = ha.iloc[i + 1]
        if not is_doji(doji):
            continue
        doji_dt = doji["datetime"].to_pydatetime() if hasattr(doji["datetime"], 'to_pydatetime') else doji["datetime"]
        next_dt = nxt["datetime"].to_pydatetime() if hasattr(nxt["datetime"], 'to_pydatetime') else nxt["datetime"]
        if doji_dt < touch_dt:
            continue
        if cutoff_dt is not None and next_dt >= cutoff_dt:
            break
        if pattern == "bullish":
            if is_open_low_pct(float(nxt["ha_open"]), float(nxt["ha_low"])):
                entry_price = float(doji["ha_high"])
                if within_entry_limit(entry_price, d_ref):
                    return entry_price, next_dt, f"Bullish doji + next HA open=low within {ENTRY_BUFFER_PERCENT:.2f}%"
                return None, None, f"Entry beyond {ENTRY_LIMIT_PERCENT:.2f}% from D"
        else:
            if is_open_high_pct(float(nxt["ha_open"]), float(nxt["ha_high"])):
                entry_price = float(doji["ha_low"])
                if within_entry_limit(entry_price, d_ref):
                    return entry_price, next_dt, f"Bearish doji + next HA open=high within {ENTRY_BUFFER_PERCENT:.2f}%"
                return None, None, f"Entry beyond {ENTRY_LIMIT_PERCENT:.2f}% from D"
    return None, None, "No valid 15m next-candle entry"


def apply_pattern_tf_post_entry_rules(setup: Dict[str, Any], pattern_df: pd.DataFrame, entry_dt: datetime, tf_minutes: int) -> None:
    if pattern_df.empty:
        return
    pattern = str(setup.get("pattern") or "").strip().lower()
    sl_ref = lowest_d(setup) if pattern == "bullish" else highest_d(setup)
    if sl_ref <= 0:
        return
    later = pattern_df[pattern_df["datetime"] >= entry_dt]
    for _, row in later.iterrows():
        close = ensure_float(row["c"], 0.0)
        row_start = row["datetime"].to_pydatetime() if hasattr(row["datetime"], 'to_pydatetime') else row["datetime"]
        close_dt = row_start + tf_delta(tf_minutes)
        if pattern == "bullish" and close < sl_ref:
            mark_stoploss(setup, close_dt, f"Pattern TF close below lowest D ({sl_ref:.2f})")
            return
        if pattern == "bearish" and close > sl_ref:
            mark_stoploss(setup, close_dt, f"Pattern TF close above highest D ({sl_ref:.2f})")
            return
    maybe_mark_target(setup, later, entry_dt)


def process_one_setup(setup: Dict[str, Any], ltp_map: Dict[str, float], historical_mode: bool) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    events: List[Dict[str, Any]] = []
    s = dict(setup)
    s["last_checked_at"] = format_dt(now_ist())
    symbol = str(s.get("symbol") or "")
    pattern = str(s.get("pattern") or "").strip().lower()
    instrument_key = symbol_to_instrument_key(symbol)
    if not instrument_key:
        s["last_debug_reason"] = "Instrument key missing"
        return s, events

    tf_minutes = infer_pattern_tf_minutes(s)
    s["pattern_tf_minutes"] = tf_minutes
    c_time = parse_any_datetime(s.get("c_time"))
    if not c_time:
        s["last_debug_reason"] = "C time missing"
        return s, events
    scan_start = c_time + tf_delta(tf_minutes)

    pattern_df = fetch_history(instrument_key, tf_minutes)
    if pattern_df.empty:
        s["last_debug_reason"] = "Pattern timeframe history missing"
        return s, events

    # Historical/off-market recalculates from data.
    if historical_mode:
        s = reset_runtime_state(s)

    # Failure before touch/entry.
    fail_dt = find_failure_before_entry(s, pattern_df, scan_start, tf_minutes)

    # Touch detection.
    touch_source = None
    touch_d = None
    touch_px = None
    touch_dt = None

    if s.get("d_hit") == "Yes" and s.get("touched_at"):
        touch_source = str(s.get("touched_d_source") or "")
        touch_d = ensure_float(s.get("touched_d_price"), 0.0)
        touch_px = ensure_float(s.get("touched_price"), 0.0)
        touch_dt = parse_any_datetime(s.get("touched_at"))

    if not touch_dt:
        if historical_mode:
            touch_source, touch_d, touch_px, touch_dt = find_touch_historical(s, pattern_df, scan_start, tf_minutes)
        else:
            ltp = ltp_map.get(symbol)
            if ltp is not None:
                touch_source, touch_d, touch_px = touch_match_from_ltp(s, ltp)
                if touch_source:
                    touch_dt = now_ist()
        if touch_dt and touch_source and touch_d is not None and touch_px is not None:
            update_touch_state(s, touch_source, touch_d, touch_px, touch_dt)
            events.append(dict(s))
            events[-1]["_event_type"] = "touched"
            debug(f"touch found | symbol={symbol} | source={touch_source} | d={touch_d} | price={touch_px} | dt={format_dt(touch_dt)}")

    if not touch_dt:
        if fail_dt:
            mark_pattern_failure(s, fail_dt, f"Pattern TF close beyond C before touch")
            events.append(dict(s))
            events[-1]["_event_type"] = "status"
        return s, events

    if fail_dt and fail_dt <= touch_dt:
        mark_pattern_failure(s, fail_dt, "Pattern TF close beyond C before touch")
        events.append(dict(s))
        events[-1]["_event_type"] = "status"
        return s, events

    # Entry search after touch, before failure.
    df15 = fetch_history(instrument_key, 15)
    entry_price = None
    entry_dt = None
    entry_reason = ""
    if not s.get("entry_found"):
        entry_price, entry_dt, entry_reason = find_entry_15m(s, df15, touch_dt, fail_dt)
        s["last_debug_reason"] = entry_reason
        if entry_price and entry_dt:
            mark_entry(s, entry_price, entry_dt, entry_reason)
            events.append(dict(s))
            events[-1]["_event_type"] = "entry"
            debug(f"entry found | symbol={symbol} | entry={entry_price} | dt={format_dt(entry_dt)}")
        elif fail_dt:
            mark_pattern_failure(s, fail_dt, "Pattern TF close beyond C before entry")
            events.append(dict(s))
            events[-1]["_event_type"] = "status"
            return s, events
        else:
            s["status"] = "touched"
            return s, events
    else:
        entry_dt = parse_any_datetime(s.get("entry_time"))

    if s.get("entry_found") and entry_dt:
        apply_pattern_tf_post_entry_rules(s, pattern_df, entry_dt, tf_minutes)
        if s.get("sl_hit") or s.get("target_hit"):
            events.append(dict(s))
            events[-1]["_event_type"] = "status"

    return s, events


# =========================
# MAIN
# =========================
def main() -> None:
    if not UPSTOX_ACCESS_TOKEN:
        raise RuntimeError("Missing UPSTOX_ACCESS_TOKEN")
    log("Strict pattern-state scanner started")
    debug(f"Startup env | JSON_FILE={JSON_FILE} | ONLY_MARKET_HOURS={ONLY_MARKET_HOURS} | ENTRY_LIMIT_PERCENT={ENTRY_LIMIT_PERCENT} | ENTRY_BUFFER_PERCENT={ENTRY_BUFFER_PERCENT} | HA_BODY_PERCENT={HA_BODY_PERCENT}")
    load_instrument_master()

    while True:
        try:
            historical_mode = not ONLY_MARKET_HOURS
            if ONLY_MARKET_HOURS and not is_market_hours():
                wait_until_market_start()
                continue

            setups = load_setups()
            debug(f"Loaded setups | count={len(setups)}")
            if not setups:
                time.sleep(max(POLL_SECONDS, 30))
                continue

            ltp_map = {} if historical_mode else batch_fetch_ltps(setups)
            updated: List[Dict[str, Any]] = []
            all_events: Dict[str, List[Dict[str, Any]]] = {"touched": [], "entry": [], "status": []}

            for s in setups:
                ns, events = process_one_setup(s, ltp_map, historical_mode=historical_mode)
                if ns.get("symbol") in ltp_map:
                    ns["ltp"] = round(float(ltp_map[ns["symbol"]]), 2)
                updated.append(ns)
                for ev in events:
                    kind = ev.pop("_event_type", "status")
                    if kind == "touched":
                        all_events["touched"].append(ev)
                    elif kind == "entry":
                        all_events["entry"].append(ev)
                    else:
                        all_events["status"].append(ev)

            save_setups(updated)
            send_status_boards(all_events)

            if historical_mode and HISTORICAL_RUN_ONCE:
                log("Historical mode run completed once; exiting")
                return

        except Exception as e:
            log(f"Main loop error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

"""
npattern_upstox_final_v4_auto_expiry.py

Final version with:
- nearest expiry auto-detection from Upstox option contracts
- 5-strike OI table
- 1 stock per image
- state-aware JSON processing
"""

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

UPSTOX_ACCESS_TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or "").strip()
JSON_FILE = os.getenv("JSON_FILE", "npattern_selected.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
ENTRY_ZONE_PERCENT = float(os.getenv("ENTRY_ZONE_PERCENT", "2.0"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", str(6 * 60 * 60)))
STRONG_ZONE_PCT = float(os.getenv("STRONG_ZONE_PCT", "0.5"))
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").strip().lower() == "true"
USE_STRONG_ZONE_ALERTS = os.getenv("USE_STRONG_ZONE_ALERTS", "true").strip().lower() == "true"
SEND_REAL_STARTUP_SAMPLE = os.getenv("SEND_REAL_STARTUP_SAMPLE", "false").strip().lower() == "true"
REAL_STARTUP_SAMPLE_SENT_FILE = os.getenv("REAL_STARTUP_SAMPLE_SENT_FILE", ".real_startup_sample_sent")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "0.8"))
HA_BODY_PERCENT = float(os.getenv("HA_BODY_PERCENT", "30"))
ENTRY_DISTANCE_PERCENT = float(os.getenv("ENTRY_DISTANCE_PERCENT", "0.5"))
ENTRY_BUFFER_PERCENT = float(os.getenv("ENTRY_BUFFER_PERCENT", "0.1"))
REQUIRE_OI_CONFIRM = os.getenv("REQUIRE_OI_CONFIRM", "true").strip().lower() == "true"
STRONG_ZONE_REQUIRED = os.getenv("STRONG_ZONE_REQUIRED", "false").strip().lower() == "true"
IMAGE_WIDTH = int(os.getenv("IMAGE_WIDTH", "1300"))
IMAGE_HEIGHT = int(os.getenv("IMAGE_HEIGHT", "1100"))

TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

MARKET_START = (9, 15)
MARKET_END = (15, 30)

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
}

INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

_INSTRUMENT_MAP: Dict[str, Dict[str, Any]] = {}
_EXPIRY_CACHE: Dict[str, Dict[str, str]] = {}


def now_ist() -> datetime:
    return datetime.now(IST)


def log(msg: str) -> None:
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] {msg}", flush=True)


def short_symbol(symbol: str) -> str:
    s = str(symbol or "").replace("NSE:", "").replace("BSE:", "")
    for suf in ["-EQ", "-BE", "-BZ", "-BL", "-SM", "-INDEX"]:
        s = s.replace(suf, "")
    return s.strip()


def ensure_float(x: Any, default: float = 0.0) -> float:
    try:
        if x in (None, ""):
            return default
        return float(x)
    except Exception:
        return default


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
    return f"{sign}{n:.0f}"


def get_current_d_from_levels(setup: Dict[str, Any]) -> Tuple[float, str]:
    d_levels = setup.get("d_levels") or []
    current_idx = int(ensure_float(setup.get("current_d_index"), 0))
    if isinstance(d_levels, list) and d_levels:
        if current_idx < 0:
            current_idx = 0
        if current_idx >= len(d_levels):
            current_idx = len(d_levels) - 1
        item = d_levels[current_idx] or {}
        value = ensure_float(item.get("value"), 0.0)
        label = str(item.get("level") or setup.get("which_extension_active") or "Active D")
        if value > 0:
            return value, label
    active_d = ensure_float(setup.get("active_d"), 0.0)
    if active_d > 0:
        return active_d, str(setup.get("which_extension_active") or "Active D")
    return 0.0, ""


def in_market_hours() -> bool:
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
        if in_market_hours():
            return
        n = now_ist()
        if n.weekday() >= 5:
            log("Weekend; waiting for next market session")
            time.sleep(600)
        else:
            start = n.replace(hour=MARKET_START[0], minute=MARKET_START[1], second=0, microsecond=0)
            if n < start:
                mins = int((start - n).total_seconds() // 60)
                log(f"Waiting for market start; {mins} min left")
                time.sleep(min(300, max(30, mins * 30)))
            else:
                time.sleep(60)


def _load_font(size: int, bold: bool = False):
    from PIL import ImageFont
    import os

    font_name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    fallback_name = "LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"

    base_paths = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__)),
    ]

    candidates = []
    for base in base_paths:
        candidates.append(os.path.join(base, "fonts", font_name))
        candidates.append(os.path.join(base, "fonts", fallback_name))
        candidates.append(os.path.join(base, font_name))
        candidates.append(os.path.join(base, fallback_name))

    candidates += [
        f"/usr/share/fonts/truetype/dejavu/{font_name}",
        f"/usr/share/fonts/truetype/liberation2/{fallback_name}",
    ]

    for path in candidates:
        try:
            if os.path.exists(path):
                print(f"[FONT LOADED] {path}")
                return ImageFont.truetype(path, size)
        except Exception as e:
            print(f"[FONT SKIP] {path} | {e}")

    print("[FONT ERROR] Using default font")
    return ImageFont.load_default()


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
        files = {"photo": ("npattern_alert.png", image_bytes, "image/png")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
        resp = requests.post(url, files=files, data=data, timeout=max(HTTP_TIMEOUT, 30))
        resp.raise_for_status()
    except Exception as e:
        log(f"Telegram photo failed: {e}")
        send_telegram_text(caption)


def upstox_get(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            log(f"GET retry {attempt}/{MAX_RETRIES} failed: {url} | {e}")
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
    symbol_clean = short_symbol(symbol).upper()
    rec = load_instrument_master().get(symbol_clean)
    if rec:
        return str(rec.get("instrument_key", "")).strip()
    return None


def load_setups() -> List[Dict[str, Any]]:
    if not os.path.exists(JSON_FILE):
        log(f"JSON file not found: {JSON_FILE}")
        return []
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_setups(items: List[Dict[str, Any]]) -> None:
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


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
    if bool(setup.get("entry_found")) or status in {"entry_found", "open"}:
        return False
    return True


def batch_fetch_ltps(setups: List[Dict[str, Any]]) -> Dict[str, float]:
    key_to_symbol: Dict[str, str] = {}
    keys: List[str] = []
    for s in setups:
        sym = str(s.get("symbol") or "")
        key = symbol_to_instrument_key(sym)
        if key:
            keys.append(key)
            key_to_symbol[key] = sym
        else:
            log(f"Instrument key missing for {sym}")
    if not keys:
        return {}

    joined = ",".join(sorted(set(keys)))
    try:
        data = upstox_get("https://api.upstox.com/v3/market-quote/ltp", params={"instrument_key": joined})
    except Exception as e:
        log(f"Batch LTP failed: {e}")
        return {}

    out: Dict[str, float] = {}
    payload = data.get("data", {}) if isinstance(data, dict) else {}
    for key, row in payload.items():
        sym = key_to_symbol.get(key)
        if not sym:
            continue
        ltp = row.get("last_price")
        if ltp is None:
            ltp = row.get("ltp")
        try:
            out[sym] = float(ltp)
        except Exception:
            pass
    return out


def fetch_history_15m(instrument_key: str) -> Optional[pd.DataFrame]:
    to_date = now_ist().strftime("%Y-%m-%d")
    from_date = (now_ist() - timedelta(days=5)).strftime("%Y-%m-%d")
    urls = [
        f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/15/{to_date}/{from_date}",
        f"https://api.upstox.com/v2/historical-candle/{instrument_key}/15minute/{to_date}/{from_date}",
    ]
    for url in urls:
        try:
            data = upstox_get(url)
            candles = ((data.get("data") or {}).get("candles")) or data.get("candles") or []
            if not candles:
                continue
            cols = ["ts", "o", "h", "l", "c", "v"]
            df = pd.DataFrame(candles, columns=cols[:len(candles[0])])
            rename = {}
            for old, new in zip(df.columns[:6], ["ts", "o", "h", "l", "c", "v"][:len(df.columns[:6])]):
                rename[old] = new
            df = df.rename(columns=rename)
            if "ts" not in df.columns or not {"o", "h", "l", "c"}.issubset(df.columns):
                continue
            df["datetime"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            return df[["datetime", "o", "h", "l", "c"]].dropna().reset_index(drop=True)
        except Exception:
            continue
    return None


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


def is_doji(row: pd.Series, max_body_ratio: Optional[float] = None) -> bool:
    rng = float(row["ha_high"] - row["ha_low"])
    if rng <= 0:
        return False
    body = abs(float(row["ha_close"] - row["ha_open"]))
    ratio_limit = (HA_BODY_PERCENT / 100.0) if max_body_ratio is None else max_body_ratio
    return (body / rng) <= ratio_limit



def distance_percent(price_a: float, price_b: float) -> float:
    if price_b == 0:
        return 999.0
    return abs(price_a - price_b) / abs(price_b) * 100.0


def oi_filter_allows(pattern: str, oi_bias: str) -> bool:
    if not REQUIRE_OI_CONFIRM:
        return True
    p = str(pattern or "").strip().lower()
    b = str(oi_bias or "").strip().lower()
    if p == "bullish":
        return "bullish" in b
    if p == "bearish":
        return "bearish" in b
    return True


def entry_filter_allows(setup: Dict[str, Any], ltp: float, d_price: float, oi_bias: str) -> Tuple[bool, str]:
    dist_pct = distance_percent(ltp, d_price)
    if dist_pct > ENTRY_DISTANCE_PERCENT:
        return False, f"Distance too far ({dist_pct:.2f}% > {ENTRY_DISTANCE_PERCENT:.2f}%)"

    if STRONG_ZONE_REQUIRED:
        strong_zone_count = int(ensure_float(setup.get("strong_zone_count"), 0))
        if strong_zone_count < 2:
            return False, "Strong zone required"

    if not oi_filter_allows(str(setup.get("pattern", "")), oi_bias):
        return False, f"OI mismatch ({oi_bias})"

    return True, "OK"


def is_open_low_pct(open_p: float, low_p: float) -> bool:
    if low_p == 0:
        return False
    diff_pct = abs(open_p - low_p) / abs(low_p) * 100.0
    return diff_pct <= ENTRY_BUFFER_PERCENT

def is_open_high_pct(open_p: float, high_p: float) -> bool:
    if high_p == 0:
        return False
    diff_pct = abs(open_p - high_p) / abs(high_p) * 100.0
    return diff_pct <= ENTRY_BUFFER_PERCENT

def check_entry_after_touch(df: pd.DataFrame, d_price: float, pattern: str, max_entry_percent: float = 2.0) -> Tuple[Optional[float], Optional[str], str]:
    if df is None or df.empty or len(df) < 2:
        return None, None, "No candles"
    ha = to_heikin_ashi(df)

    for i in range(len(ha) - 1):
        first = ha.iloc[i]
        second = ha.iloc[i + 1]

        if pattern.lower() == "bullish":
            nearest_scan_price = min(float(first["ha_open"]), float(first["ha_close"]), float(first["ha_low"]))
        else:
            nearest_scan_price = max(float(first["ha_open"]), float(first["ha_close"]), float(first["ha_high"]))

        if abs(nearest_scan_price - d_price) > abs(d_price) * (ENTRY_DISTANCE_PERCENT / 100.0):
            return None, None, f"Moved beyond {ENTRY_DISTANCE_PERCENT:.2f}% from D"

        # Keep body/doji condition
        if not is_doji(first):
            continue

        # Add open=low / open=high percentage condition
        if pattern.lower() == "bullish":
            confirm_ok = is_open_low_pct(float(second["ha_open"]), float(second["ha_low"]))
            if confirm_ok:
                return float(second["ha_high"]), str(second["datetime"]), f"Bullish doji + open=low within {ENTRY_BUFFER_PERCENT:.2f}%"
        else:
            confirm_ok = is_open_high_pct(float(second["ha_open"]), float(second["ha_high"]))
            if confirm_ok:
                return float(second["ha_low"]), str(second["datetime"]), f"Bearish doji + open=high within {ENTRY_BUFFER_PERCENT:.2f}%"

    return None, None, "No valid entry"


def get_nearest_expiry(instrument_key: str) -> Optional[str]:
    try:
        data = upstox_get("https://api.upstox.com/v2/option/contract", params={"instrument_key": instrument_key})
        rows = data.get("data", []) if isinstance(data, dict) else []
        expiries = set()
        today = now_ist().date()

        for row in rows:
            exp = str(row.get("expiry") or "").strip()
            if not exp:
                continue
            try:
                exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            except Exception:
                continue
            if exp_date >= today:
                expiries.add(exp_date)

        if not expiries:
            return None
        nearest = min(expiries)
        return nearest.strftime("%Y-%m-%d")
    except Exception as e:
        log(f"Nearest expiry fetch failed for {instrument_key}: {e}")
        return None


def get_cached_nearest_expiry(instrument_key: str) -> Optional[str]:
    today = now_ist().strftime("%Y-%m-%d")
    cached = _EXPIRY_CACHE.get(instrument_key)
    if cached and cached.get("asof") == today:
        return cached.get("expiry")
    expiry = get_nearest_expiry(instrument_key)
    if expiry:
        _EXPIRY_CACHE[instrument_key] = {"asof": today, "expiry": expiry}
    return expiry


def get_option_chain_5_strikes(setup: Dict[str, Any], instrument_key: str, ltp: float) -> Tuple[List[Dict[str, Any]], str]:
    expiry = get_cached_nearest_expiry(instrument_key)
    if not expiry:
        return [], "NA"

    try:
        data = upstox_get("https://api.upstox.com/v2/option/chain", params={"instrument_key": instrument_key, "expiry_date": expiry})
    except Exception as e:
        log(f"Option chain failed for {setup.get('symbol')}: {e}")
        return [], "NA"

    rows = data.get("data", []) if isinstance(data, dict) else []
    if not rows:
        return [], "NA"

    normalized: List[Dict[str, Any]] = []
    for x in rows:
        strike = ensure_float(x.get("strike_price"))
        ce = x.get("call_options", {}) or {}
        pe = x.get("put_options", {}) or {}
        if strike <= 0:
            continue
        ce_md = ce.get("market_data", {}) or {}
        pe_md = pe.get("market_data", {}) or {}
        ce_oi = ensure_float(ce_md.get("oi"))
        pe_oi = ensure_float(pe_md.get("oi"))
        ce_prev = ensure_float(ce_md.get("prev_oi"))
        pe_prev = ensure_float(pe_md.get("prev_oi"))
        normalized.append({
            "strike": strike,
            "ce_oi": ce_oi,
            "ce_chg": ce_oi - ce_prev,
            "pe_oi": pe_oi,
            "pe_chg": pe_oi - pe_prev,
            "ce_ltp": ensure_float(ce_md.get("ltp")),
            "pe_ltp": ensure_float(pe_md.get("ltp")),
            "ce_vol": ensure_float(ce_md.get("volume")),
            "pe_vol": ensure_float(pe_md.get("volume")),
        })

    normalized = sorted(normalized, key=lambda r: r["strike"])
    if not normalized:
        return [], "NA"

    atm_index = min(range(len(normalized)), key=lambda i: abs(normalized[i]["strike"] - ltp))
    start = max(0, atm_index - 2)
    end = min(len(normalized), atm_index + 3)
    view = normalized[start:end]

    ce_sum = sum(r["ce_chg"] for r in view)
    pe_sum = sum(r["pe_chg"] for r in view)
    bias = "Bullish" if pe_sum > ce_sum else "Bearish" if ce_sum > pe_sum else "Neutral"
    return view, bias


def find_strong_reversal_zones(setups: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, List[float]] = {}
    for s in setups:
        d = ensure_float(s.get("active_d") or s.get("fib_d") or 0.0)
        if d <= 0:
            continue
        grouped.setdefault(str(s.get("symbol") or ""), []).append(d)

    zones: Dict[str, Dict[str, Any]] = {}
    for sym, vals in grouped.items():
        vals = sorted(vals)
        if len(vals) < 2:
            continue
        clusters: List[List[float]] = []
        current = [vals[0]]
        for v in vals[1:]:
            if abs(v - current[-1]) <= current[-1] * (STRONG_ZONE_PCT / 100.0):
                current.append(v)
            else:
                if len(current) >= 2:
                    clusters.append(current)
                current = [v]
        if len(current) >= 2:
            clusters.append(current)
        if not clusters:
            continue
        best = max(clusters, key=len)
        zones[sym] = {"low": min(best), "high": max(best), "count": len(best)}
    return zones


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


def confidence_score(setup: Dict[str, Any], ltp: float, bias: str) -> int:
    score = 50
    current_d_value, _ = get_current_d_from_levels(setup)
    d_price = ensure_float(setup.get("touched_d_price") or current_d_value or setup.get("current_fib_d") or setup.get("fib_d"))
    if d_price > 0:
        diff_pct = abs(ltp - d_price) / d_price * 100.0
        if diff_pct <= 0.25:
            score += 20
        elif diff_pct <= 0.50:
            score += 12
        elif diff_pct <= ENTRY_ZONE_PERCENT:
            score += 6
    p = str(setup.get("pattern", "")).lower()
    b = bias.lower()
    if p == "bullish" and "bullish" in b:
        score += 15
    if p == "bearish" and "bearish" in b:
        score += 15
    if str(setup.get("which_extension_active") or "").lower().startswith("1st"):
        score += 5
    return max(1, min(99, int(score)))


def build_stock_alert_image(setup: Dict[str, Any], ltp: float, entry_price: float, entry_time: str, oi_rows: List[Dict[str, Any]], oi_bias: str) -> bytes:
    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (242, 245, 249))
    draw = ImageDraw.Draw(img)

    black = (18, 18, 18)
    dark = (44, 62, 80)
    blue = (41, 98, 255)
    blue_soft = (232, 240, 255)
    green = (22, 163, 74)
    green_soft = (230, 244, 234)
    red = (220, 38, 38)
    red_soft = (254, 242, 242)
    orange = (234, 88, 12)
    orange_soft = (255, 237, 213)
    white = (255, 255, 255)
    border = (220, 226, 232)
    head_fill = (240, 244, 248)
    gray = (99, 115, 129)

    f_title = _load_font(54, True)
    f_sub = _load_font(26, False)
    f_head = _load_font(28, True)
    f_body = _load_font(28, False)
    f_small = _load_font(22, False)
    f_tbl_h = _load_font(24, True)
    f_tbl_b = _load_font(23, False)

    # Background panels
    draw.rounded_rectangle((24, 18, IMAGE_WIDTH - 24, 110), radius=26, fill=white, outline=border, width=2)
    draw.rounded_rectangle((24, 128, IMAGE_WIDTH - 24, 520), radius=26, fill=white, outline=border, width=2)
    draw.rounded_rectangle((24, 540, IMAGE_WIDTH - 24, IMAGE_HEIGHT - 24), radius=26, fill=white, outline=border, width=2)

    # Header
    draw.text((46, 30), "N PATTERN ENTRY ALERT", font=f_title, fill=blue)
    stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
    sb = draw.textbbox((0, 0), stamp, font=f_sub)
    draw.text((IMAGE_WIDTH - 46 - (sb[2] - sb[0]), 42), stamp, font=f_sub, fill=dark)

    # Top stat pills
    pills = [
        ("Stock", short_symbol(setup.get("symbol")), blue_soft, blue),
        ("Pattern", str(setup.get("pattern", "")), blue_soft, blue),
        ("Status", str(setup.get("status", "")), green_soft if str(setup.get("status","")).lower()=="entry_found" else orange_soft, green if str(setup.get("status","")).lower()=="entry_found" else orange),
    ]
    px = 42
    py = 128 + 18
    for label, value, fill, color in pills:
        txt = f"{label}: {value}"
        bb = draw.textbbox((0, 0), txt, font=f_small)
        w = (bb[2] - bb[0]) + 34
        draw.rounded_rectangle((px, py, px + w, py + 42), radius=18, fill=fill, outline=fill, width=1)
        draw.text((px + 16, py + 8), txt, font=f_small, fill=color)
        px += w + 14

    strong_zone_text = "N/A"
    strong_zone_low = ensure_float(setup.get("strong_zone_low"), 0.0)
    strong_zone_high = ensure_float(setup.get("strong_zone_high"), 0.0)
    strong_zone_count = int(ensure_float(setup.get("strong_zone_count"), 0))
    if strong_zone_low > 0 and strong_zone_high > 0 and strong_zone_count >= 2:
        strong_zone_text = f"{strong_zone_low:.2f} - {strong_zone_high:.2f} ({strong_zone_count})"

    left = [
        ("D Source", str(setup.get("touched_d_source") or setup.get("which_extension_active") or "Active D")),
        ("Touched D", f"{ensure_float(setup.get('touched_d_price') or setup.get('active_d')):.2f}"),
        ("Entry", f"{entry_price:.2f}"),
        ("LTP", f"{ltp:.2f}"),
        ("Entry Time", str(entry_time).replace("T", " ")[:19]),
    ]
    sl_price = ensure_float(setup.get("sl_price"))
    target_price = ensure_float(setup.get("target_price"))
    conf = int(setup.get("confidence_score", 0))
    right = [
        ("SL", f"{sl_price:.2f}" if sl_price > 0 else "-"),
        ("Target", f"{target_price:.2f}" if target_price > 0 else "-"),
        ("OI Bias", oi_bias),
        ("Confidence", f"{conf}%"),
        ("Strong Zone", strong_zone_text),
    ]

    # Info columns
    y = 128 + 86
    for lab, val in left:
        draw.text((52, y), f"{lab}:", font=f_head, fill=dark)
        col = green if lab == "Entry" else black
        draw.text((280, y), val, font=f_body, fill=col)
        y += 58

    y = 128 + 86
    for lab, val in right:
        if lab == "SL":
            color = red
        elif lab == "Target":
            color = green
        elif lab == "OI Bias":
            color = green if "Bullish" in val else red if "Bearish" in val else dark
        elif lab == "Strong Zone" and strong_zone_text != "-":
            color = orange
        elif lab == "Confidence":
            color = blue
        else:
            color = black
        draw.text((740, y), f"{lab}:", font=f_head, fill=dark)
        draw.text((980, y), val, font=f_body, fill=color)
        y += 58

    # OI title
    draw.text((48, 560), "OI TABLE (5 STRIKES)", font=f_head, fill=dark)
    subtitle = "1 stock per image • CE/PE change highlights • ATM-centered"
    draw.text((48, 596), subtitle, font=f_small, fill=gray)

    headers = ["Strike", "CE LTP", "CE OI", "CE Chg", "PE LTP", "PE OI", "PE Chg"]
    widths = [155, 170, 185, 170, 170, 185, 170]
    xs = [48]
    for w in widths:
        xs.append(xs[-1] + w)
    row_h = 70
    y0 = 640

    for j, h in enumerate(headers):
        draw.rounded_rectangle((xs[j], y0, xs[j + 1], y0 + row_h), radius=10, fill=head_fill, outline=border, width=1)
        bbox = draw.textbbox((0, 0), h, font=f_tbl_h)
        tw = bbox[2] - bbox[0]
        draw.text((xs[j] + (widths[j] - tw) / 2, y0 + 20), h, font=f_tbl_h, fill=blue)

    base_y = y0 + row_h + 8
    for i, r in enumerate(oi_rows[:5]):
        y1 = base_y + i * row_h
        y2 = y1 + row_h - 6
        vals = [
            f"{ensure_float(r.get('strike')):.0f}",
            f"{ensure_float(r.get('ce_ltp')):.2f}",
            compact_num(r.get("ce_oi")),
            compact_num(r.get("ce_chg")),
            f"{ensure_float(r.get('pe_ltp')):.2f}",
            compact_num(r.get("pe_oi")),
            compact_num(r.get("pe_chg")),
        ]
        colors = [
            black,
            black,
            black,
            green if ensure_float(r.get("ce_chg")) > 0 else red if ensure_float(r.get("ce_chg")) < 0 else dark,
            black,
            black,
            green if ensure_float(r.get("pe_chg")) > 0 else red if ensure_float(r.get("pe_chg")) < 0 else dark,
        ]
        fill = (250, 252, 255) if i % 2 == 0 else white
        for j, v in enumerate(vals):
            draw.rounded_rectangle((xs[j], y1, xs[j + 1], y2), radius=8, fill=fill, outline=border, width=1)
            bbox = draw.textbbox((0, 0), v, font=f_tbl_b)
            tw = bbox[2] - bbox[0]
            draw.text((xs[j] + (widths[j] - tw) / 2, y1 + 18), v, font=f_tbl_b, fill=colors[j])

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


def detect_touch(setup: Dict[str, Any], ltp: float) -> Tuple[Optional[float], str]:
    current_d_value, current_d_label = get_current_d_from_levels(setup)
    fib_d = ensure_float(setup.get("current_fib_d") or setup.get("fib_d"))
    trend_d = ensure_float(setup.get("trend_d"))

    if current_d_value > 0:
        allowed = abs(current_d_value) * (ENTRY_ZONE_PERCENT / 100.0)
        if abs(ltp - current_d_value) <= allowed:
            return current_d_value, current_d_label or "Active D"

    if fib_d:
        allowed = abs(fib_d) * (ENTRY_ZONE_PERCENT / 100.0)
        if abs(ltp - fib_d) <= allowed:
            return fib_d, "Fib"

    if trend_d:
        allowed = abs(trend_d) * (ENTRY_ZONE_PERCENT / 100.0)
        if abs(ltp - trend_d) <= allowed:
            return trend_d, "Trend"

    return None, ""


def process_setup(setup: Dict[str, Any], ltp: float) -> Dict[str, Any]:
    setup["last_checked_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    if not should_process_setup(setup):
        return setup
    if setup.get("alert_sent") and in_cooldown(setup):
        return setup

    d_price, source = detect_touch(setup, ltp)
    if d_price is None:
        setup["last_debug_reason"] = f"No D touch | LTP={ltp}"
        return setup

    setup["touched_d_source"] = source
    setup["touched_d_price"] = round(float(d_price), 2)
    setup["active_d"] = round(float(d_price), 2)

    instrument_key = symbol_to_instrument_key(str(setup.get("symbol") or ""))
    if not instrument_key:
        setup["last_debug_reason"] = "Instrument key missing"
        return setup

    df = fetch_history_15m(instrument_key)
    entry_price, entry_time, reason = check_entry_after_touch(df, d_price, str(setup.get("pattern", "")), ENTRY_ZONE_PERCENT)
    setup["last_debug_reason"] = reason

    if not entry_price or not entry_time:
        setup["status"] = "touched"
        if is_completed_without_entry(setup):
            setup["status"] = "completed_without_entry"
        return setup

    oi_rows, oi_bias = get_option_chain_5_strikes(setup, instrument_key, ltp)
    setup["oi_bias"] = oi_bias

    allowed_entry, filter_reason = entry_filter_allows(setup, float(ltp), float(d_price), oi_bias)
    if not allowed_entry:
        setup["status"] = "touched"
        setup["last_debug_reason"] = filter_reason
        return setup

    setup["entry_found"] = True
    setup["alert_sent"] = True
    setup["entry_price"] = round(float(entry_price), 2)
    setup["entry_time"] = entry_time
    setup["ltp_at_alert"] = round(float(ltp), 2)
    setup["last_alert_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    setup["confidence_score"] = confidence_score(setup, float(ltp), oi_bias)
    setup["status"] = "entry_found"

    if str(setup.get("pattern", "")).lower() == "bullish":
        setup["sl_price"] = round(float(d_price), 2)
        setup["target_price"] = round(float(entry_price) * 1.02, 2)
    else:
        setup["sl_price"] = round(float(d_price), 2)
        setup["target_price"] = round(float(entry_price) * 0.98, 2)

    setup["target_hit"] = False
    setup["sl_hit"] = False

    caption = (
        f"🔥 N PATTERN ENTRY\n"
        f"Stock: {short_symbol(setup.get('symbol'))}\n"
        f"Pattern: {setup.get('pattern', '')}\n"
        f"D: {source} ({d_price:.2f})\n"
        f"Entry: {entry_price:.2f}\n"
        f"SL: {ensure_float(setup.get('sl_price')):.2f}\n"
        f"Target: {ensure_float(setup.get('target_price')):.2f}\n"
        f"OI Bias: {oi_bias}\n"
        f"Confidence: {int(setup.get('confidence_score', 0))}%\n"
        f"HA Body%: {HA_BODY_PERCENT:.0f} | Dist%: {ENTRY_DISTANCE_PERCENT:.2f}"
    )
    image_bytes = build_stock_alert_image(setup, ltp, float(entry_price), entry_time, oi_rows, oi_bias)
    send_telegram_photo(image_bytes, caption)
    return setup


def track_open_positions(setups: List[Dict[str, Any]], ltp_map: Dict[str, float]) -> List[Dict[str, Any]]:
    updated = []
    for s in setups:
        if is_completed_without_entry(s):
            s["status"] = "completed_without_entry"
            updated.append(s)
            continue
        if not s.get("entry_found") or str(s.get("status", "")) not in {"entry_found", "open"}:
            updated.append(s)
            continue
        if s.get("target_hit") or s.get("sl_hit"):
            updated.append(s)
            continue

        symbol = str(s.get("symbol") or "")
        ltp = ltp_map.get(symbol)
        if ltp is None:
            updated.append(s)
            continue

        pattern = str(s.get("pattern", "")).lower()
        sl_price = ensure_float(s.get("sl_price"))
        target_price = ensure_float(s.get("target_price"))
        hit_text = None

        if pattern == "bullish":
            if ltp >= target_price > 0:
                s["target_hit"] = True
                s["status"] = "target_hit"
                hit_text = f"✅ TARGET HIT\n{short_symbol(symbol)}\nLTP: {ltp:.2f}\nTarget: {target_price:.2f}"
            elif ltp <= sl_price and sl_price > 0:
                s["sl_hit"] = True
                s["status"] = "sl_hit"
                hit_text = f"❌ STOPLOSS HIT\n{short_symbol(symbol)}\nLTP: {ltp:.2f}\nSL: {sl_price:.2f}"
        else:
            if ltp <= target_price and target_price > 0:
                s["target_hit"] = True
                s["status"] = "target_hit"
                hit_text = f"✅ TARGET HIT\n{short_symbol(symbol)}\nLTP: {ltp:.2f}\nTarget: {target_price:.2f}"
            elif ltp >= sl_price > 0:
                s["sl_hit"] = True
                s["status"] = "sl_hit"
                hit_text = f"❌ STOPLOSS HIT\n{short_symbol(symbol)}\nLTP: {ltp:.2f}\nSL: {sl_price:.2f}"

        if hit_text:
            s["last_alert_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
            send_telegram_text(hit_text)
        updated.append(s)
    return updated



def send_real_startup_sample_once() -> None:
    if not SEND_REAL_STARTUP_SAMPLE:
        return
    if os.path.exists(REAL_STARTUP_SAMPLE_SENT_FILE):
        return
    try:
        setups = load_setups()
        if not setups:
            log("No setups for real startup sample")
            return

        # prefer active setups first
        chosen = None
        preferred_status = {"waiting", "touched", "shifted_to_next_d", "entry_found"}
        for s in setups:
            st = str(s.get("status") or "").strip().lower()
            if st in preferred_status:
                chosen = s
                break
        if chosen is None:
            chosen = setups[0]

        symbol = str(chosen.get("symbol") or "")
        instrument_key = symbol_to_instrument_key(symbol)
        if not instrument_key:
            log(f"Real startup sample skipped: instrument key missing for {symbol}")
            return

        ltps = batch_fetch_ltps([chosen])
        ltp = ensure_float(ltps.get(symbol), ensure_float(chosen.get("ltp"), 0.0))
        if ltp <= 0:
            log(f"Real startup sample skipped: LTP missing for {symbol}")
            return

        entry_price = ensure_float(chosen.get("entry_price"), 0.0)
        if entry_price <= 0:
            entry_price = ltp

        entry_time = str(chosen.get("entry_time") or now_ist().strftime("%Y-%m-%d %H:%M:%S"))
        oi_rows, oi_bias = get_option_chain_5_strikes(chosen, instrument_key, ltp)

        zones = find_strong_reversal_zones(setups)
        z = zones.get(symbol)
        if z:
            chosen["strong_zone_low"] = round(float(z["low"]), 2)
            chosen["strong_zone_high"] = round(float(z["high"]), 2)
            chosen["strong_zone_count"] = int(z["count"])

        if ensure_float(chosen.get("sl_price"), 0.0) <= 0:
            chosen["sl_price"] = round(ensure_float(chosen.get("active_d") or chosen.get("touched_d_price")), 2)
        if ensure_float(chosen.get("target_price"), 0.0) <= 0:
            if str(chosen.get("pattern", "")).lower() == "bullish":
                chosen["target_price"] = round(entry_price * 1.02, 2)
            else:
                chosen["target_price"] = round(entry_price * 0.98, 2)

        if ensure_float(chosen.get("confidence_score"), 0.0) <= 0:
            chosen["confidence_score"] = confidence_score(chosen, ltp, oi_bias)

        image_bytes = build_stock_alert_image(chosen, ltp, entry_price, entry_time, oi_rows, oi_bias)
        caption = (
            f"🧪 REAL STARTUP SAMPLE\n"
            f"Stock: {short_symbol(symbol)}\n"
            f"Status: {chosen.get('status', '')}\n"
            f"LTP: {ltp:.2f}"
        )
        send_telegram_photo(image_bytes, caption)

        with open(REAL_STARTUP_SAMPLE_SENT_FILE, "w", encoding="utf-8") as f:
            f.write(now_ist().strftime("%Y-%m-%d %H:%M:%S"))
        log(f"Real startup sample sent for {symbol}")
    except Exception as e:
        log(f"Real startup sample failed: {e}")


def main() -> None:
    if not UPSTOX_ACCESS_TOKEN:
        raise RuntimeError("Missing UPSTOX_ACCESS_TOKEN")
    log("N Pattern Upstox Final V4 Auto Expiry started")
    try:
        print("WORKING DIR:", os.getcwd())
        print("FILES:", os.listdir())
        print("FONTS:", os.listdir("fonts") if os.path.exists("fonts") else "NO FONTS DIR")
    except Exception as e:
        print("FONT DEBUG ERROR:", e)
    send_real_startup_sample_once()
    log(f"Smart filters | HA_BODY_PERCENT={HA_BODY_PERCENT} | ENTRY_DISTANCE_PERCENT={ENTRY_DISTANCE_PERCENT} | ENTRY_BUFFER_PERCENT={ENTRY_BUFFER_PERCENT} | REQUIRE_OI_CONFIRM={REQUIRE_OI_CONFIRM} | STRONG_ZONE_REQUIRED={STRONG_ZONE_REQUIRED}")
    load_instrument_master()

    sent_zone_keys: set = set()

    while True:
        try:
            if ONLY_MARKET_HOURS and not in_market_hours():
                wait_until_market_start()
                continue

            setups = load_setups()
            if not setups:
                log("No setups found")
                time.sleep(max(POLL_SECONDS, 30))
                continue

            for s in setups:
                if is_completed_without_entry(s):
                    s["status"] = "completed_without_entry"

            zones = find_strong_reversal_zones(setups)
            for s in setups:
                z = zones.get(str(s.get("symbol") or ""))
                if z:
                    s["strong_zone_low"] = round(float(z["low"]), 2)
                    s["strong_zone_high"] = round(float(z["high"]), 2)
                    s["strong_zone_count"] = int(z["count"])
                else:
                    s["strong_zone_low"] = None
                    s["strong_zone_high"] = None
                    s["strong_zone_count"] = 0

            if USE_STRONG_ZONE_ALERTS:
                for sym, z in zones.items():
                    key = f"{sym}|{z['low']:.2f}|{z['high']:.2f}|{z['count']}"
                    if key not in sent_zone_keys:
                        msg = (
                            f"🔥 STRONG REVERSAL ZONE\n"
                            f"Stock: {short_symbol(sym)}\n"
                            f"Zone: {z['low']:.2f} - {z['high']:.2f}\n"
                            f"Patterns: {z['count']}"
                        )
                        send_telegram_text(msg)
                        sent_zone_keys.add(key)

            ltp_map = batch_fetch_ltps(setups)

            updated = []
            for s in setups:
                sym = str(s.get("symbol") or "")
                ltp = ltp_map.get(sym)
                if ltp is not None:
                    s["ltp"] = round(float(ltp), 2)
                    s = process_setup(s, ltp)
                updated.append(s)
                time.sleep(RATE_LIMIT_SLEEP)

            updated = track_open_positions(updated, ltp_map)
            save_setups(updated)

        except Exception as e:
            log(f"Main loop error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

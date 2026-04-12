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
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "0.8"))
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
    paths = []
    if bold:
        paths += [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        ]
    else:
        paths += [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ]
    for p in paths:
        try:
            if os.path.exists(p):
                return ImageFont.truetype(p, size)
        except Exception:
            pass
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


def is_doji(row: pd.Series, max_body_ratio: float = 0.30) -> bool:
    rng = float(row["ha_high"] - row["ha_low"])
    if rng <= 0:
        return False
    body = abs(float(row["ha_close"] - row["ha_open"]))
    return (body / rng) <= max_body_ratio


def check_entry_after_touch(df: pd.DataFrame, d_price: float, pattern: str, max_entry_percent: float = 2.0) -> Tuple[Optional[float], Optional[str], str]:
    if df is None or df.empty or len(df) < 2:
        return None, None, "No candles"
    ha = to_heikin_ashi(df)
    allowed = abs(d_price) * (max_entry_percent / 100.0)

    for i in range(len(ha) - 1):
        first = ha.iloc[i]
        second = ha.iloc[i + 1]

        if pattern.lower() == "bullish":
            nearest_scan_price = min(float(first["ha_open"]), float(first["ha_close"]), float(first["ha_low"]))
        else:
            nearest_scan_price = max(float(first["ha_open"]), float(first["ha_close"]), float(first["ha_high"]))

        if abs(nearest_scan_price - d_price) > allowed:
            return None, None, f"Moved beyond {max_entry_percent}% from D"

        if not is_doji(first):
            continue

        if pattern.lower() == "bullish":
            confirm_ok = abs(float(second["ha_open"]) - float(second["ha_low"])) <= max(0.2, d_price * 0.0005)
            if confirm_ok:
                return float(second["ha_high"]), str(second["datetime"]), "Bullish doji + open=low confirmed"
        else:
            confirm_ok = abs(float(second["ha_open"]) - float(second["ha_high"])) <= max(0.2, d_price * 0.0005)
            if confirm_ok:
                return float(second["ha_low"]), str(second["datetime"]), "Bearish doji + open=high confirmed"

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
    d_price = ensure_float(setup.get("touched_d_price") or setup.get("active_d") or setup.get("fib_d"))
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
    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (245, 245, 245))
    draw = ImageDraw.Draw(img)
    black = (20, 20, 20)
    green = (28, 148, 82)
    red = (198, 58, 58)
    blue = (48, 97, 184)
    white = (255, 255, 255)
    border = (210, 210, 210)
    head_fill = (236, 240, 247)

    f_title = _load_font(40, True)
    f_head = _load_font(24, True)
    f_body = _load_font(24, False)
    f_tbl_h = _load_font(20, True)
    f_tbl_b = _load_font(19, False)

    draw.rounded_rectangle((24, 18, IMAGE_WIDTH - 24, 92), radius=22, fill=white, outline=border, width=2)
    draw.text((48, 36), "N PATTERN ENTRY ALERT", font=f_title, fill=blue)
    stamp = now_ist().strftime("%d-%m-%Y %H:%M IST")
    sb = draw.textbbox((0, 0), stamp, font=f_body)
    draw.text((IMAGE_WIDTH - 48 - (sb[2] - sb[0]), 40), stamp, font=f_body, fill=black)

    draw.rounded_rectangle((24, 112, IMAGE_WIDTH - 24, 430), radius=22, fill=white, outline=border, width=2)
    left = [
        ("Stock", short_symbol(setup.get("symbol"))),
        ("Pattern", str(setup.get("pattern", ""))),
        ("Status", str(setup.get("status", ""))),
        ("D Source", str(setup.get("touched_d_source") or setup.get("which_extension_active") or "Active D")),
        ("Touched D", f"{ensure_float(setup.get('touched_d_price') or setup.get('active_d')):.2f}"),
    ]
    sl_price = ensure_float(setup.get("sl_price"))
    target_price = ensure_float(setup.get("target_price"))
    conf = int(setup.get("confidence_score", 0))
    right = [
        ("Entry", f"{entry_price:.2f}"),
        ("LTP", f"{ltp:.2f}"),
        ("SL", f"{sl_price:.2f}" if sl_price > 0 else "-"),
        ("Target", f"{target_price:.2f}" if target_price > 0 else "-"),
        ("OI Bias", oi_bias),
        ("Confidence", f"{conf}%"),
    ]
    y = 138
    for lab, val in left:
        draw.text((52, y), f"{lab}:", font=f_head, fill=black)
        draw.text((260, y), val, font=f_body, fill=black)
        y += 52
    y = 138
    for lab, val in right:
        color = green if lab in {"Entry", "Target"} else red if lab == "SL" else green if ("Bullish" in val) else red if ("Bearish" in val) else black
        draw.text((700, y), f"{lab}:", font=f_head, fill=black)
        draw.text((915, y), val, font=f_body, fill=color)
        y += 44

    draw.rounded_rectangle((24, 454, IMAGE_WIDTH - 24, IMAGE_HEIGHT - 24), radius=22, fill=white, outline=border, width=2)
    draw.text((48, 474), "OI TABLE (5 STRIKES)", font=f_head, fill=black)

    headers = ["Strike", "CE LTP", "CE OI", "CE Chg", "PE LTP", "PE OI", "PE Chg"]
    widths = [150, 160, 180, 160, 160, 180, 160]
    xs = [48]
    for w in widths:
        xs.append(xs[-1] + w)
    row_h = 58
    y0 = 520
    for j, h in enumerate(headers):
        draw.rectangle((xs[j], y0, xs[j + 1], y0 + row_h), fill=head_fill, outline=border, width=1)
        bbox = draw.textbbox((0, 0), h, font=f_tbl_h)
        tw = bbox[2] - bbox[0]
        draw.text((xs[j] + (widths[j] - tw) / 2, y0 + 16), h, font=f_tbl_h, fill=blue)

    base_y = y0 + row_h
    for i, r in enumerate(oi_rows[:5]):
        y1 = base_y + i * row_h
        y2 = y1 + row_h
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
            green if ensure_float(r.get("ce_chg")) > 0 else red if ensure_float(r.get("ce_chg")) < 0 else black,
            black,
            black,
            green if ensure_float(r.get("pe_chg")) > 0 else red if ensure_float(r.get("pe_chg")) < 0 else black,
        ]
        fill = (252, 252, 252) if i % 2 == 0 else white
        for j, v in enumerate(vals):
            draw.rectangle((xs[j], y1, xs[j + 1], y2), fill=fill, outline=border, width=1)
            bbox = draw.textbbox((0, 0), v, font=f_tbl_b)
            tw = bbox[2] - bbox[0]
            draw.text((xs[j] + (widths[j] - tw) / 2, y1 + 17), v, font=f_tbl_b, fill=colors[j])

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


def detect_touch(setup: Dict[str, Any], ltp: float) -> Tuple[Optional[float], str]:
    active_d = ensure_float(setup.get("active_d"))
    fib_d = ensure_float(setup.get("fib_d"))
    trend_d = ensure_float(setup.get("trend_d"))
    if active_d:
        allowed = abs(active_d) * (ENTRY_ZONE_PERCENT / 100.0)
        if abs(ltp - active_d) <= allowed:
            return active_d, str(setup.get("which_extension_active") or "Active D")
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
    setup["entry_found"] = True
    setup["alert_sent"] = True
    setup["entry_price"] = round(float(entry_price), 2)
    setup["entry_time"] = entry_time
    setup["ltp_at_alert"] = round(float(ltp), 2)
    setup["last_alert_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    setup["confidence_score"] = confidence_score(setup, float(ltp), oi_bias)
    setup["status"] = "entry_found"

    if str(setup.get("pattern", "")).lower() == "bullish":
        setup["sl_price"] = round(ensure_float(setup.get("active_d") or d_price), 2)
        setup["target_price"] = round(float(entry_price) * 1.02, 2)
    else:
        setup["sl_price"] = round(ensure_float(setup.get("active_d") or d_price), 2)
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
        f"Confidence: {int(setup.get('confidence_score', 0))}%"
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

def send_startup_sample():
    setup = {
        "symbol": "NSE:HDFCLIFE-EQ",
        "pattern": "Bullish",
        "status": "entry_found",
        "touched_d_source": "Fib",
        "touched_d_price": 558.40,
        "active_d": 552.10,
        "sl_price": 552.10,
        "target_price": 571.46,
        "confidence_score": 82
    }

    ltp = 560.25
    entry_price = 560.25
    entry_time = now_ist().strftime("%Y-%m-%d %H:%M")

    # Dummy OI (sample)
    oi_rows = [
        {"strike":550,"ce_ltp":14.2,"ce_oi":4200,"ce_chg":-1200,"pe_ltp":4.1,"pe_oi":6300,"pe_chg":1800},
        {"strike":555,"ce_ltp":10.8,"ce_oi":5100,"ce_chg":-900,"pe_ltp":5.6,"pe_oi":7400,"pe_chg":2200},
        {"strike":560,"ce_ltp":7.4,"ce_oi":6800,"ce_chg":-450,"pe_ltp":7.9,"pe_oi":8900,"pe_chg":2600},
        {"strike":565,"ce_ltp":4.8,"ce_oi":3500,"ce_chg":200,"pe_ltp":11.7,"pe_oi":5600,"pe_chg":1500},
        {"strike":570,"ce_ltp":3.1,"ce_oi":2100,"ce_chg":600,"pe_ltp":16.4,"pe_oi":3900,"pe_chg":900},
    ]

    oi_bias = "Bullish"

    img = build_stock_alert_image(
        setup, ltp, entry_price, entry_time, oi_rows, oi_bias
    )

    caption = f"🔥 TEST ALERT\n{short_symbol(setup['symbol'])}\nEntry: {entry_price}"

    send_telegram_photo(img, caption)


def main() -> None:
    if not UPSTOX_ACCESS_TOKEN:
        raise RuntimeError("Missing UPSTOX_ACCESS_TOKEN")
    log("N Pattern Upstox Final V4 Auto Expiry started")
    load_instrument_master()
    log("N Pattern Upstox Final V4 Auto Expiry started")
    send_startup_sample()   # 👈 ADD THIS LINE  

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

            if USE_STRONG_ZONE_ALERTS:
                zones = find_strong_reversal_zones(setups)
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

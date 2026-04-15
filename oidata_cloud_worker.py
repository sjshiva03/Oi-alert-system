import os
import io
import json
import math
import time
import hashlib
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from PIL import Image, ImageDraw, ImageFont

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 15
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MIN = 30

# =========================
# ENV
# =========================

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


UPSTOX_ACCESS_TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or "").strip()
JSON_FILE = os.getenv("JSON_FILE", "npattern_selected.json")
JSON_SOURCE_URL = (os.getenv("JSON_SOURCE_URL") or "").strip()
REFRESH_JSON_EACH_CYCLE = env_bool("REFRESH_JSON_EACH_CYCLE", False)
POLL_SECONDS = env_int("POLL_SECONDS", 30)
HTTP_TIMEOUT = env_int("HTTP_TIMEOUT", 20)
MAX_RETRIES = env_int("MAX_RETRIES", 3)
DE_BUG = env_bool("DE_BUG", False)
ONLY_MARKET_HOURS = env_bool("ONLY_MARKET_HOURS", True)
AFTER_MARKET_MODE = env_bool("AFTER_MARKET_MODE", False)
HISTORICAL_RUN_ONCE = env_bool("HISTORICAL_RUN_ONCE", False)
PATTERN_TF_MINUTES = env_int("PATTERN_TF_MINUTES", 240)
ENTRY_TF_MINUTES = env_int("ENTRY_TF_MINUTES", 15)
ENTRY_LIMIT_PERCENT = env_float("ENTRY_LIMIT_PERCENT", env_float("ENTRY_DISTANCE_PERCENT", 2.0))
ENTRY_BUFFER_PERCENT = env_float("ENTRY_BUFFER_PERCENT", 0.2)
HA_BODY_PERCENT = env_float("HA_BODY_PERCENT", 30.0)
TARGET_PERCENT = env_float("TARGET_PERCENT", 2.0)
ALERT_COOLDOWN_SECONDS = env_int("ALERT_COOLDOWN_SECONDS", 21600)
LOOKBACK_DAYS_PATTERN = env_int("LOOKBACK_DAYS_PATTERN", 180)
LOOKBACK_DAYS_ENTRY = env_int("LOOKBACK_DAYS_ENTRY", 60)
IMAGE_WIDTH = env_int("IMAGE_WIDTH", 1900)
IMAGE_HEIGHT = env_int("IMAGE_HEIGHT", 900)
CARDS_PER_IMAGE = env_int("CARDS_PER_IMAGE", 10)
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
DIGEST_FILE = os.getenv("DIGEST_FILE", ".status_digest.json")
STATE_JSON_FILE = (os.getenv("STATE_JSON_FILE") or "npattern_selected_runtime.json").strip()
PERSIST_RUNTIME_STATE = env_bool("PERSIST_RUNTIME_STATE", True)
LIVE_REQUIRE_LTP_NEAR_D = env_bool("LIVE_REQUIRE_LTP_NEAR_D", True)
USE_GITHUB_STATE_WRITEBACK = env_bool("USE_GITHUB_STATE_WRITEBACK", False)
GITHUB_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()
GITHUB_REPO = (os.getenv("GITHUB_REPO") or "").strip()
GITHUB_BRANCH = (os.getenv("GITHUB_BRANCH") or "main").strip()
GITHUB_FILE_PATH = (os.getenv("GITHUB_FILE_PATH") or "").strip()

INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
HEADERS_AUTH = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"} if UPSTOX_ACCESS_TOKEN else {"Accept": "application/json"}
HEADERS_NOAUTH = {"Accept": "application/json"}
_INSTRUMENT_MAP: Dict[str, Dict[str, Any]] = {}


def now_ist() -> datetime:
    return datetime.now(IST)


def log(msg: str) -> None:
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] {msg}", flush=True)


def dbg(msg: str) -> None:
    if DE_BUG:
        log(f"[DE_BUG] {msg}")


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


def parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        formats = [
            "%d-%m-%Y %H:%M",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        dt = None
        for fmt in formats:
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                continue
        if dt is None:
            try:
                dt = pd.to_datetime(s).to_pydatetime()
            except Exception:
                return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def fmt_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def in_market_hours() -> bool:
    n = now_ist()
    if n.weekday() >= 5:
        return False
    start = n.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)
    end = n.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN, second=0, microsecond=0)
    return start <= n <= end


def wait_until_market_start() -> None:
    while True:
        if in_market_hours():
            return
        n = now_ist()
        if n.weekday() >= 5:
            log("Weekend; sleeping until next market day")
            time.sleep(600)
            continue
        start = n.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)
        if n < start:
            secs = max(30, int((start - n).total_seconds()))
            log(f"Waiting for market start; {secs // 60} min left")
            time.sleep(min(secs, 300))
        else:
            time.sleep(60)


def requests_get(url: str, headers: Optional[Dict[str, str]] = None) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers or HEADERS_NOAUTH, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            dbg(f"GET retry {attempt}/{MAX_RETRIES} failed | url={url} | error={e}")
            time.sleep(min(5, attempt * 1.5))
    raise RuntimeError(f"GET failed | url={url} | {last_err}")


def requests_post(url: str, **kwargs) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, timeout=kwargs.pop("timeout", HTTP_TIMEOUT), **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            dbg(f"POST retry {attempt}/{MAX_RETRIES} failed | url={url} | error={e}")
            time.sleep(min(5, attempt * 1.5))
    raise RuntimeError(f"POST failed | url={url} | {last_err}")


def refresh_json_source() -> None:
    if not JSON_SOURCE_URL:
        return
    if PERSIST_RUNTIME_STATE and os.path.exists(STATE_JSON_FILE):
        dbg(f"Runtime state file present; skipping source refresh | path={STATE_JSON_FILE}")
        return
    if os.path.exists(JSON_FILE) and not REFRESH_JSON_EACH_CYCLE:
        return
    try:
        resp = requests_get(JSON_SOURCE_URL, headers={"Accept": "application/json"})
        data = resp.json()
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        if PERSIST_RUNTIME_STATE:
            with open(STATE_JSON_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        dbg(f"Refreshed JSON from source | url={JSON_SOURCE_URL} | count={len(data) if isinstance(data, list) else 'na'}")
    except Exception as e:
        log(f"JSON source refresh failed: {e}")


def github_writeback_if_enabled(data: List[Dict[str, Any]]) -> None:
    if not USE_GITHUB_STATE_WRITEBACK:
        return
    if not all([GITHUB_TOKEN, GITHUB_REPO, GITHUB_FILE_PATH]):
        log("GitHub writeback skipped: missing GITHUB_TOKEN/GITHUB_REPO/GITHUB_FILE_PATH")
        return
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        current = requests.get(api_url, headers=headers, timeout=HTTP_TIMEOUT)
        sha = None
        if current.status_code == 200:
            sha = current.json().get("sha")
        content = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        payload = {
            "message": f"Update scanner state {now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}",
            "content": __import__("base64").b64encode(content).decode("ascii"),
            "branch": GITHUB_BRANCH,
        }
        if sha:
            payload["sha"] = sha
        resp = requests.put(api_url, headers=headers, json=payload, timeout=max(HTTP_TIMEOUT, 30))
        resp.raise_for_status()
        dbg("GitHub writeback success")
    except Exception as e:
        log(f"GitHub writeback failed: {e}")


def load_setups() -> List[Dict[str, Any]]:
    refresh_json_source()
    path = STATE_JSON_FILE if (PERSIST_RUNTIME_STATE and os.path.exists(STATE_JSON_FILE)) else JSON_FILE
    if not os.path.exists(path):
        log(f"JSON file not found: {path}")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("JSON file must contain a list of setups")
    dbg(f"Loaded setups | path={path} | count={len(data)}")
    return data


def save_setups(items: List[Dict[str, Any]]) -> None:
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
    if PERSIST_RUNTIME_STATE:
        with open(STATE_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)
    github_writeback_if_enabled(items)


def load_instrument_master() -> Dict[str, Dict[str, Any]]:
    global _INSTRUMENT_MAP
    if _INSTRUMENT_MAP:
        return _INSTRUMENT_MAP
    import gzip
    try:
        resp = requests_get(INSTRUMENTS_URL, headers={"Accept": "application/json"})
        rows = json.loads(gzip.decompress(resp.content).decode("utf-8"))
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
        if trading_symbol and instrument_key and segment == "NSE_EQ" and instrument_type in {"EQ", "BE", "BZ", "BL", "SM"}:
            mapping[trading_symbol] = rec
    _INSTRUMENT_MAP = mapping
    log(f"Loaded {len(mapping)} NSE_EQ instruments")
    return _INSTRUMENT_MAP


def symbol_to_instrument_key(symbol: str) -> Optional[str]:
    rec = load_instrument_master().get(short_symbol(symbol).upper())
    key = str(rec.get("instrument_key", "")).strip() if rec else None
    if key:
        dbg(f"Instrument key mapped | symbol={symbol} | cleaned={short_symbol(symbol).upper()} | key={key}")
    return key


def parse_candles(raw: Any) -> pd.DataFrame:
    rows = []
    if isinstance(raw, dict):
        raw = raw.get("data", {}).get("candles") or raw.get("candles") or []
    for item in raw or []:
        if not isinstance(item, list) or len(item) < 5:
            continue
        ts = item[0]
        dt = parse_dt(ts)
        if not dt:
            continue
        o = ensure_float(item[1])
        h = ensure_float(item[2])
        l = ensure_float(item[3])
        c = ensure_float(item[4])
        v = ensure_float(item[5]) if len(item) > 5 else 0.0
        oi = ensure_float(item[6]) if len(item) > 6 else 0.0
        rows.append({"datetime": dt.astimezone(IST), "o": o, "h": h, "l": l, "c": c, "v": v, "oi": oi})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)
    return df


def tf_endpoint(tf_minutes: int, instrument_key: str, to_date: date, from_date: date) -> str:
    if tf_minutes < 60:
        return f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{tf_minutes}/{to_date.isoformat()}/{from_date.isoformat()}"
    if tf_minutes % 60 == 0 and tf_minutes < 1440:
        return f"https://api.upstox.com/v3/historical-candle/{instrument_key}/hours/{tf_minutes // 60}/{to_date.isoformat()}/{from_date.isoformat()}"
    if tf_minutes == 1440:
        return f"https://api.upstox.com/v3/historical-candle/{instrument_key}/days/1/{to_date.isoformat()}/{from_date.isoformat()}"
    raise ValueError(f"Unsupported tf_minutes={tf_minutes}")


def intraday_endpoint(tf_minutes: int, instrument_key: str) -> str:
    if tf_minutes < 60:
        return f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/minutes/{tf_minutes}"
    if tf_minutes % 60 == 0 and tf_minutes < 1440:
        return f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/hours/{tf_minutes // 60}"
    if tf_minutes == 1440:
        return f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/days/1"
    raise ValueError(f"Unsupported tf_minutes={tf_minutes}")


def max_days_per_request(tf_minutes: int) -> int:
    if tf_minutes <= 15:
        return 28
    if tf_minutes <= 300:
        return 90
    return 365


def session_schedule_for_day(day: date, tf_minutes: int) -> List[Tuple[datetime, datetime]]:
    start = datetime(day.year, day.month, day.day, MARKET_OPEN_HOUR, MARKET_OPEN_MIN, tzinfo=IST)
    end = datetime(day.year, day.month, day.day, MARKET_CLOSE_HOUR, MARKET_CLOSE_MIN, tzinfo=IST)
    out: List[Tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(minutes=tf_minutes), end)
        out.append((cur, nxt))
        cur = nxt
    return out


def filter_to_closed_intraday(df: pd.DataFrame, tf_minutes: int, asof: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    asof = asof.astimezone(IST)
    keep = []
    for _, row in df.iterrows():
        dt = row["datetime"].astimezone(IST)
        if dt.date() != asof.date():
            keep.append(True)
            continue
        closed = False
        for s, e in session_schedule_for_day(dt.date(), tf_minutes):
            if s == dt:
                closed = e <= asof
                break
        keep.append(closed)
    return df[pd.Series(keep, index=df.index)].reset_index(drop=True)


def fetch_history(instrument_key: str, tf_minutes: int, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    start_dt = start_dt.astimezone(IST)
    end_dt = end_dt.astimezone(IST)
    if start_dt > end_dt:
        return pd.DataFrame(columns=["datetime", "o", "h", "l", "c", "v", "oi"])

    combined = pd.DataFrame(columns=["datetime", "o", "h", "l", "c", "v", "oi"])
    max_days = max_days_per_request(tf_minutes)

    # historical chunks for dates before today
    hist_end_date = min(end_dt.date(), now_ist().date() - timedelta(days=1))
    cur_start_date = start_dt.date()
    while cur_start_date <= hist_end_date:
        cur_end_date = min(hist_end_date, cur_start_date + timedelta(days=max_days - 1))
        url = tf_endpoint(tf_minutes, instrument_key, cur_end_date, cur_start_date)
        try:
            resp = requests_get(url, headers=HEADERS_NOAUTH)
            chunk = parse_candles(resp.json())
            combined = pd.concat([combined, chunk], ignore_index=True)
            dbg(f"fetch_history success | key={instrument_key} | tf={tf_minutes} | candles={len(chunk)} | url={url}")
        except Exception as e:
            dbg(f"fetch_history failed | key={instrument_key} | tf={tf_minutes} | url={url} | error={e}")
        cur_start_date = cur_end_date + timedelta(days=1)

    # intraday chunk for today
    if end_dt.date() >= now_ist().date() and start_dt.date() <= now_ist().date():
        try:
            url = intraday_endpoint(tf_minutes, instrument_key)
            resp = requests_get(url, headers=HEADERS_NOAUTH)
            today_df = parse_candles(resp.json())
            today_df = filter_to_closed_intraday(today_df, tf_minutes, now_ist())
            combined = pd.concat([combined, today_df], ignore_index=True)
            dbg(f"fetch_intraday success | key={instrument_key} | tf={tf_minutes} | candles={len(today_df)} | url={url}")
        except Exception as e:
            dbg(f"fetch_intraday failed | key={instrument_key} | tf={tf_minutes} | error={e}")

    if combined.empty:
        return combined
    combined = combined.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)
    combined = combined[(combined["datetime"] >= start_dt) & (combined["datetime"] <= end_dt)].reset_index(drop=True)
    return combined


def to_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    ha = df.copy().reset_index(drop=True)
    ha["ha_close"] = (ha["o"] + ha["h"] + ha["l"] + ha["c"]) / 4.0
    ha["ha_open"] = 0.0
    ha.loc[0, "ha_open"] = (ha.loc[0, "o"] + ha.loc[0, "c"]) / 2.0
    for i in range(1, len(ha)):
        ha.loc[i, "ha_open"] = (ha.loc[i - 1, "ha_open"] + ha.loc[i - 1, "ha_close"]) / 2.0
    ha["ha_high"] = ha[["h", "ha_open", "ha_close"]].max(axis=1)
    ha["ha_low"] = ha[["l", "ha_open", "ha_close"]].min(axis=1)
    return ha


def is_doji_ha(row: pd.Series) -> bool:
    rng = float(row["ha_high"] - row["ha_low"])
    if rng <= 0:
        return False
    body = abs(float(row["ha_close"] - row["ha_open"]))
    return (body / rng) * 100.0 <= HA_BODY_PERCENT


def open_low_ok(row: pd.Series) -> bool:
    low = ensure_float(row["ha_low"])
    opn = ensure_float(row["ha_open"])
    if low <= 0:
        return False
    return abs(opn - low) / low * 100.0 <= ENTRY_BUFFER_PERCENT


def open_high_ok(row: pd.Series) -> bool:
    high = ensure_float(row["ha_high"])
    opn = ensure_float(row["ha_open"])
    if high <= 0:
        return False
    return abs(opn - high) / high * 100.0 <= ENTRY_BUFFER_PERCENT


def ltp_quotes(symbol_keys: Dict[str, str]) -> Dict[str, float]:
    if not symbol_keys:
        return {}
    joined = ",".join(sorted(set(symbol_keys.values())))
    url = f"https://api.upstox.com/v3/market-quote/ltp?instrument_key={joined}"
    try:
        resp = requests_get(url, headers=HEADERS_AUTH)
        data = resp.json()
        payload = data.get("data", {}) if isinstance(data, dict) else {}
        by_instr = {v: k for k, v in symbol_keys.items()}
        out: Dict[str, float] = {}
        for outer_key, row in payload.items():
            if not isinstance(row, dict):
                continue
            ikey = row.get("instrument_token") or row.get("instrument_key")
            symbol = by_instr.get(ikey)
            if symbol:
                out[symbol] = ensure_float(row.get("last_price"), 0.0)
        dbg(f"ltp_quotes fetched | requested={len(symbol_keys)} | returned={len(out)}")
        return out
    except Exception as e:
        dbg(f"ltp_quotes failed | error={e}")
        return {}


def get_d_levels(setup: Dict[str, Any]) -> List[Dict[str, Any]]:
    levels = setup.get("d_levels") or []
    out = []
    if isinstance(levels, list) and levels:
        for idx, item in enumerate(levels):
            name = str((item or {}).get("level") or f"D{idx+1}")
            val = ensure_float((item or {}).get("value"))
            if val > 0:
                out.append({"level": name, "value": val})
    else:
        fib_d = ensure_float(setup.get("fib_d"))
        trend_d = ensure_float(setup.get("trend_d"))
        if trend_d > 0:
            out.append({"level": "Trend", "value": trend_d})
        if fib_d > 0:
            out.append({"level": "Fib", "value": fib_d})
    # preserve original order when provided; otherwise for bullish high->low and bearish low->high
    pattern = str(setup.get("pattern") or "Bullish").strip().lower()
    if not (setup.get("d_levels") or []):
        out = sorted(out, key=lambda x: x["value"], reverse=(pattern == "bullish"))
    return out


def remaining_d_levels(setup: Dict[str, Any]) -> List[Dict[str, Any]]:
    levels = get_d_levels(setup)
    idx = int(ensure_float(setup.get("current_d_index"), 0))
    idx = max(0, min(idx, len(levels) - 1)) if levels else 0
    return levels[idx:]


def current_active_d(setup: Dict[str, Any]) -> Tuple[float, str, int]:
    levels = get_d_levels(setup)
    if not levels:
        val = ensure_float(setup.get("active_d") or setup.get("trend_d") or setup.get("fib_d"))
        return val, str(setup.get("which_extension_active") or "Active D"), 0
    idx = int(ensure_float(setup.get("current_d_index"), 0))
    idx = max(0, min(idx, len(levels) - 1))
    return ensure_float(levels[idx]["value"]), str(levels[idx]["level"]), idx


def lowest_remaining_d(setup: Dict[str, Any]) -> float:
    rem = remaining_d_levels(setup)
    vals = [ensure_float(x["value"]) for x in rem if ensure_float(x["value"]) > 0]
    return min(vals) if vals else ensure_float(setup.get("active_d"))


def highest_remaining_d(setup: Dict[str, Any]) -> float:
    rem = remaining_d_levels(setup)
    vals = [ensure_float(x["value"]) for x in rem if ensure_float(x["value"]) > 0]
    return max(vals) if vals else ensure_float(setup.get("active_d"))


def price_near_any_remaining_d(price: float, setup: Dict[str, Any]) -> bool:
    rem = remaining_d_levels(setup)
    for item in rem:
        d = ensure_float(item.get("value"))
        if d > 0 and abs(price - d) / d * 100.0 <= ENTRY_LIMIT_PERCENT:
            return True
    return False


def choose_touched_level(low_or_high: float, setup: Dict[str, Any], bullish: bool) -> Optional[Dict[str, Any]]:
    rem = remaining_d_levels(setup)
    if bullish:
        touched = [x for x in rem if low_or_high <= ensure_float(x["value"])]
        if not touched:
            return None
        return min(touched, key=lambda x: abs(ensure_float(x["value"]) - low_or_high))
    touched = [x for x in rem if low_or_high >= ensure_float(x["value"])]
    if not touched:
        return None
    return min(touched, key=lambda x: abs(ensure_float(x["value"]) - low_or_high))


def shift_to_next_d(setup: Dict[str, Any], candle_dt: datetime) -> bool:
    levels = get_d_levels(setup)
    idx = int(ensure_float(setup.get("current_d_index"), 0))
    if idx + 1 < len(levels):
        idx += 1
        setup["current_d_index"] = idx
        setup["active_d"] = ensure_float(levels[idx]["value"])
        setup["which_extension_active"] = str(levels[idx]["level"])
        setup["status"] = "shifted_to_next_d"
        setup["target_status"] = "Shifted to next D"
        setup["last_checked_at"] = fmt_dt(candle_dt)
        setup["entry_reason"] = f"Closed beyond active D; moved to {levels[idx]['level']}"
        dbg(f"shift_to_next_d | symbol={setup.get('symbol')} | new_idx={idx} | new_active={setup['active_d']}")
        return True
    setup["status"] = "next_d_exhausted"
    setup["pattern_active"] = "No"
    setup["target_status"] = "No next D"
    setup["last_checked_at"] = fmt_dt(candle_dt)
    return False


def reset_runtime_fields(setup: Dict[str, Any]) -> Dict[str, Any]:
    s = dict(setup)
    active_val, active_label, idx = current_active_d(s)
    s["current_d_index"] = idx
    s["active_d"] = active_val
    s["which_extension_active"] = active_label
    s["d_hit"] = "No"
    s["touched_d_price"] = 0.0
    s["touched_d_source"] = ""
    s["touched_price"] = None
    s["touched_at"] = ""
    s["touch_pattern_time"] = ""
    s["entry_found"] = False
    s["entry_price"] = None
    s["entry_time"] = ""
    s["entry_reason"] = ""
    s["sl_price"] = None
    s["target_price"] = None
    s["target_hit"] = False
    s["sl_hit"] = False
    s["status"] = "waiting"
    s["target_status"] = "Waiting"
    s["alert_sent"] = False
    s["pattern_active"] = "Yes"
    s["ltp_at_alert"] = None
    s["confidence_score"] = 0
    s["last_checked_at"] = fmt_dt(now_ist())
    s["failure_time"] = ""
    s["failure_reason"] = ""
    s["result_bucket"] = ""
    return s


def evaluate_entry_window(
    setup: Dict[str, Any],
    instrument_key: str,
    touch_candle_end: datetime,
    invalidation_time: Optional[datetime],
    ltp_now: Optional[float],
) -> Tuple[Optional[float], Optional[datetime], str]:
    start_dt = touch_candle_end
    end_dt = invalidation_time or now_ist()
    entry_df = fetch_history(instrument_key, ENTRY_TF_MINUTES, start_dt - timedelta(minutes=ENTRY_TF_MINUTES), end_dt)
    if entry_df.empty:
        return None, None, "No 15m candles"
    entry_df = entry_df[entry_df["datetime"] >= start_dt].reset_index(drop=True)
    if entry_df.empty:
        return None, None, "No 15m candles after touch"
    ha = to_heikin_ashi(entry_df)
    bullish = str(setup.get("pattern") or "Bullish").strip().lower() == "bullish"
    for i in range(len(ha) - 1):
        first = ha.iloc[i]
        second = ha.iloc[i + 1]
        if not is_doji_ha(first):
            continue
        ok = open_low_ok(second) if bullish else open_high_ok(second)
        if not ok:
            continue
        gate_price = ensure_float(second["c"], 0.0) if AFTER_MARKET_MODE or not LIVE_REQUIRE_LTP_NEAR_D else ensure_float(ltp_now, 0.0)
        if gate_price <= 0:
            gate_price = ensure_float(second["c"], 0.0)
        if not price_near_any_remaining_d(gate_price, setup):
            continue
        entry_price = ensure_float(first["ha_high"] if bullish else first["ha_low"])
        entry_time = second["datetime"].to_pydatetime() if hasattr(second["datetime"], "to_pydatetime") else second["datetime"]
        return entry_price, entry_time, "Doji + immediate next candle confirmed"
    return None, None, "No valid doji->next-candle entry"


def evaluate_setup(setup: Dict[str, Any], ltp_now: Optional[float]) -> Dict[str, Any]:
    s = reset_runtime_fields(setup)
    symbol = str(s.get("symbol") or "")
    pattern = str(s.get("pattern") or "Bullish").strip().lower()
    bullish = pattern == "bullish"
    c_dt = parse_dt(s.get("c_time")) or parse_dt(s.get("scan_date")) or now_ist() - timedelta(days=30)
    instrument_key = symbol_to_instrument_key(symbol)
    if not instrument_key:
        s["status"] = "instrument_missing"
        s["result_bucket"] = "DATA ERROR"
        return s

    start_dt = c_dt - timedelta(days=5)
    end_dt = now_ist()
    pattern_df = fetch_history(instrument_key, PATTERN_TF_MINUTES, start_dt, end_dt)
    if pattern_df.empty:
        s["status"] = "no_pattern_candles"
        s["result_bucket"] = "DATA ERROR"
        return s

    pattern_df = pattern_df[pattern_df["datetime"] >= c_dt].reset_index(drop=True)
    if pattern_df.empty:
        s["status"] = "no_pattern_candles_after_c"
        s["result_bucket"] = "DATA ERROR"
        return s

    touch_found = False
    entry_found = False
    entry_time: Optional[datetime] = None
    entry_price: Optional[float] = None
    touch_end: Optional[datetime] = None

    # build end times for pattern candles using session schedule when intraday
    pattern_times: List[Tuple[datetime, datetime]] = []
    for _, row in pattern_df.iterrows():
        dt = row["datetime"].to_pydatetime() if hasattr(row["datetime"], "to_pydatetime") else row["datetime"]
        dt = dt.astimezone(IST)
        end_guess = dt + timedelta(minutes=PATTERN_TF_MINUTES)
        # adjust for market-close partial last candle
        for st, en in session_schedule_for_day(dt.date(), PATTERN_TF_MINUTES):
            if st == dt:
                end_guess = en
                break
        pattern_times.append((dt, end_guess))

    i = 0
    while i < len(pattern_df):
        row = pattern_df.iloc[i]
        candle_start, candle_end = pattern_times[i]
        low = ensure_float(row["l"])
        high = ensure_float(row["h"])
        close = ensure_float(row["c"])
        active_d, active_label, _ = current_active_d(s)
        lowest_d = lowest_remaining_d(s)
        highest_d = highest_remaining_d(s)

        # stoploss for active trade on pattern timeframe close only
        if entry_found:
            if bullish and close < lowest_d:
                s["sl_hit"] = True
                s["status"] = "stoploss_hit"
                s["target_status"] = "Stoploss hit"
                s["pattern_active"] = "No"
                s["failure_time"] = fmt_dt(candle_end)
                s["failure_reason"] = "Pattern timeframe candle closed below lowest D"
                break
            if (not bullish) and close > highest_d:
                s["sl_hit"] = True
                s["status"] = "stoploss_hit"
                s["target_status"] = "Stoploss hit"
                s["pattern_active"] = "No"
                s["failure_time"] = fmt_dt(candle_end)
                s["failure_reason"] = "Pattern timeframe candle closed above highest D"
                break

        # pre-entry pattern failure
        if not entry_found:
            c_price = ensure_float(s.get("c_price"))
            if bullish and close > c_price:
                s["status"] = "pattern_failed"
                s["pattern_active"] = "No"
                s["target_status"] = "Pattern failed"
                s["failure_time"] = fmt_dt(candle_end)
                s["failure_reason"] = "Pattern timeframe candle closed above C"
                dbg(f"pattern failed | symbol={symbol} | close={close} | c={c_price} | dt={fmt_dt(candle_end)}")
                break
            if (not bullish) and close < c_price:
                s["status"] = "pattern_failed"
                s["pattern_active"] = "No"
                s["target_status"] = "Pattern failed"
                s["failure_time"] = fmt_dt(candle_end)
                s["failure_reason"] = "Pattern timeframe candle closed below C"
                break

        # shift to next D before entry if close crossed current active D
        if not entry_found:
            if bullish and close < active_d:
                shifted = shift_to_next_d(s, candle_end)
                if not shifted:
                    break
                i += 1
                continue
            if (not bullish) and close > active_d:
                shifted = shift_to_next_d(s, candle_end)
                if not shifted:
                    break
                i += 1
                continue

        # touch detection
        if not touch_found and not entry_found:
            touched_item = choose_touched_level(low if bullish else high, s, bullish)
            valid_touch = False
            if bullish and touched_item is not None and close >= lowest_d:
                valid_touch = True
            if (not bullish) and touched_item is not None and close <= highest_d:
                valid_touch = True
            if valid_touch:
                touch_found = True
                touch_end = candle_end
                s["d_hit"] = "Yes"
                s["touched_d_source"] = str(touched_item.get("level"))
                s["touched_d_price"] = ensure_float(touched_item.get("value"))
                s["touched_price"] = low if bullish else high
                s["touched_at"] = fmt_dt(candle_end)
                s["touch_pattern_time"] = fmt_dt(candle_start)
                s["status"] = "touched"
                s["target_status"] = "D touched"
                s["entry_reason"] = "Touched on closed pattern candle"
                dbg(f"touch found | symbol={symbol} | source={s['touched_d_source']} | d={s['touched_d_price']} | price={s['touched_price']} | dt={fmt_dt(candle_start)}")

                # find invalidation time after touch and before entry
                invalidation_time = None
                for j in range(i + 1, len(pattern_df)):
                    prow = pattern_df.iloc[j]
                    pstart, pend = pattern_times[j]
                    pclose = ensure_float(prow["c"])
                    c_price = ensure_float(s.get("c_price"))
                    current_d_after_touch, _, _ = current_active_d(s)
                    if bullish and pclose > c_price:
                        invalidation_time = pend
                        break
                    if (not bullish) and pclose < c_price:
                        invalidation_time = pend
                        break
                    if bullish and pclose < current_d_after_touch:
                        invalidation_time = pend
                        break
                    if (not bullish) and pclose > current_d_after_touch:
                        invalidation_time = pend
                        break

                e_price, e_time, reason = evaluate_entry_window(s, instrument_key, touch_end, invalidation_time, ltp_now)
                if e_price is not None and e_time is not None:
                    entry_found = True
                    entry_time = e_time
                    entry_price = e_price
                    s["entry_found"] = True
                    s["entry_price"] = round(float(e_price), 2)
                    s["entry_time"] = fmt_dt(e_time)
                    s["entry_reason"] = reason
                    s["status"] = "entry_waiting"
                    s["target_status"] = "Entry triggered; waiting"
                    sl_ref = lowest_d if bullish else highest_d
                    s["sl_price"] = round(float(sl_ref), 2)
                    target = e_price * (1 + TARGET_PERCENT / 100.0) if bullish else e_price * (1 - TARGET_PERCENT / 100.0)
                    s["target_price"] = round(float(target), 2)
                    s["ltp_at_alert"] = round(float(ltp_now), 2) if ltp_now else None
                # do not break; continue scanning pattern candles for SL after entry
        i += 1

    # target evaluation on closed 15m candles after entry
    if entry_found and entry_time and entry_price:
        end_dt_target = now_ist()
        post_df = fetch_history(instrument_key, ENTRY_TF_MINUTES, entry_time, end_dt_target)
        if not post_df.empty:
            post_df = post_df[post_df["datetime"] >= entry_time].reset_index(drop=True)
            if bullish:
                hit = post_df[post_df["h"] >= ensure_float(s.get("target_price"))]
            else:
                hit = post_df[post_df["l"] <= ensure_float(s.get("target_price"))]
            if not hit.empty:
                hit_dt = hit.iloc[0]["datetime"]
                s["target_hit"] = True
                s["status"] = "target_hit"
                s["target_status"] = "Target hit"
                s["pattern_active"] = "No"
                s["failure_time"] = fmt_dt(hit_dt.to_pydatetime() if hasattr(hit_dt, "to_pydatetime") else hit_dt)

    # result bucket
    bucket = {
        "touched": "D TOUCHED",
        "entry_waiting": "ENTRY WAITING",
        "target_hit": "TARGET HIT",
        "stoploss_hit": "STOPLOSS HIT",
        "shifted_to_next_d": "NEXT D",
        "pattern_failed": "PATTERN FAILED",
        "next_d_exhausted": "NEXT D",
        "instrument_missing": "DATA ERROR",
        "no_pattern_candles": "DATA ERROR",
        "no_pattern_candles_after_c": "DATA ERROR",
        "waiting": "ACTIVE",
    }.get(str(s.get("status")), "ACTIVE")
    s["result_bucket"] = bucket
    s["last_checked_at"] = fmt_dt(now_ist())
    return s


def _load_font(size: int, bold: bool = False):
    names = [
        ("DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"),
        ("LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"),
    ]
    base_paths = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__)),
        "/usr/share/fonts/truetype/dejavu",
        "/usr/share/fonts/truetype/liberation2",
    ]
    candidates = []
    for base in base_paths:
        for name in names:
            candidates.append(os.path.join(base, "fonts", name))
            candidates.append(os.path.join(base, name))
    for font_path in candidates:
        if os.path.exists(font_path):
            try:
                return ImageFont.truetype(font_path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def chunked(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def send_telegram_photo(image_bytes: bytes, caption: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        requests_post(url, files={"photo": ("status.png", image_bytes, "image/png")}, data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption}, timeout=max(HTTP_TIMEOUT, 30))
    except Exception as e:
        log(f"Telegram photo failed: {e}")


def digest_for_groups(groups: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    out = {}
    for name, items in groups.items():
        payload = [(x.get("pattern_id") or x.get("symbol"), x.get("status"), x.get("entry_price"), x.get("target_price"), x.get("sl_price"), x.get("current_d_index")) for x in items]
        out[name] = hashlib.md5(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return out


def load_digest_cache() -> Dict[str, str]:
    if not os.path.exists(DIGEST_FILE):
        return {}
    try:
        with open(DIGEST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_digest_cache(d: Dict[str, str]) -> None:
    with open(DIGEST_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2)


def _status_color(status: str) -> Tuple[int, int, int]:
    s = str(status or "").lower()
    if "target" in s:
        return (22, 163, 74)
    if "stoploss" in s or "failed" in s:
        return (220, 38, 38)
    if "shifted" in s or "next" in s:
        return (234, 88, 12)
    return (37, 99, 235)


def _fmt_num(val: Any) -> str:
    if val in (None, "", "NA"):
        return "-"
    try:
        return f"{float(val):.2f}"
    except Exception:
        return str(val)


def _row_text(item: Dict[str, Any]) -> Dict[str, str]:
    touch_txt = "-"
    if ensure_float(item.get("touched_d_price")):
        tp = _fmt_num(item.get("touched_d_price"))
        tpr = _fmt_num(item.get("touched_price"))
        tt = str(item.get("touched_at") or "")[:16]
        touch_txt = f"{tp} @ {tpr}" if tpr != "-" else tp
        if tt:
            touch_txt += f" | {tt}"
    entry_txt = "-"
    if item.get("entry_price") not in (None, ""):
        ep = _fmt_num(item.get("entry_price"))
        et = str(item.get("entry_time") or "")[:16]
        entry_txt = f"{ep} | {et}" if et else ep
    level = str(item.get("touched_d_source") or item.get("which_extension_active") or "-")
    if level.startswith("2nd Active D") or level.startswith("1st Active D") or level.startswith("3rd"):
        level = str(item.get("which_extension_active") or level)
    return {
        "name": short_symbol(str(item.get("symbol"))),
        "status": str(item.get("status") or "-"),
        "d": _fmt_num(item.get("active_d")),
        "touch": touch_txt,
        "entry": entry_txt,
        "sl": _fmt_num(item.get("sl_price")),
        "target": _fmt_num(item.get("target_price")),
        "level": f"{item.get('current_d_index', '-')} | {level}",
    }


def build_status_image(title: str, items: List[Dict[str, Any]], mode_label: str, page_no: int = 1, page_total: int = 1) -> bytes:
    black = (20, 20, 20)
    blue = (37, 99, 235)
    gray = (100, 116, 139)
    border = (220, 226, 232)
    white = (255, 255, 255)
    head_fill = (235, 241, 255)
    alt_fill = (249, 251, 255)

    f_title = _load_font(64, True)
    f_sub = _load_font(30, False)
    f_tbl_h = _load_font(28, True)
    f_tbl_b = _load_font(26, False)
    f_small = _load_font(24, False)

    left = 24
    top = 128
    table_w = IMAGE_WIDTH - 48
    row_h = 60
    header_h = 64
    footer_h = 42
    max_rows = max(1, CARDS_PER_IMAGE)
    rows = items[:max_rows]

    dynamic_height = max(360, min(IMAGE_HEIGHT, top + header_h + 12 + len(rows) * row_h + 24 + footer_h))
    img = Image.new("RGB", (IMAGE_WIDTH, dynamic_height), (244, 247, 250))
    draw = ImageDraw.Draw(img)

    draw.rounded_rectangle((18, 18, IMAGE_WIDTH - 18, 108), radius=18, fill=white, outline=border, width=2)
    draw.text((34, 24), title, font=f_title, fill=blue)
    stamp = f"{mode_label} | {now_ist().strftime('%d-%m-%Y %H:%M IST')} | Page {page_no}/{page_total}"
    sb = draw.textbbox((0, 0), stamp, font=f_sub)
    draw.text((IMAGE_WIDTH - 34 - (sb[2] - sb[0]), 40), stamp, font=f_sub, fill=gray)

    headers = ["NAME", "STATUS", "D POINT", "TOUCH", "ENTRY", "SL", "TARGET", "LEVEL"]
    col_widths = [240, 260, 170, 430, 360, 145, 165, table_w - (240+260+170+430+360+145+165)]
    xs = [left]
    for w in col_widths:
        xs.append(xs[-1] + w)

    draw.rounded_rectangle((left, top, left + table_w, top + header_h), radius=12, fill=head_fill, outline=border, width=1)
    for i, h in enumerate(headers):
        draw.text((xs[i] + 12, top + 15), h, font=f_tbl_h, fill=blue)
        if i > 0:
            draw.line((xs[i], top + 6, xs[i], top + header_h - 6), fill=border, width=1)

    y = top + header_h + 10
    for idx, item in enumerate(rows):
        fill = white if idx % 2 == 0 else alt_fill
        draw.rounded_rectangle((left, y, left + table_w, y + row_h - 6), radius=10, fill=fill, outline=border, width=1)
        row = _row_text(item)
        vals = [row["name"], row["status"], row["d"], row["touch"], row["entry"], row["sl"], row["target"], row["level"]]
        for i, val in enumerate(vals):
            color = _status_color(row["status"]) if i == 1 else black
            text_val = str(val)
            while len(text_val) > 2:
                bb = draw.textbbox((0, 0), text_val, font=f_tbl_b)
                if (bb[2] - bb[0]) <= (col_widths[i] - 18):
                    break
                text_val = text_val[:-2] + "…"
            draw.text((xs[i] + 10, y + 14), text_val, font=f_tbl_b, fill=color)
            if i > 0:
                draw.line((xs[i], y + 4, xs[i], y + row_h - 10), fill=border, width=1)
        y += row_h

    footer = f"Count: {len(items)} | Format: NAME | STATUS | D POINT | TOUCH | ENTRY | SL | TARGET | LEVEL"
    draw.text((left + 6, dynamic_height - 34), footer, font=f_small, fill=gray)

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


def build_status_text(title: str, items: List[Dict[str, Any]], limit: int = 30) -> str:
    lines = [title]
    for item in items[:limit]:
        row = _row_text(item)
        lines.append(f"{row['name']} | D {row['d']} | {row['status']}")
    if len(items) > limit:
        lines.append(f"... +{len(items) - limit} more")
    return "\n".join(lines)


def send_status_images(items: List[Dict[str, Any]]) -> None:
    groups: Dict[str, List[Dict[str, Any]]] = {
        "D TOUCHED": [],
        "ENTRY WAITING": [],
        "TARGET HIT": [],
        "STOPLOSS HIT": [],
        "NEXT D": [],
        "PATTERN FAILED": [],
        "ACTIVE SUMMARY": [],
    }
    for item in items:
        bucket = str(item.get("result_bucket") or "")
        status = str(item.get("status") or "")
        if bucket in groups:
            groups[bucket].append(item)
        if status in {"touched", "entry_waiting", "entry_found", "waiting", "shifted_to_next_d"} and bucket not in {"PATTERN FAILED", "STOPLOSS HIT", "TARGET HIT"}:
            groups["ACTIVE SUMMARY"].append(item)

    cur_digest = digest_for_groups(groups)
    old_digest = load_digest_cache()
    mode_label = "AFTER MARKET" if AFTER_MARKET_MODE else "LIVE MARKET"
    for name, group in groups.items():
        if not group:
            continue
        if old_digest.get(name) == cur_digest.get(name):
            continue
        pages = chunked(group, max(1, CARDS_PER_IMAGE))
        for i, page in enumerate(pages, start=1):
            img = build_status_image(name, page, mode_label, i, len(pages))
            caption = build_status_text(f"{name} ({i}/{len(pages)})", page)
            send_telegram_photo(img, caption)
    save_digest_cache(cur_digest)


def main() -> None:
    log("Strict live/after-market scanner started")
    dbg(f"Startup env | JSON_FILE={JSON_FILE} | STATE_JSON_FILE={STATE_JSON_FILE} | ONLY_MARKET_HOURS={ONLY_MARKET_HOURS} | AFTER_MARKET_MODE={AFTER_MARKET_MODE} | PATTERN_TF_MINUTES={PATTERN_TF_MINUTES} | ENTRY_LIMIT_PERCENT={ENTRY_LIMIT_PERCENT} | ENTRY_BUFFER_PERCENT={ENTRY_BUFFER_PERCENT}")
    load_instrument_master()

    while True:
        try:
            if not AFTER_MARKET_MODE and ONLY_MARKET_HOURS and not in_market_hours():
                wait_until_market_start()
                continue

            setups = load_setups()
            if not setups:
                log("No setups found")
                time.sleep(max(POLL_SECONDS, 30))
                continue

            symbol_keys: Dict[str, str] = {}
            if not AFTER_MARKET_MODE and LIVE_REQUIRE_LTP_NEAR_D:
                for s in setups:
                    key = symbol_to_instrument_key(str(s.get("symbol") or ""))
                    if key:
                        symbol_keys[str(s.get("symbol"))] = key
            ltp_map = ltp_quotes(symbol_keys) if symbol_keys else {}

            updated: List[Dict[str, Any]] = []
            for idx, setup in enumerate(setups, start=1):
                symbol = str(setup.get("symbol") or "")
                dbg(f"Evaluate[{idx}] | symbol={symbol}")
                evaluated = evaluate_setup(setup, ltp_map.get(symbol))
                updated.append(evaluated)

            save_setups(updated)
            send_status_images(updated)

            if AFTER_MARKET_MODE and HISTORICAL_RUN_ONCE:
                log("Historical after-market run completed once; exiting")
                return
        except Exception as e:
            log(f"Main loop error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

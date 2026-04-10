import os
import io
import json
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from fyers_apiv3 import fyersModel

IST = timezone(timedelta(hours=5, minutes=30))

JSON_FILE = os.getenv("JSON_FILE", "npattern_selected.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
ENTRY_ZONE_PERCENT = float(os.getenv("ENTRY_ZONE_PERCENT", "2.0"))
RATE_LIMIT_SLEEP = float(os.getenv("RATE_LIMIT_SLEEP", "1.2"))
ONLY_MARKET_HOURS = os.getenv("ONLY_MARKET_HOURS", "true").strip().lower() == "true"
MARKET_START = (9, 15)
MARKET_END = (15, 30)
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "6"))
OI_CONFIRMATION_REQUIRED = os.getenv("OI_CONFIRMATION_REQUIRED", "false").strip().lower() == "true"
TELEGRAM_SEND_MODE = os.getenv("TELEGRAM_SEND_MODE", "photo").strip().lower() or "photo"
IMAGE_WIDTH = int(os.getenv("IMAGE_WIDTH", "1500"))
IMAGE_HEIGHT = int(os.getenv("IMAGE_HEIGHT", "900"))
SHOW_STARTUP_SAMPLE = os.getenv("SHOW_STARTUP_SAMPLE", "true").strip().lower() == "true"
STARTUP_SAMPLE_SENT_FILE = os.getenv("STARTUP_SAMPLE_SENT_FILE", ".startup_sample_sent")

CLIENT_ID = (os.getenv("FYERS_CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("FYERS_ACCESS_TOKEN") or "").strip()
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()


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
    start = n.replace(hour=MARKET_START[0], minute=MARKET_START[1], second=0, microsecond=0)
    end = n.replace(hour=MARKET_END[0], minute=MARKET_END[1], second=0, microsecond=0)
    return start <= n <= end


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
    try:
        try:
            resp = FYERS.history(data=payload)
        except TypeError:
            resp = FYERS.history(payload)
        candles = resp.get("candles", []) if isinstance(resp, dict) else []
        if not candles:
            return None
        df = pd.DataFrame(candles, columns=["ts", "o", "h", "l", "c", "v"])
        df["datetime"] = pd.to_datetime(df["ts"], unit="s")
        return df
    except Exception as e:
        log(f"History error for {symbol}: {e}")
        return None



def get_ltp(symbol: str) -> Optional[float]:
    payload = {"symbols": symbol}
    try:
        try:
            resp = FYERS.quotes(data=payload)
        except TypeError:
            resp = FYERS.quotes(payload)
        for item in resp.get("d", []):
            values = item.get("v", {})
            ltp = values.get("lp") or values.get("ltp") or values.get("last_price")
            if ltp is not None:
                return float(ltp)
    except Exception as e:
        log(f"Quote error for {symbol}: {e}")
    return None



def fetch_option_chain_snapshot(symbol: str, strikecount: int = STRIKECOUNT) -> Dict[str, Any]:
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": ""}
    try:
        try:
            resp = FYERS.optionchain(data=payload)
        except TypeError:
            resp = FYERS.optionchain(payload)
    except Exception as e:
        return {"ok": False, "reason": f"optionchain error: {e}", "rows": [], "bias": "NA", "underlying_ltp": None}

    raw = resp.get("data") if isinstance(resp, dict) and isinstance(resp.get("data"), dict) else resp
    chain = []
    if isinstance(raw, dict):
        chain = raw.get("optionsChain") or raw.get("chain") or raw.get("options_chain") or raw.get("d") or []
        underlying_ltp = raw.get("ltp") or raw.get("underlying_ltp") or raw.get("underlyingPrice")
    else:
        chain = []
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
        row[f"{side.lower()}_ltp"] = float(item.get("ltp") or 0)
        row[f"{side.lower()}_oi"] = float(item.get("oi") or 0)
        row[f"{side.lower()}_oich"] = float(item.get("oich") or item.get("oi_change") or 0)
        row[f"{side.lower()}_vol"] = float(item.get("volume") or 0)

    rows = sorted(rows_by_strike.values(), key=lambda x: x["strike"])
    if not rows:
        return {"ok": False, "reason": "empty option chain", "rows": [], "bias": "NA", "underlying_ltp": underlying_ltp}

    if underlying_ltp is None:
        underlying_ltp = get_ltp(symbol)
    try:
        underlying_ltp = float(underlying_ltp)
    except Exception:
        underlying_ltp = None

    atm_index = 0
    if underlying_ltp is not None:
        atm_index = min(range(len(rows)), key=lambda i: abs(rows[i]["strike"] - underlying_ltp))
    start = max(0, atm_index - 2)
    end = min(len(rows), atm_index + 3)
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
    ce = float(atm.get("ce_oich") or 0)
    pe = float(atm.get("pe_oich") or 0)
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
        r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=20)
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
        r = requests.post(url, data=data, files=files, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log(f"Telegram photo error: {e}")
        send_telegram_text(caption)


# =========================
# Dashboard image
# =========================
def _load_font(size: int, bold: bool = False):
    import os
    from PIL import ImageFont

    base_dir = os.path.dirname(__file__)
    fonts_dir = os.path.join(base_dir, "fonts")

    # ✅ Priority 1: YOUR GitHub fonts
    if bold:
        custom_paths = [
            os.path.join(fonts_dir, "DejaVuSans-Bold.ttf"),
        ]
    else:
        custom_paths = [
            os.path.join(fonts_dir, "DejaVuSans.ttf"),
        ]

    for p in custom_paths:
        try:
            if os.path.exists(p):
                print(f"✅ Using custom font: {p}")
                return ImageFont.truetype(p, size)
        except Exception as e:
            print(f"Font load failed: {p} -> {e}")

    # ⚠️ fallback (Railway system fonts)
    fallback = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]

    for p in fallback:
        try:
            if os.path.exists(p):
                return ImageFont.truetype(p, size)
        except:
            pass

    return ImageFont.load_default()



def build_dashboard_image(setup: Dict[str, Any], entry_price: float, entry_time: datetime, oisnap: Dict[str, Any]) -> bytes:
    img = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (245, 245, 245))
    draw = ImageDraw.Draw(img)

    black = (0, 0, 0)
    green = (20, 140, 70)
    red = (190, 40, 40)
    blue = (40, 90, 180)
    white = (255, 255, 255)
    soft = (255, 255, 255)
    border = (210, 210, 210)

    f_title = _load_font(46, True)
    f_h = _load_font(30, True)
    f_b = _load_font(24, False)
    f_s = _load_font(20, False)

    draw.rounded_rectangle((30, 25, IMAGE_WIDTH - 30, 120), radius=22, fill=white, outline=border, width=2)
    draw.text((55, 42), "N PATTERN ENTRY ALERT", font=f_title, fill=blue)
    draw.text((IMAGE_WIDTH - 360, 56), now_ist().strftime("%d-%m-%Y %H:%M IST"), font=f_s, fill=black)

    draw.rounded_rectangle((30, 145, IMAGE_WIDTH - 30, 420), radius=22, fill=soft, outline=border, width=2)
    y = 170
    lines = [
        ("Stock", short_symbol(setup["symbol"])),
        ("Pattern", str(setup.get("pattern", ""))),
        ("D Source", str(setup.get("touched_d_source") or "")),
        ("Touched D", f"{float(setup.get('touched_d_price') or setup.get('fib_d') or 0):.2f}"),
        ("Active D / Stop Basis", f"{float(setup.get('active_d') or 0):.2f}"),
        ("Entry", f"{entry_price:.2f}"),
        ("Target (+2%)", f"{entry_price * 1.02:.2f}" if str(setup.get("pattern", "")).lower() == "bullish" else f"{entry_price * 0.98:.2f}"),
        ("Entry Time", entry_time.astimezone(IST).strftime("%d-%m-%Y %H:%M")),
        ("OI Bias", str(oisnap.get("bias", "NA"))),
    ]
    for label, value in lines:
        draw.text((60, y), f"{label}: ", font=f_h, fill=black)
        draw.text((420, y), value, font=f_h, fill=green if label in {"Entry", "Target (+2%)", "OI Bias"} else black)
        y += 32

    draw.rounded_rectangle((30, 450, IMAGE_WIDTH - 30, IMAGE_HEIGHT - 30), radius=22, fill=white, outline=border, width=2)
    draw.text((55, 470), "OI SNAPSHOT", font=f_h, fill=black)

    x_cols = [60, 250, 430, 620, 830, 1020, 1210]
    headers = ["Strike", "CE", "CE OI Chg", "PE", "PE OI Chg", "CE Vol", "PE Vol"]
    for x, h in zip(x_cols, headers):
        draw.text((x, 520), h, font=f_b, fill=blue)

    row_y = 565
    for r in oisnap.get("rows", [])[:5]:
        strike = f"{float(r.get('strike', 0)):.0f}"
        ce_ltp = f"{float(r.get('ce_ltp', 0)):.2f}"
        ce_oich = f"{float(r.get('ce_oich', 0)):.0f}"
        pe_ltp = f"{float(r.get('pe_ltp', 0)):.2f}"
        pe_oich = f"{float(r.get('pe_oich', 0)):.0f}"
        ce_vol = f"{float(r.get('ce_vol', 0)):.0f}"
        pe_vol = f"{float(r.get('pe_vol', 0)):.0f}"
        vals = [strike, ce_ltp, ce_oich, pe_ltp, pe_oich, ce_vol, pe_vol]
        for i, (x, v) in enumerate(zip(x_cols, vals)):
            fill = black
            if i == 2:
                fill = green if float(r.get("ce_oich", 0)) >= 0 else red
            if i == 4:
                fill = green if float(r.get("pe_oich", 0)) >= 0 else red
            draw.text((x, row_y), v, font=f_b, fill=fill)
        row_y += 50

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    return bio.getvalue()


def build_sample_setup() -> Dict[str, Any]:
    return {
        "symbol": "NSE:HDFCLIFE-EQ",
        "pattern": "Bullish",
        "fib_d": 558.40,
        "trend_d": 552.10,
        "active_d": 552.10,
        "touched_d_price": 558.40,
        "touched_d_source": "Fib",
        "entry_tf": "15",
        "status": "sample_preview",
        "alert_sent": False,
    }


def maybe_send_startup_sample() -> None:
    if not SHOW_STARTUP_SAMPLE:
        log("Startup sample disabled")
        return
    if os.path.exists(STARTUP_SAMPLE_SENT_FILE):
        log("Startup sample already sent earlier; skipping")
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("Telegram credentials missing; cannot send startup sample")
        return

    sample_setup = build_sample_setup()
    sample_entry_price = 560.25
    sample_entry_time = now_ist()
    sample_oisnap = {
        "bias": "Bullish",
        "rows": [
            {"strike": 550, "ce_ltp": 14.2, "ce_oich": -1200, "pe_ltp": 4.1, "pe_oich": 1800, "ce_vol": 4200, "pe_vol": 6300},
            {"strike": 555, "ce_ltp": 10.8, "ce_oich": -900, "pe_ltp": 5.6, "pe_oich": 2200, "ce_vol": 5100, "pe_vol": 7400},
            {"strike": 560, "ce_ltp": 7.4, "ce_oich": -450, "pe_ltp": 7.9, "pe_oich": 2600, "ce_vol": 6800, "pe_vol": 8900},
            {"strike": 565, "ce_ltp": 4.8, "ce_oich": 200, "pe_ltp": 11.7, "pe_oich": 1500, "ce_vol": 3500, "pe_vol": 5600},
            {"strike": 570, "ce_ltp": 3.1, "ce_oich": 600, "pe_ltp": 16.4, "pe_oich": 900, "ce_vol": 2100, "pe_vol": 3900},
        ],
    }

    caption = (
        "🧪 STARTUP SAMPLE PREVIEW\n"
        f"Stock: {short_symbol(sample_setup['symbol'])}\n"
        f"Pattern: {sample_setup['pattern']}\n"
        f"D: {sample_setup['touched_d_source']} ({sample_setup['touched_d_price']:.2f})\n"
        f"Entry: {sample_entry_price:.2f}\n"
        f"Time: {sample_entry_time.strftime('%d-%m-%Y %H:%M IST')}\n"
        f"OI Bias: {sample_oisnap['bias']}\n"
        "This is only a startup preview image."
    )

    try:
        image_bytes = build_dashboard_image(sample_setup, sample_entry_price, sample_entry_time, sample_oisnap)
        send_telegram_photo(image_bytes, caption)
        with open(STARTUP_SAMPLE_SENT_FILE, "w", encoding="utf-8") as f:
            f.write(now_ist().strftime("%Y-%m-%d %H:%M:%S"))
        log("Startup sample preview sent once")
    except Exception as e:
        log(f"Startup sample send failed: {e}")


# =========================
# State file
# =========================
def load_setups() -> List[Dict[str, Any]]:
    if not os.path.exists(JSON_FILE):
        log(f"JSON file not found: {JSON_FILE}")
        return []
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        return json.load(f)



def save_setups(items: List[Dict[str, Any]]) -> None:
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


# =========================
# Main monitor
# =========================
def detect_touch(setup: Dict[str, Any], ltp: float) -> Tuple[Optional[float], str]:
    fib_d = float(setup.get("fib_d") or 0)
    trend_d = float(setup.get("trend_d") or 0)
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



def process_setup(setup: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str(setup.get("symbol") or "")
    if not symbol:
        return setup
    if setup.get("alert_sent"):
        return setup

    ltp = get_ltp(symbol)
    if ltp is None:
        log(f"LTP missing for {symbol}")
        return setup

    d_price, source = detect_touch(setup, ltp)
    if d_price is None:
        log(f"{symbol}: no D touch | LTP={ltp}")
        return setup

    log(f"{symbol}: touched {source} D @ {d_price} | LTP={ltp}")

    oisnap = fetch_option_chain_snapshot(symbol)
    bias = str(oisnap.get("bias", "NA"))
    log(f"{symbol}: OI bias={bias}")
    if not oi_allows_entry(str(setup.get("pattern", "")), bias):
        log(f"{symbol}: OI rejected entry")
        setup["oi_bias"] = bias
        setup["last_checked_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
        return setup

    df = fetch_history(symbol, resolution=str(setup.get("entry_tf") or "15"), days=5)
    if df is None or df.empty:
        log(f"{symbol}: lower timeframe history unavailable")
        return setup

    entry_price, entry_time, debug = check_entry_after_touch(df, d_price, str(setup.get("pattern", "Bullish")), ENTRY_ZONE_PERCENT)
    setup["oi_bias"] = bias
    setup["last_checked_at"] = now_ist().strftime("%Y-%m-%d %H:%M:%S")
    setup["last_debug_reason"] = debug.get("reason", "")

    if not entry_price or not entry_time:
        log(f"{symbol}: no entry found")
        return setup

    setup["alert_sent"] = True
    setup["entry_found"] = True
    setup["entry_price"] = round(float(entry_price), 2)
    setup["entry_time"] = entry_time.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
    setup["status"] = "entry_found"
    setup["ltp_at_alert"] = round(float(ltp), 2)
    setup["oi_bias"] = bias
    setup["touched_d_source"] = source
    setup["touched_d_price"] = round(float(d_price), 2)

    caption = (
        f"🔥 N PATTERN ENTRY\n"
        f"Stock: {short_symbol(symbol)}\n"
        f"Pattern: {setup.get('pattern', '')}\n"
        f"D: {source} ({d_price:.2f})\n"
        f"Entry: {entry_price:.2f}\n"
        f"Time: {entry_time.astimezone(IST).strftime('%d-%m-%Y %H:%M IST')}\n"
        f"OI Bias: {bias}"
    )

    if TELEGRAM_SEND_MODE == "text":
        send_telegram_text(caption)
    else:
        image_bytes = build_dashboard_image(setup, float(entry_price), entry_time, oisnap)
        send_telegram_photo(image_bytes, caption)

    log(f"{symbol}: alert sent")
    return setup



def main() -> None:
    log("N Pattern Railway live monitor with OI + dashboard image started")
    maybe_send_startup_sample()
    while True:
        try:
            if not in_market_hours():
                log("Outside market hours; sleeping")
                time.sleep(max(POLL_SECONDS, 60))
                continue

            setups = load_setups()
            if not setups:
                log("No setups found; sleeping")
                time.sleep(max(POLL_SECONDS, 60))
                continue

            updated = []
            for setup in setups:
                updated.append(process_setup(setup))
                time.sleep(RATE_LIMIT_SLEEP)

            save_setups(updated)
        except Exception as e:
            log(f"Main loop error: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

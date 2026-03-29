import os
import time
import math
import threading
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer

from fyers_apiv3 import fyersModel

try:
    from twilio.rest import Client as TwilioClient
except Exception:
    TwilioClient = None

# =========================
# Config
# =========================
ACCESS_TOKEN_FILE = os.getenv("ACCESS_TOKEN_FILE", "access_token.txt")
CLIENT_ID_FILE = os.getenv("CLIENT_ID_FILE", "client_id.txt")
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", "Nifty50.txt")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "10"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
ONLY_STRONG_ALERTS = os.getenv("ONLY_STRONG_ALERTS", "true").strip().lower() == "true"
WATCHLIST = os.getenv("WATCHLIST", "NSE:NIFTY50-INDEX,NSE:BANKNIFTY-INDEX")
LOG_PREFIX = os.getenv("LOG_PREFIX", "OIDATA-CLOUD")
PORT = int(os.getenv("PORT", "8080"))

# Optional Twilio WhatsApp
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "").strip()  # ex: whatsapp:+14155238886
TWILIO_WHATSAPP_TO = os.getenv("TWILIO_WHATSAPP_TO", "").strip()      # ex: whatsapp:+91xxxxxxxxxx

INDICES_MAP = {
    "NIFTY": "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "FINNIFTY": "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
}

DEFAULT_NIFTY50 = [
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

LAST_ALERTS = {}
LAST_HEARTBEAT = {"ts": datetime.now(), "message": "starting"}


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{LOG_PREFIX}] {msg}"
    LAST_HEARTBEAT["ts"] = datetime.now()
    LAST_HEARTBEAT["message"] = msg
    print(line, flush=True)


def read_text_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def get_access_token() -> str:
    env_token = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
    if not env_token:
        raise Exception("Missing FYERS_ACCESS_TOKEN in Railway variables")
    return env_token


def get_client_id() -> str:
    env_client = os.getenv("FYERS_CLIENT_ID", "").strip()
    if  not env_client:
        raise Exception("Missing FYERS_CLIENT_ID in Railway variables")
    return env_client


def create_fyers():
    return fyersModel.FyersModel(
        client_id=get_client_id(),
        token=get_access_token(),
        is_async=False,
        log_path=""
    )


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def display_symbol(sym: str) -> str:
    s = str(sym).strip()
    if ":" in s:
        s = s.split(":", 1)[1]
    for suffix in ["-EQ", "-INDEX", "-BE", "-BZ", "-BL", "-SM"]:
        if s.endswith(suffix):
            s = s[:-len(suffix)]
            break
    return s


def load_nifty50_symbols(file_path=SYMBOLS_FILE):
    try:
        text = Path(file_path).read_text(encoding="utf-8")
        parts = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if ":" in line and line.split(":", 1)[0].upper().startswith("NIFTY"):
                line = line.split(":", 1)[1]
            for p in line.split(","):
                p = p.strip()
                if p:
                    parts.append(p)
        cleaned = []
        for sym in parts:
            if not sym.startswith(("NSE:", "BSE:")):
                sym = f"NSE:{sym}" if sym.endswith("-EQ") else f"NSE:{sym}-EQ"
            cleaned.append(sym)
        out = []
        seen = set()
        for s in cleaned:
            if s not in seen:
                out.append(s)
                seen.add(s)
        return out or DEFAULT_NIFTY50
    except Exception:
        return DEFAULT_NIFTY50


def parse_watchlist() -> list[str]:
    raw = WATCHLIST.strip()
    if raw.upper() == "NIFTY50":
        return load_nifty50_symbols()
    symbols = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if p.upper() in INDICES_MAP:
            symbols.append(INDICES_MAP[p.upper()])
        elif not p.startswith(("NSE:", "BSE:")):
            symbols.append(f"NSE:{p}-EQ")
        else:
            symbols.append(p)
    return symbols


def fyers_history_safe(fyers, payload):
    try:
        return fyers.history(data=payload)
    except TypeError:
        return fyers.history(payload)


def fetch_quotes_map(fyers, symbols):
    if not symbols:
        return {}
    payload = {"symbols": ",".join(symbols)}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception:
        return {}
    out = {}
    items = resp.get("d") or resp.get("data") or []
    for item in items:
        sym = item.get("n") or item.get("symbol") or item.get("name") or ""
        vals = item.get("v") or item.get("values") or item
        out[sym] = {
            "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),
            "open": safe_float(vals.get("open_price") or vals.get("open"), 0.0),
            "high": safe_float(vals.get("high_price") or vals.get("high"), 0.0),
            "low": safe_float(vals.get("low_price") or vals.get("low"), 0.0),
            "prev_close": safe_float(vals.get("prev_close_price") or vals.get("prev_close") or vals.get("close"), 0.0),
        }
    return out


def fetch_single_ltp(fyers, symbol):
    q = fetch_quotes_map(fyers, [symbol])
    if symbol in q:
        return safe_float(q[symbol].get("ltp"), 0.0)
    for _, vals in q.items():
        return safe_float(vals.get("ltp"), 0.0)
    return 0.0


def fetch_option_chain(fyers, symbol, strikecount=10):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": ""}
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)


def extract_options_chain_list(resp):
    data = resp.get("data", {}) if isinstance(resp, dict) else {}
    if isinstance(data.get("optionsChain"), list):
        return data["optionsChain"]
    if isinstance(data.get("optionschain"), list):
        return data["optionschain"]
    if isinstance(data.get("options"), list):
        return data["options"]
    return []


def extract_underlying_ltp(resp):
    data = resp.get("data", {}) if isinstance(resp, dict) else {}
    for key in ["ltp", "underlying_ltp", "underlyingLtp", "underlying_price", "underlyingPrice"]:
        if key in data:
            return safe_float(data.get(key), 0.0)
    return 0.0


def normalize_chain_fast(options_list):
    call_map, put_map = {}, {}
    for x in options_list:
        strike = x.get("strike_price") or x.get("strikePrice") or x.get("strike") or x.get("sp")
        if strike is None:
            continue
        strike = safe_float(strike, None)
        if strike is None:
            continue
        option_type = str(x.get("option_type") or x.get("optionType") or x.get("type") or x.get("otype") or "").upper().strip()
        symbol = str(x.get("symbol", ""))
        row = {
            "symbol": symbol,
            "ltp": safe_float(x.get("ltp") or x.get("last_price") or x.get("lastPrice"), 0.0),
            "chg": safe_float(x.get("chg") or x.get("change") or x.get("ch"), 0.0),
            "oi": safe_float(x.get("oi") or x.get("open_interest") or x.get("openInterest")),
            "oi_change": safe_float(x.get("oich") or x.get("oi_change") or x.get("oiChange")),
            "volume": safe_float(x.get("volume") or x.get("vol") or x.get("tradedVolume") or x.get("tot_vol")),
        }
        sym_upper = symbol.upper()
        if option_type in ("CE", "CALL", "C") or sym_upper.endswith("CE"):
            call_map[int(strike)] = row
        elif option_type in ("PE", "PUT", "P") or sym_upper.endswith("PE"):
            put_map[int(strike)] = row

    strikes = sorted(set(call_map) | set(put_map))
    rows = []
    for strike in strikes:
        c = call_map.get(strike, {})
        p = put_map.get(strike, {})
        rows.append({
            "strike": strike,
            "call_oich": safe_float(c.get("oi_change"), 0.0),
            "put_oich": safe_float(p.get("oi_change"), 0.0),
            "call_oi": safe_float(c.get("oi"), 0.0),
            "put_oi": safe_float(p.get("oi"), 0.0),
            "call_ltp": safe_float(c.get("ltp"), 0.0),
            "put_ltp": safe_float(p.get("ltp"), 0.0),
        })
    return rows


def market_is_open(now=None):
    now = now or datetime.now().time()
    return dtime(9, 15) <= now <= dtime(15, 30)


def can_evaluate_15m_setup(now=None):
    now = now or datetime.now().time()
    return now >= dtime(9, 45)


def get_intraday_candles_today(fyers, symbol, resolution="15"):
    start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
    end = datetime.now()
    payload = {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": start.strftime("%Y-%m-%d"),
        "range_to": end.strftime("%Y-%m-%d"),
        "cont_flag": "1",
    }
    resp = fyers_history_safe(fyers, payload)
    candles = resp.get("candles") or []
    rows = []
    for c in candles:
        if not isinstance(c, (list, tuple)) or len(c) < 6:
            continue
        ts, o, h, l, cl, v = c[:6]
        dt = datetime.fromtimestamp(int(ts))
        if dt.date() != datetime.now().date():
            continue
        rows.append({"dt": dt, "open": safe_float(o), "high": safe_float(h), "low": safe_float(l), "close": safe_float(cl), "volume": safe_float(v)})
    rows.sort(key=lambda x: x["dt"])
    return rows


def get_first5_low(fyers, symbol):
    candles = get_intraday_candles_today(fyers, symbol, "5")
    if candles:
        return safe_float(candles[0]["low"], 0.0)
    return 0.0


def fetch_intraday_setup_only(fyers, symbol):
    try:
        candles15 = get_intraday_candles_today(fyers, symbol, "15")
        if len(candles15) < 2:
            return {
                "first15_high": 0.0, "first15_low": 0.0,
                "second15_high": 0.0, "second15_low": 0.0,
                "first15_range_pct": 0.0, "first5_low": 0.0,
                "inside15_ready": False,
            }, "not enough 15m candles"

        first, second = candles15[0], candles15[1]
        fhigh, flow = safe_float(first["high"], 0.0), safe_float(first["low"], 0.0)
        first_open = safe_float(first["open"], 0.0)
        frange_pct = ((fhigh - flow) / first_open * 100.0) if first_open > 0 else 0.0

        setup = {
            "first15_high": fhigh,
            "first15_low": flow,
            "second15_high": safe_float(second["high"], 0.0),
            "second15_low": safe_float(second["low"], 0.0),
            "first15_range_pct": frange_pct,
            "first5_low": get_first5_low(fyers, symbol),
            "inside15_ready": False,
        }

        valid_setup = (
            frange_pct < 1.0 and
            setup["second15_high"] < setup["first15_high"] and
            setup["second15_low"] > setup["first15_low"] and
            setup["second15_high"] != setup["first15_high"] and
            setup["second15_low"] != setup["first15_low"]
        )
        setup["inside15_ready"] = valid_setup
        return setup, ""
    except Exception as e:
        return {
            "first15_high": 0.0, "first15_low": 0.0,
            "second15_high": 0.0, "second15_low": 0.0,
            "first15_range_pct": 0.0, "first5_low": 0.0,
            "inside15_ready": False,
        }, str(e)


def compute_base_breakout(ltp, setup):
    if not can_evaluate_15m_setup():
        return ""
    if not setup.get("inside15_ready"):
        return ""
    first15_high = safe_float(setup.get("first15_high"), 0.0)
    first15_low = safe_float(setup.get("first15_low"), 0.0)
    if ltp > first15_high:
        return "BUY"
    if ltp < first15_low:
        return "SELL"
    return "INSIDE"


def classify_oi_confirmation(side, rows, underlying_ltp):
    if not rows or underlying_ltp <= 0:
        return side
    rows_sorted = sorted(rows, key=lambda r: safe_float(r.get("strike"), 0.0))
    atm_idx = min(range(len(rows_sorted)), key=lambda i: abs(safe_float(rows_sorted[i].get("strike"), 0.0) - underlying_ltp))

    idxs = {atm_idx}
    if side == "BUY":
        if atm_idx + 1 < len(rows_sorted):
            idxs.add(atm_idx + 1)
        if atm_idx + 2 < len(rows_sorted):
            idxs.add(atm_idx + 2)
    elif side == "SELL":
        if atm_idx - 1 >= 0:
            idxs.add(atm_idx - 1)
        if atm_idx - 2 >= 0:
            idxs.add(atm_idx - 2)

    score = 0
    put_add = 0.0
    call_add = 0.0

    for i in sorted(idxs):
        r = rows_sorted[i]
        c = safe_float(r.get("call_oich"), 0.0)
        p = safe_float(r.get("put_oich"), 0.0)
        call_add += c
        put_add += p

        if side == "BUY":
            if p > 0:
                score += 1
            if c < 0:
                score += 1
            elif c > 0:
                score -= 1
        elif side == "SELL":
            if c > 0:
                score += 1
            if p < 0:
                score += 1
            elif p > 0:
                score -= 1

    if side == "BUY":
        if put_add > abs(call_add):
            score += 1
        elif call_add > put_add:
            score -= 1
    elif side == "SELL":
        if call_add > abs(put_add):
            score += 1
        elif put_add > call_add:
            score -= 1

    if score >= 3:
        return f"{side} STRONG"
    if score <= 0:
        return f"{side} WEAK"
    return side


def twilio_client():
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TwilioClient):
        return None
    return TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def send_whatsapp_alert(message: str) -> bool:
    client = twilio_client()
    if not client:
        log(f"WhatsApp not configured. Message would be: {message}")
        return False
    if not (TWILIO_WHATSAPP_FROM and TWILIO_WHATSAPP_TO):
        log("Twilio WhatsApp numbers missing.")
        return False
    try:
        client.messages.create(from_=TWILIO_WHATSAPP_FROM, to=TWILIO_WHATSAPP_TO, body=message)
        log(f"WhatsApp sent: {message}")
        return True
    except Exception as e:
        log(f"WhatsApp send failed: {e}")
        return False


def should_alert(symbol, signal):
    if signal in ("", "INSIDE", "BUY WEAK", "SELL WEAK"):
        return False
    if ONLY_STRONG_ALERTS and signal not in ("BUY STRONG", "SELL STRONG"):
        return False
    key = f"{symbol}|{signal}"
    last_ts = LAST_ALERTS.get(key)
    now = time.time()
    if last_ts and now - last_ts < ALERT_COOLDOWN_SECONDS:
        return False
    LAST_ALERTS[key] = now
    return True


def build_alert_message(symbol, ltp, breakout, oi_signal, setup, underlying_ltp):
    name = display_symbol(symbol)
    return (
        f"{name}\n"
        f"15M BREAK: {breakout}\n"
        f"15M OI CONFIRM: {oi_signal}\n"
        f"Spot: {ltp:.2f}\n"
        f"First15 High: {safe_float(setup.get('first15_high')):.2f}\n"
        f"First15 Low: {safe_float(setup.get('first15_low')):.2f}\n"
        f"Underlying: {underlying_ltp:.2f}\n"
        f"Time: {datetime.now().strftime('%H:%M:%S')}"
    )


def scan_symbol(fyers, symbol):
    ltp = fetch_single_ltp(fyers, symbol)
    setup, setup_err = fetch_intraday_setup_only(fyers, symbol)
    breakout = compute_base_breakout(ltp, setup)

    option_rows = []
    underlying_ltp = ltp
    try:
        resp = fetch_option_chain(fyers, symbol, STRIKECOUNT)
        if isinstance(resp, dict) and resp.get("s") != "error":
            option_rows = normalize_chain_fast(extract_options_chain_list(resp))
            chain_ltp = extract_underlying_ltp(resp)
            if chain_ltp > 0:
                underlying_ltp = chain_ltp
    except Exception as e:
        log(f"{display_symbol(symbol)} option chain warning: {e}")

    if breakout in ("BUY", "SELL"):
        oi_signal = classify_oi_confirmation(breakout, option_rows, underlying_ltp)
    else:
        oi_signal = breakout

    payload = {
        "symbol": symbol,
        "name": display_symbol(symbol),
        "ltp": ltp,
        "underlying_ltp": underlying_ltp,
        "setup": setup,
        "breakout": breakout,
        "oi_signal": oi_signal,
        "setup_err": setup_err,
    }
    return payload


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return
        age = int((datetime.now() - LAST_HEARTBEAT["ts"]).total_seconds())
        body = (
            '{"status":"ok","message":"%s","heartbeat_age_seconds":%d}'
            % (LAST_HEARTBEAT["message"].replace('"', "'"), age)
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def start_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        log(f"Health server running on port {PORT}")
    except Exception as e:
        log(f"Health server failed: {e}")


def main():
    symbols = parse_watchlist()
    log(f"Watching {len(symbols)} symbol(s): {', '.join(display_symbol(s) for s in symbols)}")
    start_health_server()

    while True:
        try:
            fyers = create_fyers()
            break
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(10)

    while True:
        try:
            if not market_is_open():
                log("Market closed. Sleeping...")
                time.sleep(60)
                continue

            for symbol in symbols:
                try:
                    result = scan_symbol(fyers, symbol)
                    name = result["name"]
                    breakout = result["breakout"] or "-"
                    oi_signal = result["oi_signal"] or "-"
                    ltp = safe_float(result["ltp"], 0.0)
                    fhigh = safe_float(result["setup"].get("first15_high"), 0.0)
                    flow = safe_float(result["setup"].get("first15_low"), 0.0)
                    log(f"{name} | LTP={ltp:.2f} | 15M={breakout} | OI={oi_signal} | H={fhigh:.2f} | L={flow:.2f}")

                    if should_alert(symbol, oi_signal):
                        msg = build_alert_message(symbol, ltp, breakout, oi_signal, result["setup"], result["underlying_ltp"])
                        send_whatsapp_alert(msg)
                except Exception as e:
                    log(f"{display_symbol(symbol)} scan failed: {e}")
            time.sleep(max(5, POLL_SECONDS))
        except KeyboardInterrupt:
            log("Stopped by user.")
            break
        except Exception as e:
            log(f"Main loop error: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()

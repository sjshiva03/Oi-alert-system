import os
import math
import time
import threading
from datetime import datetime, time as dtime
from http.server import BaseHTTPRequestHandler, HTTPServer

from fyers_apiv3 import fyersModel
from twilio.rest import Client


# =========================
# ENV CONFIG
# =========================
RAW_ACCESS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
RAW_CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "8"))
ONLY_STRONG_ALERTS = os.getenv("ONLY_STRONG_ALERTS", "true").strip().lower() == "true"
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
PORT = int(os.getenv("PORT", "8080"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "4"))
BATCH_PAUSE_SECONDS = float(os.getenv("BATCH_PAUSE_SECONDS", "2.5"))
SEND_STARTUP_TEST_MESSAGE = os.getenv("SEND_STARTUP_TEST_MESSAGE", "true").strip().lower() == "true"

RISK_FREE_RATE = 0.06


# =========================
# LOG HELPERS
# =========================
def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [OIDATA-CLOUD] {msg}", flush=True)


# =========================
# GENERAL HELPERS
# =========================
def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def display_symbol_name(symbol):
    s = str(symbol).upper()
    if "NIFTYBANK" in s:
        return "BANKNIFTY"
    if "NIFTY50" in s or s.endswith("NIFTY-INDEX"):
        return "NIFTY"
    if symbol.endswith("-EQ"):
        return symbol.split(":")[-1].replace("-EQ", "")
    return symbol.split(":")[-1]


def get_today_str():
    return datetime.now().strftime("%Y-%m-%d")


def chunk_list(items, chunk_size):
    if chunk_size <= 0:
        chunk_size = 1
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


# =========================
# WATCHLIST
# =========================
def normalize_symbol(sym):
    s = str(sym).strip().upper()
    if not s:
        return ""

    if s.endswith("-INDEX") and ":" in s:
        return s

    if ":" not in s:
        s = "NSE:" + s

    if s.startswith("NSE:") or s.startswith("BSE:"):
        tail = s.split(":", 1)[1]
        if not tail.endswith("-EQ") and not tail.endswith("-INDEX"):
            s = s + "-EQ"

    return s


def get_watchlist():
    raw = os.getenv("WATCHLIST", "").strip()

    if raw:
        items = []
        normalized = raw.replace("\n", ",").replace(";", ",")
        for part in normalized.split(","):
            s = normalize_symbol(part)
            if s:
                items.append(s)

        final_list = list(dict.fromkeys(items))
        log(f"DEBUG WATCHLIST RAW = {repr(raw)}")
        log(f"DEBUG WATCHLIST PARSED = {final_list}")
        return final_list

    final_list = [
        "NSE:NIFTY50-INDEX",
        "NSE:NIFTYBANK-INDEX",
        "NSE:RELIANCE-EQ",
        "NSE:TCS-EQ",
        "NSE:HDFCBANK-EQ",
        "NSE:ICICIBANK-EQ",
        "NSE:SBIN-EQ",
    ]

    log("DEBUG WATCHLIST RAW = ''")
    log(f"DEBUG WATCHLIST PARSED = {final_list}")
    return final_list


WATCHLIST = get_watchlist()


# =========================
# TWILIO HELPERS
# =========================
def send_whatsapp_alert(message):
    sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
    auth = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    wa_from = os.getenv("TWILIO_WHATSAPP_FROM", "").strip()
    wa_to = os.getenv("TWILIO_WHATSAPP_TO", "").strip()

    log(
        f"TWILIO DEBUG -> "
        f"SID={bool(sid)}, AUTH={bool(auth)}, FROM={bool(wa_from)}, TO={bool(wa_to)}"
    )

    if not (sid and auth and wa_from and wa_to):
        log("WhatsApp not configured")
        return

    try:
        client = Client(sid, auth)
        msg = client.messages.create(
            from_=wa_from,
            to=wa_to,
            body=message
        )
        log(f"WhatsApp sent SUCCESS: {msg.sid}")
    except Exception as e:
        log(f"WhatsApp ERROR FULL: {repr(e)}")


# =========================
# FYERS HELPERS
# =========================
def get_fyers_creds():
    raw_token = RAW_ACCESS_TOKEN
    raw_client = RAW_CLIENT_ID

    if not raw_token:
        raise Exception("Missing FYERS_ACCESS_TOKEN in Railway variables")

    if raw_client:
        return raw_client, raw_token

    if ":" in raw_token:
        client_id, access_token = raw_token.split(":", 1)
        client_id = client_id.strip()
        access_token = access_token.strip()
        if not client_id or not access_token:
            raise Exception("Invalid FYERS_ACCESS_TOKEN format")
        return client_id, access_token

    raise Exception(
        "Missing FYERS_CLIENT_ID in Railway variables. "
        "Either add FYERS_CLIENT_ID separately or store FYERS_ACCESS_TOKEN as APPID:ACCESS_TOKEN"
    )


def create_fyers():
    client_id, access_token = get_fyers_creds()
    return fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path=""
    )


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
    if not isinstance(resp, dict):
        return out

    items = resp.get("d") or resp.get("data") or []
    if not isinstance(items, list):
        return out

    for item in items:
        if not isinstance(item, dict):
            continue

        sym = item.get("n") or item.get("symbol") or item.get("name") or ""
        vals = item.get("v") or item.get("values") or item

        out[sym] = {
            "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),
            "prev_close": safe_float(
                vals.get("prev_close_price")
                or vals.get("prev_close")
                or vals.get("prevClose")
                or vals.get("close")
                or vals.get("prevClosePrice"),
                None
            ),
            "chg": safe_float(vals.get("ch") or vals.get("chg") or vals.get("change"), None)
        }

    return out


def fetch_history(fyers, symbol, resolution, date_from, date_to, cont_flag="1"):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": date_from,
        "range_to": date_to,
        "cont_flag": cont_flag,
    }
    try:
        return fyers.history(data=payload)
    except TypeError:
        return fyers.history(payload)
    except Exception:
        return {}


def fetch_option_chain(fyers, symbol, strikecount=10, timestamp=""):
    payload = {
        "symbol": symbol,
        "strikecount": strikecount,
        "timestamp": timestamp
    }
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)
    except Exception:
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


def extract_underlying_ltp(resp):
    if not isinstance(resp, dict):
        return 0.0

    data = resp.get("data", {})
    if not isinstance(data, dict):
        return 0.0

    for key in ["ltp", "underlying_ltp", "underlyingLtp", "underlying_price", "underlyingPrice"]:
        if key in data:
            return safe_float(data.get(key), 0.0)

    return 0.0


def get_ltp_fallback(fyers, symbol):
    today = get_today_str()
    resp = fetch_history(fyers, symbol, "1", today, today)
    candles = resp.get("candles") or resp.get("data", {}).get("candles") or []
    if candles:
        last = candles[-1]
        if isinstance(last, list) and len(last) >= 5:
            return safe_float(last[4], 0.0)
    return 0.0


# =========================
# OPTION IV / CHAIN HELPERS
# =========================
def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price(spot, strike, time_years, rate, vol, option_type):
    if spot <= 0 or strike <= 0 or time_years <= 0 or vol <= 0:
        return 0.0

    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * time_years) / (vol * math.sqrt(time_years))
    d2 = d1 - vol * math.sqrt(time_years)

    option_type = str(option_type).upper().strip()
    if option_type == "CE":
        return spot * norm_cdf(d1) - strike * math.exp(-rate * time_years) * norm_cdf(d2)
    elif option_type == "PE":
        return strike * math.exp(-rate * time_years) * norm_cdf(-d2) - spot * norm_cdf(-d1)
    return 0.0


def implied_volatility(market_price, spot, strike, time_years, rate, option_type, max_iter=100, tolerance=1e-6):
    market_price = safe_float(market_price, 0.0)
    spot = safe_float(spot, 0.0)
    strike = safe_float(strike, 0.0)
    time_years = safe_float(time_years, 0.0)

    if market_price <= 0 or spot <= 0 or strike <= 0 or time_years <= 0:
        return 0.0

    low = 0.0001
    high = 5.0

    low_price = bs_price(spot, strike, time_years, rate, low, option_type)
    high_price = bs_price(spot, strike, time_years, rate, high, option_type)

    if market_price < low_price or market_price > high_price:
        return 0.0

    for _ in range(max_iter):
        mid = (low + high) / 2.0
        price = bs_price(spot, strike, time_years, rate, mid, option_type)
        diff = price - market_price

        if abs(diff) < tolerance:
            return mid * 100.0

        if diff > 0:
            high = mid
        else:
            low = mid

    return ((low + high) / 2.0) * 100.0


def get_time_to_expiry_from_text(expiry_text):
    try:
        expiry_dt = datetime.strptime(str(expiry_text).strip(), "%Y-%m-%d")
        expiry_dt = expiry_dt.replace(hour=15, minute=30, second=0, microsecond=0)
        seconds = (expiry_dt - datetime.now()).total_seconds()
        if seconds <= 0:
            return 0.0
        return seconds / (365.0 * 24.0 * 60.0 * 60.0)
    except Exception:
        return 0.0


def compute_option_iv(option_type, strike, ltp, underlying_ltp, expiry_text, rate=RISK_FREE_RATE):
    t = get_time_to_expiry_from_text(expiry_text)
    if t <= 0:
        return 0.0
    return implied_volatility(ltp, underlying_ltp, strike, t, rate, option_type)


def normalize_chain(options_list, quotes_map=None, underlying_ltp=0.0, expiry_text=""):
    if quotes_map is None:
        quotes_map = {}

    call_map = {}
    put_map = {}

    for x in options_list:
        if not isinstance(x, dict):
            continue

        strike = x.get("strike_price") or x.get("strikePrice") or x.get("strike") or x.get("sp")
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

        symbol = x.get("symbol", "")
        quote = quotes_map.get(symbol, {})

        ltp = safe_float(
            x.get("ltp") or x.get("last_price") or x.get("lastPrice") or quote.get("ltp"),
            0.0
        )

        prev_close = quote.get("prev_close", None)
        api_chg = safe_float(x.get("chg") or x.get("change") or x.get("ch") or quote.get("chg"), 0.0)

        if prev_close is not None:
            final_chg = ltp - prev_close
        else:
            final_chg = api_chg

        sym_upper = str(symbol).upper()
        raw_iv = x.get("iv") or x.get("IV") or x.get("implied_volatility") or x.get("impliedVolatility")
        calc_iv = safe_float(raw_iv, 0.0)
        if calc_iv <= 0 and underlying_ltp > 0 and ltp > 0 and expiry_text:
            inferred_type = option_type if option_type in ("CE", "PE") else ("CE" if sym_upper.endswith("CE") else "PE")
            calc_iv = compute_option_iv(inferred_type, strike, ltp, underlying_ltp, expiry_text)

        row = {
            "symbol": symbol,
            "ltp": ltp,
            "chg": final_chg,
            "iv": calc_iv,
            "oi": safe_float(x.get("oi") or x.get("open_interest") or x.get("openInterest")),
            "oi_change": safe_float(x.get("oich") or x.get("oi_change") or x.get("oiChange")),
            "volume": safe_float(x.get("volume") or x.get("vol") or x.get("tradedVolume") or x.get("tot_vol")),
        }

        if option_type in ("CE", "CALL", "C") or sym_upper.endswith("CE"):
            call_map[int(strike)] = row
        elif option_type in ("PE", "PUT", "P") or sym_upper.endswith("PE"):
            put_map[int(strike)] = row

    strikes = sorted(set(call_map.keys()) | set(put_map.keys()))

    final_rows = []
    for strike in strikes:
        c = call_map.get(strike, {})
        p = put_map.get(strike, {})

        final_rows.append({
            "strike": int(strike),
            "call_ltp": c.get("ltp", 0.0),
            "call_chg": c.get("chg", 0.0),
            "call_iv": c.get("iv", 0.0),
            "call_oi": c.get("oi", 0.0),
            "call_oich": c.get("oi_change", 0.0),
            "call_volume": c.get("volume", 0.0),
            "put_ltp": p.get("ltp", 0.0),
            "put_chg": p.get("chg", 0.0),
            "put_iv": p.get("iv", 0.0),
            "put_oi": p.get("oi", 0.0),
            "put_oich": p.get("oi_change", 0.0),
            "put_volume": p.get("volume", 0.0),
        })

    return final_rows


# =========================
# STRATEGY HELPERS
# =========================
def parse_candles(resp):
    if not isinstance(resp, dict):
        return []

    data = resp.get("candles") or resp.get("data", {}).get("candles") or []
    out = []
    for row in data:
        if not isinstance(row, list) or len(row) < 6:
            continue
        ts, o, h, l, c, v = row[:6]
        dt = datetime.fromtimestamp(ts)
        out.append({
            "dt": dt,
            "Open": safe_float(o),
            "High": safe_float(h),
            "Low": safe_float(l),
            "Close": safe_float(c),
            "Volume": safe_float(v),
        })
    return out


def fetch_intraday_setup_only(fyers, symbol):
    today = get_today_str()
    resp = fetch_history(fyers, symbol, "15", today, today)
    candles = parse_candles(resp)

    market_candles = [c for c in candles if dtime(9, 15) <= c["dt"].time() <= dtime(15, 30)]
    if len(market_candles) < 2:
        return None

    first = market_candles[0]
    second = market_candles[1]

    first_range_pct = 0.0
    if first["Low"] > 0:
        first_range_pct = ((first["High"] - first["Low"]) / first["Low"]) * 100.0

    second_inside_first = second["High"] <= first["High"] and second["Low"] >= first["Low"]

    return {
        "first15_high": first["High"],
        "first15_low": first["Low"],
        "second15_high": second["High"],
        "second15_low": second["Low"],
        "first15_range_pct": first_range_pct,
        "second_inside_first": second_inside_first,
    }


def classify_price_breakout(ltp, setup):
    if not setup:
        return "NO_SETUP"

    high_ = safe_float(setup.get("first15_high"), 0.0)
    low_ = safe_float(setup.get("first15_low"), 0.0)
    inside_ok = bool(setup.get("second_inside_first"))
    small_range = safe_float(setup.get("first15_range_pct"), 999.0) < 1.0

    if not inside_ok or not small_range:
        return "NO_SETUP"

    if ltp > high_:
        return "BUY"
    if ltp < low_:
        return "SELL"
    return "INSIDE"


def nearest_strike_rows(rows, underlying_ltp):
    if not rows:
        return None, None, None

    rows_sorted = sorted(rows, key=lambda r: abs(safe_float(r.get("strike"), 0.0) - underlying_ltp))
    atm = rows_sorted[0]

    all_by_strike = sorted(rows, key=lambda r: safe_float(r.get("strike"), 0.0))
    strikes = [safe_float(r["strike"], 0.0) for r in all_by_strike]
    atm_strike = safe_float(atm["strike"], 0.0)

    idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - atm_strike))
    lower_row = all_by_strike[idx - 1] if idx - 1 >= 0 else None
    upper_row = all_by_strike[idx + 1] if idx + 1 < len(all_by_strike) else None

    return lower_row, atm, upper_row


def classify_oi_confirm(price_signal, rows, underlying_ltp):
    if price_signal not in ("BUY", "SELL"):
        return price_signal

    lower_row, atm_row, upper_row = nearest_strike_rows(rows, underlying_ltp)
    if atm_row is None:
        return price_signal

    atm_call_oich = safe_float(atm_row.get("call_oich"), 0.0)
    atm_put_oich = safe_float(atm_row.get("put_oich"), 0.0)

    lower_call_oich = safe_float(lower_row.get("call_oich"), 0.0) if lower_row else 0.0
    upper_put_oich = safe_float(upper_row.get("put_oich"), 0.0) if upper_row else 0.0

    if price_signal == "SELL":
        score = 0
        if atm_call_oich > 0:
            score += 2
        if lower_call_oich > 0:
            score += 1
        if atm_put_oich <= 0:
            score += 1
        if score >= 3:
            return "SELL STRONG"
        if score <= 0:
            return "SELL WEAK"
        return "SELL"

    if price_signal == "BUY":
        score = 0
        if atm_put_oich > 0:
            score += 2
        if upper_put_oich > 0:
            score += 1
        if atm_call_oich <= 0:
            score += 1
        if score >= 3:
            return "BUY STRONG"
        if score <= 0:
            return "BUY WEAK"
        return "BUY"

    return price_signal


# =========================
# ALERT HELPERS
# =========================
signal_memory = {}


def get_signal_key(symbol, price_signal, oi_signal):
    return f"{symbol}|{price_signal}|{oi_signal}"


def should_send_smart_alert(symbol, price_signal, oi_signal, cooldown_seconds=900):
    if oi_signal == "NO_SETUP" or price_signal == "NO_SETUP":
        return False

    key = get_signal_key(symbol, price_signal, oi_signal)
    now_ts = time.time()
    last_ts = signal_memory.get(key, 0)

    if now_ts - last_ts >= cooldown_seconds:
        signal_memory[key] = now_ts
        return True
    return False


def get_trade_levels(price_signal, ltp):
    ltp = safe_float(ltp, 0.0)
    if ltp <= 0:
        return None, None

    if price_signal == "BUY":
        sl = round(ltp * 0.995, 2)
        target = round(ltp * 1.01, 2)
        return sl, target

    if price_signal == "SELL":
        sl = round(ltp * 1.005, 2)
        target = round(ltp * 0.99, 2)
        return sl, target

    return None, None


def build_smart_alert_message(symbol, ltp, price_signal, oi_signal):
    sl, target = get_trade_levels(price_signal, ltp)
    emoji = "📈" if price_signal == "BUY" else "📉"
    strength = "🔥 STRONG" if "STRONG" in oi_signal else "⚠️ NORMAL"

    return (
        f"🚨 SMART ALERT 🚨\n\n"
        f"Symbol: {display_symbol_name(symbol)}\n"
        f"{emoji} Price Signal: {price_signal}\n"
        f"📊 OI Signal: {oi_signal}\n"
        f"💰 LTP: {ltp:.2f}\n"
        f"🛑 SL: {sl if sl is not None else '-'}\n"
        f"🎯 Target: {target if target is not None else '-'}\n"
        f"{strength}\n"
        f"🕒 Time: {datetime.now().strftime('%H:%M:%S')}"
    )


# =========================
# HEALTH SERVER
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return


def run_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    log(f"Health server running on port {PORT}")
    server.serve_forever()


# =========================
# MAIN LOOP
# =========================
def run_worker():
    if SEND_STARTUP_TEST_MESSAGE:
        send_whatsapp_alert("🚀 Test message working")

    symbols = WATCHLIST
    pretty = ", ".join(display_symbol_name(s) for s in symbols)
    log(f"Watching {len(symbols)} symbol(s): {pretty}")
    log(f"Batch mode: size={BATCH_SIZE}, pause={BATCH_PAUSE_SECONDS}s")

    while True:
        try:
            fyers = create_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(10)
            continue

        batches = list(chunk_list(symbols, BATCH_SIZE))

        for batch_no, batch_symbols in enumerate(batches, start=1):
            log(f"Processing batch {batch_no}/{len(batches)}: {[display_symbol_name(s) for s in batch_symbols]}")

            batch_quotes = fetch_quotes_map(fyers, batch_symbols)

            for symbol in batch_symbols:
                try:
                    q = batch_quotes.get(symbol, {})
                    ltp = safe_float(q.get("ltp"), 0.0)

                    if ltp <= 0:
                        retry_quotes = fetch_quotes_map(fyers, [symbol])
                        q = retry_quotes.get(symbol, {})
                        ltp = safe_float(q.get("ltp"), 0.0)

                    if ltp <= 0:
                        ltp = get_ltp_fallback(fyers, symbol)

                    if ltp <= 0:
                        log(f"{display_symbol_name(symbol)} | LTP not available")
                        continue

                    setup = fetch_intraday_setup_only(fyers, symbol)
                    price_signal = classify_price_breakout(ltp, setup)

                    chain_resp = fetch_option_chain(fyers, symbol, STRIKECOUNT, "")
                    options_list = extract_options_chain_list(chain_resp)
                    underlying_ltp = extract_underlying_ltp(chain_resp)
                    if not underlying_ltp:
                        underlying_ltp = ltp

                    option_symbols = []
                    for x in options_list:
                        sym = x.get("symbol")
                        if sym:
                            option_symbols.append(sym)

                    option_quotes_map = fetch_quotes_map(fyers, option_symbols)
                    expiry_text = datetime.now().strftime("%Y-%m-%d")
                    rows = normalize_chain(options_list, option_quotes_map, underlying_ltp, expiry_text)

                    oi_signal = classify_oi_confirm(price_signal, rows, underlying_ltp)

                    if price_signal != "NO_SETUP" or oi_signal != "NO_SETUP":
                        log(
                            f"{display_symbol_name(symbol)} | "
                            f"LTP={ltp:.2f} | 15M={price_signal} | OI={oi_signal}"
                        )

                    if oi_signal == "NO_SETUP" or price_signal == "NO_SETUP":
                        continue

                    if ONLY_STRONG_ALERTS:
                        eligible = oi_signal in ("BUY STRONG", "SELL STRONG")
                    else:
                        eligible = oi_signal in (
                            "BUY STRONG", "SELL STRONG", "BUY", "SELL", "BUY WEAK", "SELL WEAK"
                        )

                    if eligible and should_send_smart_alert(
                        symbol,
                        price_signal,
                        oi_signal,
                        ALERT_COOLDOWN_SECONDS
                    ):
                        alert_msg = build_smart_alert_message(
                            symbol,
                            ltp,
                            price_signal,
                            oi_signal
                        )
                        log(f"SMART ALERT: {display_symbol_name(symbol)} -> {oi_signal}")
                        send_whatsapp_alert(alert_msg)

                except Exception as e:
                    log(f"{display_symbol_name(symbol)} | ERROR: {e}")

            if batch_no < len(batches):
                time.sleep(BATCH_PAUSE_SECONDS)

        time.sleep(max(5, POLL_SECONDS))


if __name__ == "__main__":
    threading.Thread(target=run_health_server, daemon=True).start()
    run_worker()

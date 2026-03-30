import os
import time
from datetime import datetime, timedelta, timezone
from fyers_apiv3 import fyersModel
from twilio.rest import Client


# ================= CONFIG =================
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "8"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
ONLY_STRONG_ALERTS = os.getenv("ONLY_STRONG_ALERTS", "true").strip().lower() == "true"
SEND_STARTUP_TEST_MESSAGE = os.getenv("SEND_STARTUP_TEST_MESSAGE", "true").strip().lower() == "true"

IST = timezone(timedelta(hours=5, minutes=30))


# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)


def ist_time_str():
    return now_ist().strftime("%H:%M:%S")


def ist_date_str():
    return now_ist().strftime("%Y-%m-%d")


def log(msg: str) -> None:
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [EQ-OI] {msg}", flush=True)


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def normalize_symbol(sym: str) -> str:
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
            if "NIFTY" in tail:
                s = s + "-INDEX"
            else:
                s = s + "-EQ"

    return s


def display_symbol_name(symbol: str) -> str:
    s = str(symbol).upper()
    if "NIFTYBANK" in s:
        return "BANKNIFTY"
    if "NIFTY50" in s or s.endswith("NIFTY-INDEX"):
        return "NIFTY"
    if symbol.endswith("-EQ"):
        return symbol.split(":")[-1].replace("-EQ", "")
    return symbol.split(":")[-1]


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
        log(f"WATCHLIST loaded: {final_list}")
        return final_list

    final_list = [
        "NSE:RELIANCE-EQ",
        "NSE:TCS-EQ",
        "NSE:HDFCBANK-EQ",
        "NSE:ICICIBANK-EQ",
        "NSE:NIFTY50-INDEX",
        "NSE:NIFTYBANK-INDEX",
    ]
    log(f"WATCHLIST fallback: {final_list}")
    return final_list


# ================= TWILIO =================
def send_whatsapp_alert(message: str) -> None:
    sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
    auth = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
    wa_from = os.getenv("TWILIO_WHATSAPP_FROM", "").strip()
    wa_to = os.getenv("TWILIO_WHATSAPP_TO", "").strip()

    if not (sid and auth and wa_from and wa_to):
        log("WhatsApp not configured")
        return

    try:
        client = Client(sid, auth)
        msg = client.messages.create(from_=wa_from, to=wa_to, body=message)
        log(f"WhatsApp sent: {msg.sid}")
    except Exception as e:
        log(f"WhatsApp error: {e}")


# ================= FYERS =================
def get_fyers_creds():
    raw_token = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
    raw_client = os.getenv("FYERS_CLIENT_ID", "").strip()

    if not raw_token:
        raise Exception("Missing FYERS_ACCESS_TOKEN")

    if raw_client:
        return raw_client, raw_token

    if ":" in raw_token:
        client_id, access_token = raw_token.split(":", 1)
        client_id = client_id.strip()
        access_token = access_token.strip()
        if client_id and access_token:
            return client_id, access_token

    raise Exception(
        "Missing FYERS_CLIENT_ID. Either set FYERS_CLIENT_ID separately or "
        "store FYERS_ACCESS_TOKEN as APPID:ACCESS_TOKEN"
    )


def create_fyers():
    client_id, access_token = get_fyers_creds()
    return fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path="")


def fetch_quotes_map(fyers, symbols):
    if not symbols:
        return {}

    payload = {"symbols": ",".join(symbols)}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception as e:
        log(f"quotes error: {e}")
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
        out[normalize_symbol(sym)] = {
            "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),
            "prev_close": safe_float(
                vals.get("prev_close_price")
                or vals.get("prev_close")
                or vals.get("prevClose")
                or vals.get("close")
                or vals.get("prevClosePrice"),
                0.0
            ),
            "chg": safe_float(vals.get("ch") or vals.get("chg") or vals.get("change"), 0.0),
        }

    return out


def get_ltp_from_quote(fyers, symbol: str):
    qmap = fetch_quotes_map(fyers, [symbol])
    q = qmap.get(normalize_symbol(symbol), {})
    return safe_float(q.get("ltp"), 0.0)


def fetch_option_chain(fyers, symbol: str, strikecount: int = 8, timestamp: str = ""):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": timestamp}
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)
    except Exception as e:
        log(f"optionchain error for {symbol}: {e}")
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


# ================= EQ + OPTION SEPARATE =================
def split_ce_pe_rows(options_list):
    ce_rows = []
    pe_rows = []

    for row in options_list:
        if not isinstance(row, dict):
            continue

        symbol = str(row.get("symbol", "")).upper()
        option_type = str(
            row.get("option_type")
            or row.get("optionType")
            or row.get("type")
            or row.get("otype")
            or ""
        ).upper()

        if option_type in ("CE", "CALL", "C") or symbol.endswith("CE"):
            ce_rows.append(row)
        elif option_type in ("PE", "PUT", "P") or symbol.endswith("PE"):
            pe_rows.append(row)

    return ce_rows, pe_rows


def nearest_atm_rows(ce_rows, pe_rows, underlying_ltp):
    all_rows = ce_rows + pe_rows
    if not all_rows:
        return None, None

    strikes = []
    for row in all_rows:
        strike = safe_float(
            row.get("strike_price")
            or row.get("strikePrice")
            or row.get("strike")
            or row.get("sp"),
            0.0,
        )
        if strike > 0:
            strikes.append(strike)

    if not strikes:
        return None, None

    atm_strike = min(strikes, key=lambda x: abs(x - underlying_ltp))

    atm_ce = None
    atm_pe = None

    for row in ce_rows:
        strike = safe_float(row.get("strike_price") or row.get("strikePrice") or row.get("strike") or row.get("sp"), 0.0)
        if strike == atm_strike:
            atm_ce = row
            break

    for row in pe_rows:
        strike = safe_float(row.get("strike_price") or row.get("strikePrice") or row.get("strike") or row.get("sp"), 0.0)
        if strike == atm_strike:
            atm_pe = row
            break

    return atm_ce, atm_pe


def get_row_oi_change(row):
    return safe_float(
        row.get("oich")
        or row.get("oi_change")
        or row.get("oiChange")
        or row.get("open_interest_change")
        or row.get("openInterestChange"),
        0.0,
    )


def classify_oi_signal(underlying_symbol, underlying_ltp, options_list):
    if not options_list:
        return "NO_DATA"

    ce_rows, pe_rows = split_ce_pe_rows(options_list)
    if not ce_rows and not pe_rows:
        return "NO_DATA"

    atm_ce, atm_pe = nearest_atm_rows(ce_rows, pe_rows, underlying_ltp)
    if atm_ce is None and atm_pe is None:
        return "NO_DATA"

    ce_oich = get_row_oi_change(atm_ce) if atm_ce else 0.0
    pe_oich = get_row_oi_change(atm_pe) if atm_pe else 0.0

    if pe_oich > 0 and ce_oich <= 0:
        return "BUY STRONG"
    if ce_oich > 0 and pe_oich <= 0:
        return "SELL STRONG"
    if pe_oich > ce_oich:
        return "BUY"
    if ce_oich > pe_oich:
        return "SELL"
    return "SIDEWAYS"


# ================= ALERT =================
last_alert_times = {}


def should_send_alert(symbol, signal):
    if signal in ("NO_DATA", "SIDEWAYS"):
        return False

    key = f"{symbol}|{signal}"
    now_ts = time.time()
    last_ts = last_alert_times.get(key, 0)

    if now_ts - last_ts >= ALERT_COOLDOWN_SECONDS:
        last_alert_times[key] = now_ts
        return True
    return False


def build_combined_alert_message(alert_rows):
    if not alert_rows:
        return ""

    lines = ["🚨 OI ALERT SUMMARY 🚨", ""]

    for row in alert_rows:
        lines.append(f"{row['name']} | {row['ltp']:.2f} | {row['signal']}")

    lines.append("")
    lines.append(f"Time (IST): {ist_time_str()}")

    return "\n".join(lines)


# ================= MAIN =================
def main():
    symbols = get_watchlist()

    if SEND_STARTUP_TEST_MESSAGE:
        send_whatsapp_alert(f"🚀 EQ + OPTION OI system started\nTime (IST): {ist_time_str()}")

    while True:
        try:
            fyers = create_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(10)
            continue

        alerts_to_send = []

        for symbol in symbols:
            try:
                eq_symbol = normalize_symbol(symbol)

                # 1) EQ call separately -> underlying LTP
                ltp = get_ltp_from_quote(fyers, eq_symbol)
                if ltp <= 0:
                    log(f"{eq_symbol} | LTP not available")
                    continue

                # 2) Option chain separately -> option OI/OI change
                chain_resp = fetch_option_chain(fyers, eq_symbol, STRIKECOUNT, "")
                options_list = extract_options_chain_list(chain_resp)
                signal = classify_oi_signal(eq_symbol, ltp, options_list)

                log(f"{eq_symbol} | {ltp:.2f} | {signal}")

                if ONLY_STRONG_ALERTS:
                    eligible = signal in ("BUY STRONG", "SELL STRONG")
                else:
                    eligible = signal in ("BUY STRONG", "SELL STRONG", "BUY", "SELL")

                if eligible and should_send_alert(eq_symbol, signal):
                    alerts_to_send.append({
                        "name": display_symbol_name(eq_symbol),
                        "ltp": ltp,
                        "signal": signal
                    })

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        if alerts_to_send:
            send_whatsapp_alert(build_combined_alert_message(alerts_to_send))

        time.sleep(max(5, POLL_SECONDS))


if __name__ == "__main__":
    main()

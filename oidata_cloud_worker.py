import os
import time
from datetime import datetime, timedelta
from fyers_apiv3 import fyersModel


# =====================================
# SIMPLE FYERS LTP DEBUG CHECKER
# =====================================

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))


def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] [LTP-DEBUG] {msg}", flush=True)


def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


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
        "NSE:RELIANCE-EQ",
        "NSE:TCS-EQ",
        "NSE:HDFCBANK-EQ",
        "NSE:ICICIBANK-EQ",
        "NSE:NIFTY50-INDEX",
        "NSE:NIFTYBANK-INDEX",
    ]
    log("DEBUG WATCHLIST RAW = ''")
    log(f"DEBUG WATCHLIST PARSED = {final_list}")
    return final_list


def get_fyers_creds():
    raw_token = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
    raw_client = os.getenv("FYERS_CLIENT_ID", "").strip()

    log(f"FYERS DEBUG -> CLIENT_ID={bool(raw_client)}, TOKEN={bool(raw_token)}")

    if not raw_token:
        raise Exception("Missing FYERS_ACCESS_TOKEN in environment")

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
        "Missing FYERS_CLIENT_ID. Either set FYERS_CLIENT_ID separately "
        "or store FYERS_ACCESS_TOKEN as APPID:ACCESS_TOKEN"
    )


def create_fyers():
    client_id, access_token = get_fyers_creds()
    return fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path=""
    )


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
    except Exception as e:
        log(f"history() error for {symbol} {resolution}: {e}")
        return {}


def extract_candles(resp):
    if not isinstance(resp, dict):
        return []
    return resp.get("candles") or resp.get("data", {}).get("candles") or []


def close_from_candles(candles):
    if candles:
        last = candles[-1]
        if isinstance(last, list) and len(last) >= 5:
            return safe_float(last[4], 0.0)
    return 0.0


def get_today_str():
    return datetime.now().strftime("%Y-%m-%d")


def get_prev_day_str():
    return (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def get_ltp_fallback(fyers, symbol):
    today = get_today_str()
    prev_start = get_prev_day_str()

    # 1-minute close
    resp = fetch_history(fyers, symbol, "1", today, today)
    val = close_from_candles(extract_candles(resp))
    if val > 0:
        return val, "1m_close"

    # 5-minute close
    resp = fetch_history(fyers, symbol, "5", today, today)
    val = close_from_candles(extract_candles(resp))
    if val > 0:
        return val, "5m_close"

    # Daily close
    resp = fetch_history(fyers, symbol, "D", prev_start, today)
    val = close_from_candles(extract_candles(resp))
    if val > 0:
        return val, "daily_close"

    return 0.0, "unavailable"


def fetch_raw_quote_response(fyers, symbol):
    payload = {"symbols": symbol}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception as e:
        log(f"quotes() error for {symbol}: {e}")
        return {}
    return resp


def extract_quote_ltp(raw_resp, requested_symbol):
    """
    Reads many possible quote response shapes and returns:
    (ltp, matched_symbol, raw_item)
    """
    requested_symbol = normalize_symbol(requested_symbol)

    if not isinstance(raw_resp, dict):
        return 0.0, "", None

    items = raw_resp.get("d") or raw_resp.get("data") or []
    if not isinstance(items, list):
        return 0.0, "", None

    # first pass: exact/normalized symbol match
    for item in items:
        if not isinstance(item, dict):
            continue

        sym = item.get("n") or item.get("symbol") or item.get("name") or ""
        sym_norm = normalize_symbol(sym)
        vals = item.get("v") or item.get("values") or item

        if sym_norm == requested_symbol:
            ltp = safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0)
            return ltp, sym_norm, item

    # second pass: return first nonzero ltp if only one item came back
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = item.get("n") or item.get("symbol") or item.get("name") or ""
        sym_norm = normalize_symbol(sym)
        vals = item.get("v") or item.get("values") or item
        ltp = safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0)
        if ltp > 0:
            return ltp, sym_norm, item

    return 0.0, "", None


def get_ltp(fyers, symbol):
    raw_resp = fetch_raw_quote_response(fyers, symbol)
    log(f"RAW QUOTE RESPONSE for {symbol}: {raw_resp}")

    ltp, matched_symbol, raw_item = extract_quote_ltp(raw_resp, symbol)
    if ltp > 0:
        return ltp, "quote", matched_symbol, raw_item

    ltp, source = get_ltp_fallback(fyers, symbol)
    if ltp > 0:
        return ltp, source, "", None

    return 0.0, "unavailable", "", None


def main():
    symbols = get_watchlist()

    while True:
        try:
            fyers = create_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(10)
            continue

        log(f"Checking LTP for {len(symbols)} symbol(s)")

        for symbol in symbols:
            try:
                symbol = normalize_symbol(symbol)
                log(f"Checking symbol: {symbol}")

                ltp, source, matched_symbol, raw_item = get_ltp(fyers, symbol)

                if ltp > 0:
                    if matched_symbol:
                        log(f"{symbol} | LTP={ltp:.2f} | SOURCE={source} | MATCHED={matched_symbol}")
                    else:
                        log(f"{symbol} | LTP={ltp:.2f} | SOURCE={source}")
                else:
                    log(f"{symbol} | LTP not available")

            except Exception as e:
                log(f"{symbol} | ERROR: {e}")

        time.sleep(max(5, POLL_SECONDS))


if __name__ == "__main__":
    main()

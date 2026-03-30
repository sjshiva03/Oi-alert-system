import os
import time
from datetime import datetime, timedelta, timezone

import requests
from fyers_apiv3 import fyersModel

# ================= CONFIG =================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FYERS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "900"))   # every 15 minutes
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "6"))
RR_MULTIPLIER = float(os.getenv("RR_MULTIPLIER", "2.0"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").strip().lower() == "true"

# load-friendly
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
BATCH_PAUSE_SECONDS = float(os.getenv("BATCH_PAUSE_SECONDS", "2.0"))
SYMBOL_PAUSE_SECONDS = float(os.getenv("SYMBOL_PAUSE_SECONDS", "0.25"))

IST = timezone(timedelta(hours=5, minutes=30))

WATCHLIST = [
    "NSE:ADANIENT-EQ","NSE:ADANIPORTS-EQ","NSE:APOLLOHOSP-EQ","NSE:ASIANPAINT-EQ",
    "NSE:AXISBANK-EQ","NSE:BAJAJ-AUTO-EQ","NSE:BAJFINANCE-EQ","NSE:BAJAJFINSV-EQ",
    "NSE:BEL-EQ","NSE:BHARTIARTL-EQ","NSE:BPCL-EQ","NSE:BRITANNIA-EQ",
    "NSE:CIPLA-EQ","NSE:COALINDIA-EQ","NSE:DRREDDY-EQ","NSE:EICHERMOT-EQ",
    "NSE:ETERNAL-EQ","NSE:GRASIM-EQ","NSE:HCLTECH-EQ","NSE:HDFCBANK-EQ",
    "NSE:HDFCLIFE-EQ","NSE:HEROMOTOCO-EQ","NSE:HINDALCO-EQ","NSE:HINDUNILVR-EQ",
    "NSE:ICICIBANK-EQ","NSE:INDIGO-EQ","NSE:INFY-EQ","NSE:ITC-EQ",
    "NSE:JSWSTEEL-EQ","NSE:KOTAKBANK-EQ","NSE:LT-EQ","NSE:M&M-EQ",
    "NSE:MARUTI-EQ","NSE:NESTLEIND-EQ","NSE:NTPC-EQ","NSE:ONGC-EQ",
    "NSE:POWERGRID-EQ","NSE:RELIANCE-EQ","NSE:SBILIFE-EQ","NSE:SHRIRAMFIN-EQ",
    "NSE:SBIN-EQ","NSE:SUNPHARMA-EQ","NSE:TATACONSUM-EQ","NSE:TATAMOTORS-EQ",
    "NSE:TATASTEEL-EQ","NSE:TCS-EQ","NSE:TECHM-EQ","NSE:TITAN-EQ",
    "NSE:TRENT-EQ","NSE:WIPRO-EQ"
]

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def today_ist_str():
    return now_ist().strftime("%Y-%m-%d")

def log(msg):
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [NIFTY50-15M-BOTH] {msg}", flush=True)

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def display_symbol_name(symbol):
    s = str(symbol).upper()
    if s.endswith("-EQ"):
        return s.split(":")[-1].replace("-EQ", "")
    return s.split(":")[-1]

def chunk_list(items, chunk_size):
    chunk_size = max(1, int(chunk_size))
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]

# ================= TELEGRAM =================
def send_telegram(msg):
    if not (TELEGRAM_TOKEN and CHAT_ID):
        log("Telegram not configured")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=20)
        if not r.ok:
            log(f"Telegram error: {r.text}")
    except Exception as e:
        log(f"Telegram exception: {e}")

# ================= FYERS =================
def get_fyers():
    token = FYERS_TOKEN
    client = CLIENT_ID
    if ":" in token and not client:
        client, token = token.split(":", 1)
    if not client or not token:
        raise Exception("Missing FYERS_CLIENT_ID or FYERS_ACCESS_TOKEN")
    return fyersModel.FyersModel(client_id=client, token=token, is_async=False, log_path="")

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
        out[sym] = {
            "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),
        }
    return out

def get_15min_data(fyers, symbol):
    try:
        today = today_ist_str()
        data = {
            "symbol": symbol,
            "resolution": "15",
            "date_format": "1",
            "range_from": today,
            "range_to": today,
            "cont_flag": "1",
        }
        res = fyers.history(data=data)
        candles = res.get("candles", []) or res.get("data", {}).get("candles", [])
        if len(candles) < 2:
            return None

        first = candles[0]
        second = candles[1]

        first_high = safe_float(first[2])
        first_low = safe_float(first[3])
        second_high = safe_float(second[2])
        second_low = safe_float(second[3])

        range_pct = 0.0
        if first_low > 0:
            range_pct = ((first_high - first_low) / first_low) * 100.0

        return {
            "first_high": first_high,
            "first_low": first_low,
            "second_high": second_high,
            "second_low": second_low,
            "range_pct": range_pct,
        }
    except Exception:
        return None

def fetch_option_chain(fyers, symbol, strikecount=6, timestamp=""):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": timestamp}
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

# ================= OPTION CHAIN OI =================
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

def get_row_strike(row):
    return safe_float(
        row.get("strike_price")
        or row.get("strikePrice")
        or row.get("strike")
        or row.get("sp"),
        0.0,
    )

def get_row_oi_change(row):
    return safe_float(
        row.get("oich")
        or row.get("oi_change")
        or row.get("oiChange")
        or row.get("open_interest_change")
        or row.get("openInterestChange"),
        0.0,
    )

def nearest_atm_rows(ce_rows, pe_rows, underlying_ltp):
    all_rows = ce_rows + pe_rows
    if not all_rows:
        return None, None, 0.0

    strikes = [get_row_strike(r) for r in all_rows if get_row_strike(r) > 0]
    if not strikes:
        return None, None, 0.0

    atm_strike = min(strikes, key=lambda x: abs(x - underlying_ltp))
    atm_ce = None
    atm_pe = None

    for row in ce_rows:
        if get_row_strike(row) == atm_strike:
            atm_ce = row
            break
    for row in pe_rows:
        if get_row_strike(row) == atm_strike:
            atm_pe = row
            break

    return atm_ce, atm_pe, atm_strike

def get_chain_oi_snapshot(options_list, underlying_ltp):
    ce_rows, pe_rows = split_ce_pe_rows(options_list)
    atm_ce, atm_pe, atm_strike = nearest_atm_rows(ce_rows, pe_rows, underlying_ltp)
    if atm_ce is None and atm_pe is None:
        return None
    return {
        "atm_strike": atm_strike,
        "ce_oich": get_row_oi_change(atm_ce) if atm_ce else 0.0,
        "pe_oich": get_row_oi_change(atm_pe) if atm_pe else 0.0,
    }

def classify_chain_oi_signal(snapshot):
    if not snapshot:
        return "NO_DATA"
    ce_oich = safe_float(snapshot.get("ce_oich"), 0.0)
    pe_oich = safe_float(snapshot.get("pe_oich"), 0.0)

    if pe_oich > 0 and ce_oich <= 0:
        return "BUY STRONG"
    if ce_oich > 0 and pe_oich <= 0:
        return "SELL STRONG"
    if pe_oich > ce_oich:
        return "BUY"
    if ce_oich > pe_oich:
        return "SELL"
    return "SIDEWAYS"

# ================= STRATEGY =================
def is_inside_bar(data):
    return data["second_high"] <= data["first_high"] and data["second_low"] >= data["first_low"]

def is_small_first_candle(data):
    return data["range_pct"] < 1.0

def buy_signal(ltp, data, oi_signal):
    return (
        is_inside_bar(data) and
        is_small_first_candle(data) and
        ltp > data["first_high"] and
        oi_signal in ("BUY STRONG", "BUY")
    )

def sell_signal(ltp, data, oi_signal):
    return (
        is_inside_bar(data) and
        is_small_first_candle(data) and
        ltp < data["first_low"] and
        oi_signal in ("SELL STRONG", "SELL")
    )

def build_summary(oi_rows, breakout_rows):
    lines = ["ðŸ“Š NIFTY 50 - 15 MIN SCAN", ""]

    lines.append("Strong OI:")
    if oi_rows:
        for row in oi_rows[:20]:
            lines.append(
                f"{row['name']} | {row['oi_signal']} | LTP {row['ltp']:.2f} | "
                f"ATM {row['atm']:.0f} | CE {row['ce_oich']:.0f} | PE {row['pe_oich']:.0f}"
            )
    else:
        lines.append("None")

    lines.append("")
    lines.append("Breakout Setups:")
    if breakout_rows:
        for row in breakout_rows:
            lines.append(
                f"{row['name']} | {row['side']} | Entry {row['entry']:.2f} | "
                f"SL {row['sl']:.2f} | Target {row['target']:.2f} | OI {row['oi_signal']}"
            )
    else:
        lines.append("None")

    lines.append("")
    lines.append(f"Time (IST): {ist_time_str()}")
    return "\n".join(lines)

# ================= MAIN =================
def main():
    if SEND_STARTUP_MESSAGE:
        send_telegram(
            f"ðŸš€ NIFTY 50 scanner started\n"
            f"Strong OI + breakout summary every 15 minutes\n"
            f"Time (IST): {ist_time_str()}"
        )

    while True:
        try:
            fyers = get_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(30)
            continue

        strong_oi_rows = []
        breakout_rows = []

        batches = list(chunk_list(WATCHLIST, BATCH_SIZE))
        for batch_no, batch_symbols in enumerate(batches, start=1):
            log(f"Processing batch {batch_no}/{len(batches)}")

            quotes_map = fetch_quotes_map(fyers, batch_symbols)

            for symbol in batch_symbols:
                try:
                    ltp = safe_float(quotes_map.get(symbol, {}).get("ltp"), 0.0)
                    if not ltp:
                        log(f"{symbol} | LTP not available")
                        time.sleep(SYMBOL_PAUSE_SECONDS)
                        continue

                    candle_data = get_15min_data(fyers, symbol)
                    if not candle_data:
                        log(f"{display_symbol_name(symbol)} | 15m data not available")
                        time.sleep(SYMBOL_PAUSE_SECONDS)
                        continue

                    chain_resp = fetch_option_chain(fyers, symbol, STRIKECOUNT, "")
                    options_list = extract_options_chain_list(chain_resp)
                    oi_snapshot = get_chain_oi_snapshot(options_list, ltp)
                    oi_signal = classify_chain_oi_signal(oi_snapshot)

                    if oi_snapshot and oi_signal in ("BUY STRONG", "SELL STRONG"):
                        strong_oi_rows.append({
                            "name": display_symbol_name(symbol),
                            "ltp": ltp,
                            "oi_signal": oi_signal,
                            "atm": safe_float(oi_snapshot["atm_strike"], 0.0),
                            "ce_oich": safe_float(oi_snapshot["ce_oich"], 0.0),
                            "pe_oich": safe_float(oi_snapshot["pe_oich"], 0.0),
                        })

                    if buy_signal(ltp, candle_data, oi_signal):
                        sl = candle_data["first_low"]
                        target = ltp + ((ltp - sl) * RR_MULTIPLIER)
                        breakout_rows.append({
                            "name": display_symbol_name(symbol),
                            "side": "BUY",
                            "entry": ltp,
                            "sl": sl,
                            "target": target,
                            "oi_signal": oi_signal,
                        })
                    elif sell_signal(ltp, candle_data, oi_signal):
                        sl = candle_data["first_high"]
                        target = ltp - ((sl - ltp) * RR_MULTIPLIER)
                        breakout_rows.append({
                            "name": display_symbol_name(symbol),
                            "side": "SELL",
                            "entry": ltp,
                            "sl": sl,
                            "target": target,
                            "oi_signal": oi_signal,
                        })

                    if oi_snapshot:
                        log(
                            f"{display_symbol_name(symbol)} | "
                            f"LTP={ltp:.2f} | OI={oi_signal} | "
                            f"ATM={safe_float(oi_snapshot['atm_strike'], 0.0):.0f} | "
                            f"CE_OICH={safe_float(oi_snapshot['ce_oich'], 0.0):.0f} | "
                            f"PE_OICH={safe_float(oi_snapshot['pe_oich'], 0.0):.0f}"
                        )
                    else:
                        log(f"{display_symbol_name(symbol)} | LTP={ltp:.2f} | OI=NO_DATA")

                except Exception as e:
                    log(f"{symbol} | ERROR: {e}")

                time.sleep(SYMBOL_PAUSE_SECONDS)

            if batch_no < len(batches):
                time.sleep(BATCH_PAUSE_SECONDS)

        send_telegram(build_summary(strong_oi_rows, breakout_rows))
        time.sleep(max(900, POLL_SECONDS))

if __name__ == "__main__":
    main()

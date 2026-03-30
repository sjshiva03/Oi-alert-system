import os
import time
from datetime import datetime, timedelta, timezone
import requests
from fyers_apiv3 import fyersModel

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
FYERS_TOKEN = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
CLIENT_ID = os.getenv("FYERS_CLIENT_ID", "").strip()

SNAPSHOT_SECONDS = int(os.getenv("SNAPSHOT_SECONDS", "300"))
SUMMARY_SECONDS = int(os.getenv("SUMMARY_SECONDS", "900"))
STRIKECOUNT = int(os.getenv("STRIKECOUNT", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
BATCH_PAUSE_SECONDS = float(os.getenv("BATCH_PAUSE_SECONDS", "2.0"))
SYMBOL_PAUSE_SECONDS = float(os.getenv("SYMBOL_PAUSE_SECONDS", "0.25"))
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").strip().lower() == "true"

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

def now_ist():
    return datetime.now(IST)

def ist_time_str():
    return now_ist().strftime("%H:%M:%S")

def log(msg):
    print(f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S IST')}] [NIFTY50-OICH-SEPARATE] {msg}", flush=True)

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

def human_num(v):
    v = safe_float(v, 0.0)
    if float(v).is_integer():
        return str(int(v))
    return f"{v:.2f}"

def chunk_list(items, chunk_size):
    chunk_size = max(1, int(chunk_size))
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]

def trend_label_with_icon(trend):
    t = str(trend).upper()
    if t == "SHORT BUILDUP":
        return "ðŸ”´ SHORT BUILDUP"
    if t == "LONG BUILDUP":
        return "ðŸŸ¢ LONG BUILDUP"
    if t == "SHORT COVERING":
        return "ðŸŸ¢ SHORT COVERING"
    if t == "LONG UNWINDING":
        return "ðŸ”´ LONG UNWINDING"
    return "âšª SIDEWAYS"

def send_telegram(msg):
    if not (TELEGRAM_TOKEN and CHAT_ID):
        log("Telegram not configured")
        print(msg)
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=30)
        if not r.ok:
            log(f"Telegram error: {r.text}")
    except Exception as e:
        log(f"Telegram exception: {e}")

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
        out[sym] = {"ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0)}
    return out

def fetch_option_chain(fyers, symbol, strikecount=10, timestamp=""):
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

def split_ce_pe_rows(options_list):
    ce_rows, pe_rows = [], []
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

def get_row_oi(row):
    return safe_float(row.get("oi") or row.get("open_interest") or row.get("openInterest"), 0.0)

def get_row_oich(row):
    return safe_float(
        row.get("oich")
        or row.get("oi_change")
        or row.get("oiChange")
        or row.get("open_interest_change")
        or row.get("openInterestChange"),
        0.0,
    )

def max_oi_change_row(rows):
    if not rows:
        return None
    return max(rows, key=lambda r: abs(get_row_oich(r)))

def trend_from_values(price_change, oi_change):
    chg = safe_float(price_change, 0.0)
    oich = safe_float(oi_change, 0.0)
    if chg > 0 and oich > 0:
        return "LONG BUILDUP"
    elif chg > 0 and oich < 0:
        return "SHORT COVERING"
    elif chg < 0 and oich < 0:
        return "LONG UNWINDING"
    elif chg < 0 and oich > 0:
        return "SHORT BUILDUP"
    return "SIDEWAYS"

def analyze_stock(symbol, ltp, prev_snapshot, options_list):
    ce_rows, pe_rows = split_ce_pe_rows(options_list)
    if not ce_rows and not pe_rows:
        return None

    max_ce_row = max_oi_change_row(ce_rows)
    max_pe_row = max_oi_change_row(pe_rows)
    if max_ce_row is None or max_pe_row is None:
        return None

    max_ce_strike = get_row_strike(max_ce_row)
    max_pe_strike = get_row_strike(max_pe_row)
    max_ce_oich_now = get_row_oich(max_ce_row)
    max_pe_oich_now = get_row_oich(max_pe_row)
    max_ce_oi_now = get_row_oi(max_ce_row)
    max_pe_oi_now = get_row_oi(max_pe_row)

    prev_ltp = safe_float(prev_snapshot.get("ltp"), ltp) if prev_snapshot else ltp
    price_change = ltp - prev_ltp

    prev_ce_oi = 0.0
    prev_pe_oi = 0.0
    if prev_snapshot:
        prev_ce_oi = safe_float(prev_snapshot.get("ce_oi_by_strike", {}).get(max_ce_strike), 0.0)
        prev_pe_oi = safe_float(prev_snapshot.get("pe_oi_by_strike", {}).get(max_pe_strike), 0.0)

    ce_oi_delta_5m = max_ce_oi_now - prev_ce_oi
    pe_oi_delta_5m = max_pe_oi_now - prev_pe_oi

    ce_oi_dir = "INCREASING" if ce_oi_delta_5m > 0 else ("DECREASING" if ce_oi_delta_5m < 0 else "FLAT")
    pe_oi_dir = "INCREASING" if pe_oi_delta_5m > 0 else ("DECREASING" if pe_oi_delta_5m < 0 else "FLAT")

    total_oi_delta = ce_oi_delta_5m + pe_oi_delta_5m
    total_oi_dir = "INCREASING" if total_oi_delta > 0 else ("DECREASING" if total_oi_delta < 0 else "FLAT")

    if max_pe_oich_now > 0 and max_ce_oich_now <= 0:
        dominant_signal = "STRONG BUY"
    elif max_ce_oich_now > 0 and max_pe_oich_now <= 0:
        dominant_signal = "STRONG SELL"
    elif abs(max_pe_oich_now) > abs(max_ce_oich_now):
        dominant_signal = "BUY BIAS"
    elif abs(max_ce_oich_now) > abs(max_pe_oich_now):
        dominant_signal = "SELL BIAS"
    else:
        dominant_signal = "NEUTRAL"

    trend = trend_from_values(price_change, total_oi_delta)

    snapshot = {
        "ltp": ltp,
        "ce_oi_by_strike": {get_row_strike(r): get_row_oi(r) for r in ce_rows},
        "pe_oi_by_strike": {get_row_strike(r): get_row_oi(r) for r in pe_rows},
    }

    return {
        "symbol": symbol,
        "name": display_symbol_name(symbol),
        "ltp": ltp,
        "signal": dominant_signal,
        "max_ce_strike": max_ce_strike,
        "max_pe_strike": max_pe_strike,
        "max_ce_oich": max_ce_oich_now,
        "max_pe_oich": max_pe_oich_now,
        "ce_oi_delta_5m": ce_oi_delta_5m,
        "pe_oi_delta_5m": pe_oi_delta_5m,
        "ce_oi_dir": ce_oi_dir,
        "pe_oi_dir": pe_oi_dir,
        "total_oi_delta": total_oi_delta,
        "total_oi_dir": total_oi_dir,
        "price_change_5m": price_change,
        "trend": trend,
        "snapshot": snapshot,
    }

def build_side_message(rows, side):
    if side == "BUY":
        title = "ðŸŸ¢ STRONG BUY"
        target_rows = [r for r in rows if r["signal"] == "STRONG BUY"]
    else:
        title = "ðŸ”´ STRONG SELL"
        target_rows = [r for r in rows if r["signal"] == "STRONG SELL"]

    if not target_rows:
        return f{title}
None

Time:"{ist_time_str()}"

    lines = [title, ""]
    for r in target_rows:
        lines.extend([
            title,
            r["name"],
            f"LTP:{human_num(r['ltp'])}",
            f"MAX CE OI CHANGE STRIKE:{human_num(r['max_ce_strike'])}",
            f"MAX PE OI CHANGE STRIKE:{human_num(r['max_pe_strike'])}",
            f"MAX CE OI CHANGE:{human_num(r['max_ce_oich'])}",
            f"MAX PE OI CHANGE:{human_num(r['max_pe_oich'])}",
            f"CHANGE IN OI:{human_num(r['total_oi_delta'])}",
            f"CHANGE IN OI TREND:{r['total_oi_dir']}",
            f"CE OI 5 MIN:{r['ce_oi_dir']} @ {human_num(r['max_ce_strike'])}",
            f"PE OI 5 MIN:{r['pe_oi_dir']} @ {human_num(r['max_pe_strike'])}",
            f"TREND:{trend_label_with_icon(r['trend'])}",
            title,
            ""
        ])
    lines.append(f"Time:{ist_time_str()}")
    return "\n".join(lines)

prev_snapshots = {}
last_summary_ts = 0.0

def main():
    global last_summary_ts

    if SEND_STARTUP_MESSAGE:
        send_telegram(
            "ðŸš€ Nifty 50 separate BUY/SELL OI scanner started\n"
            "Snapshot every 5 min, summary every 15 min\n"
            f"Time:{ist_time_str()}"
        )

    while True:
        try:
            fyers = get_fyers()
        except Exception as e:
            log(f"FYERS init failed: {e}")
            time.sleep(30)
            continue

        results = []
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

                    chain_resp = fetch_option_chain(fyers, symbol, STRIKECOUNT, "")
                    options_list = extract_options_chain_list(chain_resp)
                    analysis = analyze_stock(symbol, ltp, prev_snapshots.get(symbol), options_list)
                    if not analysis:
                        log(f"{display_symbol_name(symbol)} | OI NO_DATA")
                        time.sleep(SYMBOL_PAUSE_SECONDS)
                        continue

                    prev_snapshots[symbol] = analysis["snapshot"]
                    results.append(analysis)

                    log(
                        f"{analysis['name']} | {analysis['signal']} | "
                        f"LTP={human_num(analysis['ltp'])} | "
                        f"MAXCE={human_num(analysis['max_ce_strike'])} | "
                        f"MAXPE={human_num(analysis['max_pe_strike'])} | "
                        f"OI5M={analysis['total_oi_dir']} | TREND={analysis['trend']}"
                    )

                except Exception as e:
                    log(f"{symbol} | ERROR: {e}")

                time.sleep(SYMBOL_PAUSE_SECONDS)

            if batch_no < len(batches):
                time.sleep(BATCH_PAUSE_SECONDS)

        now_ts = time.time()
        if results and (now_ts - last_summary_ts >= SUMMARY_SECONDS):
            send_telegram(build_side_message(results, "SELL"))
            time.sleep(1)
            send_telegram(build_side_message(results, "BUY"))
            last_summary_ts = now_ts

        time.sleep(max(60, SNAPSHOT_SECONDS))

if __name__ == "__main__":
    main()

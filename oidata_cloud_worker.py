import os
import time
import math
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from fyers_apiv3 import fyersModel
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from bs4 import BeautifulSoup

# ================= CONFIG =================
IST = timezone(timedelta(hours=5, minutes=30))

CLIENT_ID = (os.getenv("CLIENT_ID") or "").strip()
ACCESS_TOKEN = (os.getenv("ACCESS_TOKEN") or "").strip()
TELEGRAM_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
CHAT_ID = (os.getenv("CHAT_ID") or "").strip()

WATCHLIST_RAW = (os.getenv("WATCHLIST") or "").strip()

AFTER_MARKET_RUN = (os.getenv("AFTER_MARKET_RUN", "true").strip().lower() == "true")

# Pattern filters
GAPUP_MIN_PCT = float(os.getenv("GAPUP_MIN_PCT", "0.0"))
GAPUP_CANDLE_MAX_PCT = float(os.getenv("GAPUP_CANDLE_MAX_PCT", "1.5"))
INSIDE15_FIRST_CANDLE_MIN_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MIN_PCT", "0.0"))
INSIDE15_FIRST_CANDLE_MAX_PCT = float(os.getenv("INSIDE15_FIRST_CANDLE_MAX_PCT", "2.0"))

# Trade / risk
RISK_AMOUNT = float(os.getenv("RISK_AMOUNT", "500"))
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TARGET_RR = float(os.getenv("TARGET_RR", "1.0"))
SL_BUFFER_PCT = float(os.getenv("SL_BUFFER_PCT", "0.1")) / 100.0
MAX_QTY = int(os.getenv("MAX_QTY", "100000"))

# Live tracking
LTP_INTERVAL_PER_STOCK = int(os.getenv("LTP_INTERVAL_PER_STOCK", "2"))
OI_INTERVAL_SECONDS = int(os.getenv("OI_INTERVAL_SECONDS", "180"))
OI_STOCK_GAP_SECONDS = int(os.getenv("OI_STOCK_GAP_SECONDS", "10"))
ALERT_GAP_SECONDS = int(os.getenv("ALERT_GAP_SECONDS", "300"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))

# Pivot filter
PIVOT_LTP_FILTER_PCT = float(os.getenv("PIVOT_LTP_FILTER_PCT", "3.0")) / 100.0
PIVOT_MIN_YDAY_TURNOVER = float(os.getenv("PIVOT_MIN_YDAY_TURNOVER", "0"))

NSE_HOLIDAYS_RAW = (os.getenv("NSE_HOLIDAYS") or "").strip()

if not CLIENT_ID or not ACCESS_TOKEN:
    raise Exception("Missing CLIENT_ID or ACCESS_TOKEN")

if not WATCHLIST_RAW:
    raise Exception("Missing WATCHLIST. Example: WATCHLIST=RELIANCE,TCS,HDFCBANK,ICICIBANK,INFY,M&M")

# ================= WATCHLIST =================
def convert_symbol(sym: str) -> str:
    s = sym.strip().upper()
    if not s:
        return ""
    if ":" in s:
        return s
    if s in {"NIFTY", "NIFTY50"}:
        return "NSE:NIFTY50-INDEX"
    if s == "BANKNIFTY":
        return "NSE:NIFTYBANK-INDEX"
    return f"NSE:{s}-EQ"

SYMBOLS = [convert_symbol(s) for s in WATCHLIST_RAW.split(",") if s.strip()]

# ================= GLOBAL STATE =================
watch_candidates = {}
active_trades = {}
closed_for_day = set()
blocked_entries = []
pattern_summary = {
    "gapup": [],
    "inside15": [],
    "pivot30": []
}
eod_stats = {
    "entries": [],
    "targets": [],
    "stoplosses": [],
    "dayend": [],
    "blocked": [],
    "closed": []
}
last_alert_time = {}
pivot_scan_done_keys = set()

# ================= TELEGRAM SEND =================
def send(msg: str):
    print(msg, flush=True)
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=30
        )
    except Exception as e:
        print(f"Telegram error: {e}")

# ================= HELPERS =================
def now_ist():
    return datetime.now(IST)

def now_epoch():
    return time.time()

def log(msg: str):
    print(f"[{now_ist().strftime('%H:%M:%S')}] {msg}", flush=True)

def _load_fonts():
    try:
        return {
            "title": ImageFont.truetype("DejaVuSans-Bold.ttf", 44),
            "sub": ImageFont.truetype("DejaVuSans-Bold.ttf", 24),
            "card": ImageFont.truetype("DejaVuSans-Bold.ttf", 22),
            "text": ImageFont.truetype("DejaVuSans.ttf", 16),
            "small": ImageFont.truetype("DejaVuSans.ttf", 14),
            "tiny": ImageFont.truetype("DejaVuSans.ttf", 11),
        }
    except Exception:
        f = ImageFont.load_default()
        return {"title": f, "sub": f, "card": f, "text": f, "small": f, "tiny": f}


def _text_size(draw, value, font):
    bbox = draw.textbbox((0, 0), str(value), font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _wrap_text(draw, value, font, max_width):
    words = str(value).split()
    if not words:
        return [""]
    lines, current = [], words[0]
    for word in words[1:]:
        test = current + " " + word
        if _text_size(draw, test, font)[0] <= max_width:
            current = test
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def build_rich_summary_image(items, title="SUMMARY", subtitle=""):
    fonts = _load_fonts()
    W, H = 1080, 1500
    img = Image.new("RGB", (W, H), (245, 247, 252))
    draw = ImageDraw.Draw(img)

    header_bg = (28, 36, 52)
    hero_bg = (233, 63, 51)
    buy_col = (26, 188, 156)
    sell_col = (231, 76, 60)
    neutral_col = (243, 156, 18)
    panel = (255, 255, 255)
    muted = (102, 112, 133)
    dark = (28, 33, 40)
    border = (225, 230, 238)
    soft_green = (232, 250, 242)
    soft_red = (252, 237, 236)
    soft_orange = (255, 245, 230)
    soft_blue = (236, 241, 255)
    white = (255, 255, 255)

    draw.rounded_rectangle((24, 24, W - 24, 120), radius=28, fill=header_bg)
    draw.text((44, 50), "LIVE MARKET SUMMARY", font=fonts["sub"], fill=white)
    draw.text((760, 50), analysis_date_str(), font=fonts["sub"], fill=(220, 225, 235))

    draw.rounded_rectangle((24, 140, W - 24, 265), radius=28, fill=hero_bg)
    draw.text((42, 165), title, font=fonts["title"], fill=white)
    draw.text((44, 225), subtitle or "Gap Up • 15 Min Inside • Weekly Pivot", font=fonts["text"], fill=white)

    count = len(items)
    chips = [
        ("Cards", str(count), buy_col),
        ("Symbols", str(count), neutral_col),
        ("Mode", "Live" if is_market_open() else "After", sell_col),
    ]
    x = 24
    for label, val, c in chips:
        draw.rounded_rectangle((x, 290, x + 320, 375), radius=22, fill=panel, outline=border, width=2)
        draw.rounded_rectangle((x + 18, 308, x + 110, 357), radius=16, fill=c)
        draw.text((x + 44, 319), val, font=fonts["sub"], fill=white)
        draw.text((x + 130, 320), label, font=fonts["sub"], fill=dark)
        x += 346

    draw.text((28, 405), "SUMMARY CARDS", font=fonts["sub"], fill=dark)
    draw.text((28, 444), "Rich color preview before detailed dashboard", font=fonts["tiny"], fill=muted)

    def summary_card(x, y, w, h, ctitle, lines, bg, accent):
        draw.rounded_rectangle((x, y, x + w, y + h), radius=24, fill=bg, outline=border, width=2)
        draw.rounded_rectangle((x + 18, y + 18, x + w - 18, y + 66), radius=18, fill=accent)
        title_fill = dark if accent != sell_col else white
        draw.text((x + 34, y + 28), ctitle, font=fonts["sub"], fill=title_fill)
        yy = y + 92
        for ln in lines[:3]:
            draw.text((x + 30, yy), ln, font=fonts["text"], fill=dark)
            yy += 34

    blocks = []
    for item in items[:4]:
        symbol = str(item.get("symbol", ""))
        rp = str(item.get("range_pct", "")).strip()
        header = symbol if rp in ("", "None") else f"{symbol} ({rp}%)"
        has_buy = bool(item.get("buy")) and item.get("buy", {}).get("entry", "") != ""
        has_sell = bool(item.get("sell")) and item.get("sell", {}).get("entry", "") != ""
        if has_buy and has_sell:
            lines = ["Buy & Sell setup ready", "Trigger levels prepared", "Detailed card next"]
            bg, acc = soft_orange, neutral_col
        elif has_buy:
            lines = [f"BUY Entry {item['buy'].get('entry', '')}", f"Target {item['buy'].get('target', '')}", f"SL {item['buy'].get('stoploss', '')}"]
            bg, acc = soft_green, buy_col
        elif has_sell:
            lines = [f"SELL Entry {item['sell'].get('entry', '')}", f"Target {item['sell'].get('target', '')}", f"SL {item['sell'].get('stoploss', '')}"]
            bg, acc = soft_red, sell_col
        else:
            lines = ["Watch candidate", "Pattern found", "Detailed card next"]
            bg, acc = soft_blue, (52, 152, 219)
        blocks.append((header, lines, bg, acc))

    while len(blocks) < 4:
        blocks.append(("EMPTY", ["No setup", "", ""], soft_blue, (52, 152, 219)))

    positions = [(24, 490), (556, 490), (24, 740), (556, 740)]
    for (header, lines, bg, acc), (x, y) in zip(blocks, positions):
        summary_card(x, y, 500, 220, header, lines, bg, acc)

    draw.rounded_rectangle((24, 1000, W - 24, 1165), radius=24, fill=panel, outline=border, width=2)
    draw.text((44, 1028), "HOW THIS WILL FLOW", font=fonts["sub"], fill=dark)
    flow = [
        "1. Rich color summary image first",
        "2. Then detailed dashboard result",
        "3. Live text alerts continue for entry / target / SL",
    ]
    yy = 1080
    for f in flow:
        draw.text((50, yy), f, font=fonts["text"], fill=dark)
        yy += 32

    bio = BytesIO()
    bio.name = "rich_summary.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def _dashboard_pages(items, page_size=4):
    items = list(items or [])
    if not items:
        return [[{
            "symbol": "NO SETUPS",
            "range_pct": "",
            "buy": {"entry": ""},
            "sell": {"entry": ""},
            "strategy": "No setup"
        }]]
    return [items[i:i+page_size] for i in range(0, len(items), page_size)]



def draw_rounded_rect(draw, box, radius, fill, outline=None, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def _result_to_score(result_text, has_entry=False):
    rt = str(result_text or "").upper()
    if "TARGET" in rt:
        return 98
    if "STOPLOSS" in rt:
        return 84
    if "DAY END" in rt or "EXIT" in rt:
        return 89
    if has_entry:
        return 93
    return 90


def _normalize_card_item(item):
    buy = item.get("buy", {}) or {}
    sell = item.get("sell", {}) or {}
    side = str(item.get("side", "")).upper()

    if buy and str(buy.get("entry", "")) != "":
        data = buy
        trade_side = "BUY"
    elif sell and str(sell.get("entry", "")) != "":
        data = sell
        trade_side = "SELL"
    elif side in {"BUY", "SELL"}:
        data = {
            "entry": item.get("entry", ""),
            "target": item.get("target", ""),
            "stoploss": item.get("stoploss", ""),
            "result": item.get("result", ""),
            "exit_price": item.get("exit_price", ""),
            "pl": item.get("pl", ""),
            "qty": item.get("qty", ""),
        }
        trade_side = side
    else:
        data = {"entry": "", "target": "", "stoploss": "", "result": "", "exit_price": "", "pl": "", "qty": ""}
        trade_side = "WATCH"

    score = safe_float(item.get("score", 0), 0)
    if score <= 0:
        score = _result_to_score(data.get("result", ""), has_entry=str(data.get("entry", "")) != "")

    return {
        "symbol": str(item.get("symbol", "")),
        "strategy": str(item.get("strategy", "")),
        "side": trade_side,
        "data": data,
        "rows": item.get("buy_oi_rows", []) if trade_side == "BUY" else item.get("sell_oi_rows", []) if trade_side == "SELL" else [],
        "score": int(score),
        "ltp": item.get("ltp", data.get("exit_price", "")),
    }


def _draw_dashboard_card(draw, x, y, item, fonts):
    panel_bg = (255, 255, 255)
    buy_bar = (32, 201, 151)
    sell_bar = (239, 83, 80)
    buy_box = (232, 250, 242)
    sell_box = (253, 236, 234)
    text_dark = (28, 33, 40)
    muted = (96, 108, 122)
    border = (222, 228, 235)
    profit = (18, 140, 85)
    loss = (211, 47, 47)
    neutral = (120, 130, 140)
    accent = (255, 193, 7)

    norm = _normalize_card_item(item)
    side = norm["side"]
    data = norm["data"]
    rows = norm["rows"] or []
    score = norm["score"]
    ltp = norm["ltp"]

    header_color = buy_bar if side == "BUY" else sell_bar if side == "SELL" else (52, 152, 219)
    soft_box = buy_box if side == "BUY" else sell_box if side == "SELL" else (236, 241, 255)
    title_fill = "white"

    draw_rounded_rect(draw, (x, y, x + 500, y + 360), 24, panel_bg, outline=border, width=2)
    draw_rounded_rect(draw, (x + 16, y + 16, x + 484, y + 66), 16, header_color)
    title_txt = norm["symbol"]
    if ltp not in ("", None):
        title_txt = f"{title_txt}-{ltp}"
    draw.text((x + 28, y + 28), title_txt, font=fonts["card"], fill=title_fill)
    draw.text((x + 400, y + 28), f"{score}%", font=fonts["card"], fill=title_fill)

    result_txt = str(data.get("result", "")).strip()
    status_txt = result_txt if result_txt else (f"{side} WATCH" if side != "WATCH" else "WATCH")
    strategy_line = f"{norm['strategy']} • {side} • {status_txt}" if norm["strategy"] else f"{side} • {status_txt}"
    draw_rounded_rect(draw, (x + 16, y + 82, x + 484, y + 128), 14, soft_box)
    line_fill = profit if "TARGET" in status_txt.upper() else loss if "STOPLOSS" in status_txt.upper() else text_dark
    draw.text((x + 28, y + 94), strategy_line, font=fonts["text"], fill=line_fill)

    draw.text((x + 28, y + 140), f"Entry: {data.get('entry', '')}", font=fonts["text"], fill=text_dark)
    draw.text((x + 180, y + 140), f"SL: {data.get('stoploss', '')}", font=fonts["text"], fill=text_dark)
    draw.text((x + 300, y + 140), f"Target: {data.get('target', '')}", font=fonts["text"], fill=text_dark)

    qty_txt = data.get("qty", "-") if str(data.get("qty", "")).strip() != "" else "-"
    pl = str(data.get("pl", "-"))
    pl_color = profit if pl.startswith("+") else loss if pl.startswith("-") else neutral
    lev_txt = f"{int(LEVERAGE)}X" if LEVERAGE == int(LEVERAGE) else f"{LEVERAGE}X"

    draw.text((x + 28, y + 172), f"Qty: {qty_txt}", font=fonts["text"], fill=text_dark)
    draw.text((x + 180, y + 172), f"P/L: {pl}", font=fonts["text"], fill=pl_color)
    draw.text((x + 330, y + 172), lev_txt, font=fonts["text"], fill=accent)

    draw_rounded_rect(draw, (x + 16, y + 220, x + 484, y + 338), 14, (248, 250, 252))
    draw.text((x + 28, y + 232), "Strike    PE OICh   | CE OICh", font=fonts["tiny"], fill=muted)
    oy = y + 260
    if rows:
        for r in rows[:4]:
            txt = f"{r.get('strike', '')}   {human_format(r.get('put_oich', 0))}{arrow(r.get('put_oich', 0))} | {human_format(r.get('call_oich', 0))}{arrow(r.get('call_oich', 0))}"
            draw.text((x + 28, oy), txt, font=fonts["tiny"], fill=text_dark)
            oy += 22
    else:
        draw.text((x + 28, oy), "No OI rows", font=fonts["tiny"], fill=muted)


def make_dashboard_image(items, title="STOCKS TO WATCH", subtitle="ULTIMATE DASHBOARD", page_no=1, total_pages=1):
    fonts = _load_fonts()
    W, H = 1080, 1250
    img = Image.new("RGB", (W, H), (244, 247, 252))
    draw = ImageDraw.Draw(img)

    header_bg = (229, 57, 53)
    panel_bg = (255, 255, 255)
    text_dark = (28, 33, 40)
    muted = (96, 108, 122)
    border = (222, 228, 235)
    profit = (18, 140, 85)
    loss = (211, 47, 47)
    dark_panel = (33, 43, 54)

    draw_rounded_rect(draw, (24, 24, W - 24, 190), 28, header_bg)
    draw.text((55, 40), title, font=fonts["title"], fill="white")
    draw.text((55, 106), subtitle, font=fonts["sub"], fill="white")
    right_txt = now_ist().strftime("%a, %b %d").upper()
    if total_pages > 1:
        right_txt += f"  P{page_no}/{total_pages}"
    right_w = _text_size(draw, right_txt, fonts["sub"])[0]
    draw.text((W - 55 - right_w, 106), right_txt, font=fonts["sub"], fill=(255, 235, 235))

    watch = len(items)
    active_n = len(active_trades)
    target_n = len(eod_stats.get("targets", []))
    sl_n = len(eod_stats.get("stoplosses", []))
    blocked_n = len(eod_stats.get("blocked", []))
    net_pnl = round(sum(x.get("pnl", 0.0) for x in eod_stats.get("closed", [])), 2)
    stats = [
        ("Watch", str(watch)),
        ("Active", str(active_n)),
        ("Target", str(target_n)),
        ("SL", str(sl_n)),
        ("Blocked", str(blocked_n)),
        ("Net P/L", f"₹{net_pnl:+,.0f}")
    ]

    draw_rounded_rect(draw, (24, 210, W - 24, 308), 22, dark_panel)
    x = 44
    for label, val in stats:
        draw.text((x, 228), label, font=fonts["small"], fill=(190, 205, 220))
        color = "white" if label != "Net P/L" else ((124, 255, 183) if net_pnl >= 0 else (255, 170, 170))
        draw.text((x, 255), val, font=fonts["card"], fill=color)
        x += 165

    ranked = []
    for item in items:
        norm = _normalize_card_item(item)
        ranked.append((norm["symbol"], norm["score"]))
    ranked = sorted(ranked, key=lambda z: z[1], reverse=True)[:3]

    draw_rounded_rect(draw, (24, 330, W - 24, 420), 20, panel_bg, outline=border, width=2)
    draw.text((48, 352), "TOP RANKED SETUPS", font=fonts["card"], fill=text_dark)
    rank_x = 320
    for idx, (name, score) in enumerate(ranked, 1):
        fill = profit if idx != 2 else loss
        label = f"{idx}) {name} {score}%"
        draw.text((rank_x, 356), label, font=fonts["small"], fill=fill)
        rank_x += 210
    draw.text((48, 386), "Smart filter: only strongest OI-confirmed setups shown first", font=fonts["tiny"], fill=muted)

    positions = [(24, 450), (556, 450), (24, 830), (556, 830)]
    for item, (x, y) in zip(items[:4], positions):
        _draw_dashboard_card(draw, x, y, item, fonts)

    bio = BytesIO()
    bio.name = f"ultimate_dashboard_p{page_no}.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def _after_market_cards_from_closed():
    cards = []
    for x in eod_stats.get("closed", []):
        pnl = safe_float(x.get("pnl", 0.0), 0.0)
        reason = str(x.get("reason", ""))
        cards.append({
            "symbol": x.get("symbol", ""),
            "ltp": x.get("exit", ""),
            "score": _result_to_score(reason, has_entry=True),
            "strategy": x.get("strategy", ""),
            "side": x.get("side", "SELL"),
            "result": reason,
            "entry": x.get("entry", ""),
            "stoploss": "",
            "target": "",
            "qty": "",
            "pl": f"{pnl:+.2f}",
            "pnl_value": pnl,
            "oi_rows": []
        })
    if not cards:
        cards.append({
            "symbol": "NO TRADES",
            "ltp": "",
            "score": 0,
            "strategy": "After Market",
            "side": "SELL",
            "result": "No closed trades",
            "entry": "",
            "stoploss": "",
            "target": "",
            "qty": "",
            "pl": "0.00",
            "pnl_value": 0.0,
            "oi_rows": []
        })
    return cards


def build_after_market_cards(gap_items, inside_items, pivot_items):
    cards = []

    for x in gap_items:
        pnl = safe_float(x.get("pl", 0.0), 0.0)
        cards.append({
            "symbol": x.get("symbol", ""),
            "ltp": x.get("exit_price", ""),
            "score": _result_to_score(x.get("result", ""), has_entry=True),
            "strategy": "GAPUP_PLUS",
            "side": "SELL",
            "result": x.get("result", ""),
            "entry": x.get("entry", ""),
            "stoploss": x.get("stoploss", ""),
            "target": x.get("target", ""),
            "qty": "",
            "pl": f"{pnl:+.2f}",
            "pnl_value": pnl,
            "oi_rows": []
        })

    for x in inside_items:
        for side_key, side_name in [("buy", "BUY"), ("sell", "SELL")]:
            side = x.get(side_key, {}) or {}
            pnl = safe_float(side.get("pl", 0.0), 0.0)
            cards.append({
                "symbol": x.get("symbol", ""),
                "ltp": side.get("exit_price", ""),
                "score": _result_to_score(side.get("result", ""), has_entry=True),
                "strategy": "INSIDE_15M",
                "side": side_name,
                "result": side.get("result", ""),
                "entry": side.get("entry", ""),
                "stoploss": side.get("stoploss", ""),
                "target": side.get("target", ""),
                "qty": "",
                "pl": f"{pnl:+.2f}",
                "pnl_value": pnl,
                "oi_rows": []
            })

    for x in pivot_items:
        pnl = safe_float(x.get("pl", 0.0), 0.0)
        cards.append({
            "symbol": x.get("symbol", ""),
            "ltp": x.get("exit_price", ""),
            "score": _result_to_score(x.get("result", ""), has_entry=True),
            "strategy": f"PIVOT_30M {x.get('pivot_name', '')}",
            "side": "SELL",
            "result": x.get("result", ""),
            "entry": x.get("entry", ""),
            "stoploss": x.get("stoploss", ""),
            "target": x.get("target", ""),
            "qty": "",
            "pl": f"{pnl:+.2f}",
            "pnl_value": pnl,
            "oi_rows": []
        })

    cards.sort(key=lambda z: safe_float(z.get("score", 0), 0), reverse=True)
    return cards

def _load_font(cards, title="STOCKS TO WATCH", subtitle="AFTER MARKET SUMMARY", analysis_dt=""):
    fonts = _load_fonts()

    # compact layout for 8 stocks (2 columns x 4 rows)
    W = 1080
    HEADER_H = 105
    STATS_H = 78
    RANK_H = 60
    CARD_H = 235
    GAP = 14
    PAD = 20

    total_rows = max(1, math.ceil(len(cards) / 2))
    H = PAD + HEADER_H + GAP + STATS_H + GAP + RANK_H + GAP + (total_rows * (CARD_H + GAP)) + 30

    img = Image.new("RGB", (W, H), (244, 247, 252))
    draw = ImageDraw.Draw(img)

    # colors
    red_header = (235, 51, 45)
    dark_panel = (28, 40, 58)
    white = (255, 255, 255)
    border = (225, 230, 236)
    text_dark = (30, 36, 44)
    muted = (110, 120, 130)
    buy_green = (40, 184, 120)
    sell_red = (235, 70, 80)
    soft_green = (230, 244, 236)
    soft_red = (248, 232, 233)
    pnl_green = (16, 145, 85)
    pnl_red = (211, 47, 47)
    amber = (228, 179, 18)

    # header
    draw.rounded_rectangle((PAD, PAD, W - PAD, PAD + HEADER_H), radius=28, fill=red_header)
    draw.text((PAD + 18, PAD + 12), title, font=fonts["title"], fill=white)
    draw.text((PAD + 18, PAD + 56), subtitle, font=fonts["sub"], fill=white)

    right_txt = analysis_dt or now_ist().strftime("%a, %b %d").upper()
    rw, _ = _text_size(draw, right_txt, fonts["sub"])
    draw.text((W - PAD - rw - 18, PAD + 48), right_txt, font=fonts["sub"], fill=white)

    # stats
    y_stats = PAD + HEADER_H + GAP
    draw.rounded_rectangle((PAD, y_stats, W - PAD, y_stats + STATS_H), radius=22, fill=dark_panel)

    total_watch = len(cards)
    tgt = sum(1 for c in cards if "TARGET" in str(c.get("result", "")).upper())
    sl = sum(1 for c in cards if "STOPLOSS" in str(c.get("result", "")).upper())
    exit_n = sum(1 for c in cards if any(x in str(c.get("result", "")).upper() for x in ["EXIT", "DAY END", "NO ENTRY"]))
    net_pnl = round(sum(safe_float(c.get("pnl_value", 0.0), 0.0) for c in cards), 2)

    stats = [
        ("Watch", str(total_watch)),
        ("Target", str(tgt)),
        ("Stoploss", str(sl)),
        ("Exit", str(exit_n)),
        ("Net P/L", f"₹{net_pnl:+,.0f}")
    ]

    sx = PAD + 20
    for label, value in stats:
        draw.text((sx, y_stats + 12), label, font=fonts["small"], fill=(196, 208, 221))
        col = white if label != "Net P/L" else (pnl_green if net_pnl >= 0 else pnl_red)
        draw.text((sx, y_stats + 38), value, font=fonts["card"], fill=col)
        sx += 185

    # ranked
    y_rank = y_stats + STATS_H + GAP
    draw.rounded_rectangle((PAD, y_rank, W - PAD, y_rank + RANK_H), radius=18, fill=white, outline=border, width=2)
    draw.text((PAD + 16, y_rank + 11), "TOP RANKED SETUPS", font=fonts["small"], fill=text_dark)

    ranked = sorted(cards, key=lambda x: safe_float(x.get("score", 0), 0), reverse=True)[:3]
    rx = PAD + 300
    for i, c in enumerate(ranked, 1):
        txt = f"{i}) {c.get('symbol', '')} {int(safe_float(c.get('score', 0), 0))}%"
        fill = (30, 150, 90) if i != 2 else (210, 70, 70)
        draw.text((rx, y_rank + 14), txt, font=fonts["small"], fill=fill)
        rx += 210

    def result_color(result_text):
        rt = str(result_text).upper()
        if "TARGET" in rt:
            return pnl_green
        if "STOPLOSS" in rt:
            return pnl_red
        return text_dark

    def header_color(side):
        return buy_green if str(side).upper() == "BUY" else sell_red

    def soft_color(side):
        return soft_green if str(side).upper() == "BUY" else soft_red

    def draw_after_card(x, y, item):
        side = str(item.get("side", "SELL")).upper()
        result = str(item.get("result", ""))
        strategy = str(item.get("strategy", ""))
        symbol = str(item.get("symbol", ""))
        ltp = item.get("ltp", "")
        score = int(safe_float(item.get("score", 0), 0))
        entry = item.get("entry", "")
        slv = item.get("stoploss", "")
        tgtv = item.get("target", "")
        qty = item.get("qty", "")
        pl_txt = item.get("pl", "")

        draw.rounded_rectangle((x, y, x + 500, y + CARD_H), radius=22, fill=white, outline=border, width=2)
        draw.rounded_rectangle((x + 12, y + 12, x + 488, y + 48), radius=14, fill=header_color(side))

        title_txt = f"{symbol}-{ltp}" if ltp not in ("", None) else symbol
        draw.text((x + 24, y + 18), title_txt, font=fonts["card"], fill=white)
        draw.text((x + 390, y + 18), f"{score}%", font=fonts["card"], fill=white)

        draw.rounded_rectangle((x + 12, y + 58, x + 488, y + 92), radius=10, fill=soft_color(side))
        line2 = f"{strategy} • {side} • {result}"
        draw.text((x + 22, y + 66), line2, font=fonts["text"], fill=result_color(result))

        draw.text((x + 22, y + 106), f"Entry:{entry}", font=fonts["text"], fill=text_dark)
        draw.text((x + 155, y + 106), f"SL:{slv}", font=fonts["text"], fill=text_dark)
        draw.text((x + 275, y + 106), f"Target:{tgtv}", font=fonts["text"], fill=text_dark)

        draw.text((x + 22, y + 138), f"Qty:{qty}", font=fonts["text"], fill=text_dark)
        draw.text(
            (x + 145, y + 138),
            f"P/L:{pl_txt}",
            font=fonts["text"],
            fill=(pnl_green if str(pl_txt).startswith("+") else pnl_red if str(pl_txt).startswith("-") else muted)
        )
        draw.text((x + 315, y + 138), f"{int(LEVERAGE)}X", font=fonts["text"], fill=amber)

        draw.rounded_rectangle((x + 12, y + 176, x + 488, y + 214), radius=10, fill=(242, 243, 246))
        draw.text((x + 22, y + 184), "Exit Type", font=fonts["small"], fill=muted)
        draw.text((x + 145, y + 184), result.upper(), font=fonts["small"], fill=result_color(result))
        draw.text((x + 22, y + 202), "Realized result recorded in after-market book", font=fonts["tiny"], fill=text_dark)

    start_y = y_rank + RANK_H + GAP
    current_y = start_y
    col = 0
    positions = [(PAD, current_y), (556, current_y)]

    for idx, item in enumerate(cards):
        if idx > 0 and idx % 2 == 0:
            current_y += CARD_H + GAP
            positions = [(PAD, current_y), (556, current_y)]
            col = 0

        draw_after_card(positions[col][0], positions[col][1], item)
        col += 1

    bio = BytesIO()
    bio.name = "after_market_dashboard.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio

def send_rich_summary_image(items, title="SUMMARY", subtitle="", caption=""):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        img_bytes = build_rich_summary_image(items, title=title, subtitle=subtitle)
        files = {"photo": ("rich_summary.png", img_bytes, "image/png")}
        data = {"chat_id": CHAT_ID, "caption": caption[:1024] if caption else ""}
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", data=data, files=files, timeout=60)
    except Exception as e:
        log(f"Rich summary image error: {e}")


def send_dashboard_image(items, title="STOCKS TO WATCH", subtitle="ULTIMATE DASHBOARD", caption=""):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        pages = _dashboard_pages(items, page_size=4)
        total_pages = len(pages)
        for idx, page_items in enumerate(pages, 1):
            img_bytes = make_dashboard_image(page_items, title=title, subtitle=subtitle, page_no=idx, total_pages=total_pages)
            files = {"photo": (f"ultimate_dashboard_p{idx}.png", img_bytes, "image/png")}
            page_caption = caption
            if total_pages > 1:
                page_caption = f"{caption} ({idx}/{total_pages})" if caption else f"Page {idx}/{total_pages}"
            data = {"chat_id": CHAT_ID, "caption": page_caption[:1024] if page_caption else ""}
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", data=data, files=files, timeout=60)
    except Exception as e:
        log(f"Dashboard image error: {e}")


def send_after_market_summary_image(cards=None, caption="After Market Summary"):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        img_bytes = _load_font(
            cards=cards,
            title="STOCKS TO WATCH",
            subtitle="AFTER MARKET SUMMARY",
            analysis_dt=analysis_date_str().upper()
        )
        files = {"photo": ("after_market_dashboard.png", img_bytes, "image/png")}
        data = {"chat_id": CHAT_ID, "caption": caption[:1024] if caption else ""}
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data=data,
            files=files,
            timeout=60
        )
    except Exception as e:
        log(f"After market summary image error: {e}")


def build_live_trade_image(trade, ltp=None, status=None, oi_rows=None, header_title="STOCKS TO WATCH", reason_text=""):
    fonts = _load_fonts()
    W, H = 1080, 820
    img = Image.new("RGB", (W, H), (245, 247, 252))
    draw = ImageDraw.Draw(img)

    side = trade["side"]
    is_buy = side == "BUY"

    main_color = (20, 160, 80) if is_buy else (220, 40, 40)
    soft_color = (230, 255, 240) if is_buy else (255, 235, 235)
    text_dark = (28, 33, 40)
    muted = (96, 108, 122)

    draw_rounded_rect(draw, (20, 20, W - 20, 120), 25, (230, 50, 50))
    draw.text((40, 45), header_title, fill="white", font=fonts["title"])

    draw_rounded_rect(draw, (40, 150, 1040, 780), 25, "white", outline=(200, 200, 200), width=2)

    draw_rounded_rect(draw, (60, 170, 1020, 230), 20, main_color)
    ltp_txt = round(ltp, 2) if isinstance(ltp, (int, float)) else ltp
    draw.text((80, 185), f"{short_name(trade['symbol'])}-{ltp_txt}", fill="white", font=fonts["card"])
    draw.text((900, 185), "LIVE", fill="white", font=fonts["card"])

    strategy_line = f"{trade['strategy']} • {side} • {status}"
    draw_rounded_rect(draw, (60, 250, 1020, 300), 15, soft_color)
    draw.text((80, 260), strategy_line, fill=main_color, font=fonts["text"])

    draw.text((80, 320), f"Entry: {trade['entry']}", font=fonts["text"], fill=text_dark)
    draw.text((350, 320), f"SL: {trade['stoploss']}", font=fonts["text"], fill=text_dark)
    draw.text((600, 320), f"Target: {trade['target']}", font=fonts["text"], fill=text_dark)

    pnl = safe_float(trade.get("pnl", 0), 0)
    pnl_color = (0, 150, 0) if pnl >= 0 else (200, 0, 0)

    draw.text((80, 360), f"Qty: {trade.get('qty', 0)}", font=fonts["text"], fill=text_dark)
    draw.text((350, 360), f"P/L: ₹{pnl:.2f}", fill=pnl_color, font=fonts["text"])
    lev_txt = f"{int(LEVERAGE)}X" if LEVERAGE == int(LEVERAGE) else f"{LEVERAGE}X"
    draw.text((600, 360), lev_txt, fill=(255, 170, 0), font=fonts["text"])

    if reason_text:
        draw.text((80, 395), reason_text, font=fonts["small"], fill=muted)

    y = 450
    draw.text((80, y), "Strike   PE OI   ΔPE   |   CE OI   ΔCE", font=fonts["small"], fill=muted)
    y += 35

    for r in (oi_rows or [])[:5]:
        txt = f"{r['strike']}   {human_format(r['put_oi'])}   {human_format(r['put_oich'])}{arrow(r['put_oich'])} | {human_format(r['call_oi'])}   {human_format(r['call_oich'])}{arrow(r['call_oich'])}"
        draw.text((80, y), txt, font=fonts["small"], fill=text_dark)
        y += 28

    bio = BytesIO()
    bio.name = "live_trade.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def send_live_trade_image(trade, ltp=None, status=None, oi_rows=None,
                         header_title="STOCKS TO WATCH",
                         reason_text="",
                         caption=""):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return

    try:
        img = build_live_trade_image(
            trade,
            ltp=ltp,
            status=status,
            oi_rows=oi_rows,
            header_title=header_title,
            reason_text=reason_text
        )

        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={
                "chat_id": CHAT_ID,
                "caption": caption[:1024] if caption else ""
            },
            files={"photo": ("dashboard.png", img, "image/png")},
            timeout=60
        )
    except Exception as e:
        log(f"Image send error: {e}")
def text_to_image_bytes(text, width=1200, padding=30, line_gap=12, font_size=24):
    lines = str(text).split("\n")
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", font_size)
    except Exception:
        font = ImageFont.load_default()

    dummy = Image.new("RGB", (width, 100), "white")
    draw = ImageDraw.Draw(dummy)
    line_heights = []
    max_width = 0
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        line_heights.append(h)
        max_width = max(max_width, w)

    img_width = max(width, max_width + padding * 2)
    img_height = padding * 2 + sum(line_heights) + line_gap * max(0, len(lines) - 1)

    img = Image.new("RGB", (img_width, img_height), "white")
    draw = ImageDraw.Draw(img)
    y = padding
    for i, line in enumerate(lines):
        draw.text((padding, y), line, fill="black", font=font)
        y += line_heights[i] + line_gap

    bio = BytesIO()
    bio.name = "report.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def send_photo_from_text(text, caption=""):
    print(text, flush=True)
    if not TELEGRAM_TOKEN or not CHAT_ID:
        return
    try:
        img_bytes = text_to_image_bytes(text)
        files = {"photo": ("report.png", img_bytes, "image/png")}
        data = {"chat_id": CHAT_ID, "caption": caption[:1024] if caption else ""}
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data=data,
            files=files,
            timeout=60
        )
    except Exception as e:
        log(f"Telegram photo error: {e}")


def convert_inside_items_for_dashboard(inside_items):
    cards = []
    for x in inside_items:
        cards.append({
            "symbol": x.get("symbol", ""),
            "strategy": x.get("strategy", ""),
            "range_pct": x.get("range_pct", ""),
            "buy": {
                "entry": x.get("buy", {}).get("entry", ""),
                "target": x.get("buy", {}).get("target", ""),
                "stoploss": x.get("buy", {}).get("stoploss", ""),
                "result": x.get("buy", {}).get("result", ""),
                "exit_price": x.get("buy", {}).get("exit_price", ""),
                "pl": x.get("buy", {}).get("pl", ""),
            },
            "sell": {
                "entry": x.get("sell", {}).get("entry", ""),
                "target": x.get("sell", {}).get("target", ""),
                "stoploss": x.get("sell", {}).get("stoploss", ""),
                "result": x.get("sell", {}).get("result", ""),
                "exit_price": x.get("sell", {}).get("exit_price", ""),
                "pl": x.get("sell", {}).get("pl", ""),
            },
        })
    return cards


def convert_gapup_items_for_dashboard(gap_items):
    cards = []
    for x in gap_items:
        cards.append({
            "symbol": x.get("symbol", ""),
            "strategy": x.get("strategy", ""),
            "range_pct": x.get("gap_pct", ""),
            "buy": {},
            "sell": {
                "entry": x.get("entry", ""),
                "target": x.get("target", ""),
                "stoploss": x.get("stoploss", ""),
                "result": x.get("result", ""),
                "exit_price": x.get("exit_price", ""),
                "pl": x.get("pl", ""),
            },
        })
    return cards


def convert_pivot_items_for_dashboard(pivot_items):
    cards = []
    for x in pivot_items:
        level_txt = f"{x.get('pivot_name', '')}={x.get('pivot_value', '')}"
        cards.append({
            "symbol": f"{x.get('symbol', '')} [{level_txt}]",
            "strategy": x.get("strategy", ""),
            "range_pct": "",
            "buy": {},
            "sell": {
                "entry": x.get("entry", ""),
                "target": x.get("target", ""),
                "stoploss": x.get("stoploss", ""),
                "result": x.get("result", ""),
                "exit_price": x.get("exit_price", ""),
                "pl": x.get("pl", ""),
            },
        })
    return cards

def send_long_message(text: str, chunk_size: int = 3500):
    if not text:
        return
    while len(text) > chunk_size:
        cut = text.rfind("\n", 0, chunk_size)
        if cut == -1:
            cut = chunk_size
        send(text[:cut])
        text = text[cut:].lstrip()
    if text:
        send(text)

def throttle_ok(key: str) -> bool:
    t = last_alert_time.get(key, 0)
    if now_epoch() - t >= ALERT_GAP_SECONDS:
        last_alert_time[key] = now_epoch()
        return True
    return False

def short_name(symbol: str) -> str:
    right = symbol.split(":")[1]
    return right.replace("-EQ", "").replace("-INDEX", "")

def candle_dt(ts: int):
    return datetime.fromtimestamp(ts, IST)

def pct_range(high: float, low: float, close: float) -> float:
    if close == 0:
        return 0.0
    return ((high - low) / close) * 100.0

def safe_float(x, default=0.0):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default

def human_format(n):
    n = safe_float(n, 0.0)
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 10000000:
        return f"{sign}{(n/10000000):.2f}".rstrip("0").rstrip(".") + "Cr"
    if n >= 100000:
        return f"{sign}{(n/100000):.2f}".rstrip("0").rstrip(".") + "L"
    if n >= 1000:
        return f"{sign}{(n/1000):.2f}".rstrip("0").rstrip(".") + "K"
    if float(n).is_integer():
        return f"{sign}{int(n)}"
    return f"{sign}{n:.2f}".rstrip("0").rstrip(".")

def arrow(v):
    v = safe_float(v, 0.0)
    if v > 0:
        return "↑"
    if v < 0:
        return "↓"
    return "→"

def dedupe_candles_by_ts(candles):
    seen = {}
    for c in candles:
        try:
            ts = int(c[0])
            seen[ts] = c
        except Exception:
            pass
    out = list(seen.values())
    out.sort(key=lambda x: x[0])
    return out

# ================= MARKET TIME =================
def fetch_nse_holidays_from_web(year=None):
    if year is None:
        year = now_ist().year

    url = "https://www.nseindia.com/resources/exchange-communication-holidays"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.headers.update(headers)

    try:
        session.get("https://www.nseindia.com", timeout=20)
        r = session.get(url, timeout=20)
        r.raise_for_status()
    except Exception as e:
        log(f"Holiday fetch failed: {e}")
        return set()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text("\n", strip=True)

    holidays = set()
    for token in text.replace(",", " ").split():
        token = token.strip()
        try:
            dt = datetime.strptime(token, "%d-%b-%Y")
            if dt.year == year:
                holidays.add(dt.strftime("%Y-%m-%d"))
        except Exception:
            pass

    return holidays


def get_holiday_set():
    env_holidays = set()
    for part in NSE_HOLIDAYS_RAW.replace(";", ",").split(","):
        p = part.strip()
        if p:
            env_holidays.add(p)

    web_holidays = fetch_nse_holidays_from_web(now_ist().year)
    merged = env_holidays | web_holidays

    log(f"NSE holidays loaded: {sorted(list(merged))}")
    return merged


try:
    HOLIDAYS = get_holiday_set()
except Exception as e:
    log(f"Holiday init failed: {e}")
    HOLIDAYS = set()

def is_market_day(dt_obj):
    return dt_obj.weekday() < 5 and dt_obj.strftime("%Y-%m-%d") not in HOLIDAYS

def is_market_open():
    now = now_ist()
    return is_market_day(now) and dtime(9, 15) <= now.time() <= dtime(15, 30)

def next_market_open_datetime():
    now = now_ist()
    if is_market_day(now) and now.time() < dtime(9, 15):
        return now.replace(hour=9, minute=15, second=0, microsecond=0)

    nxt = (now + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    while not is_market_day(nxt):
        nxt = (nxt + timedelta(days=1)).replace(hour=9, minute=15, second=0, microsecond=0)
    return nxt

def sleep_until_next_market_open():
    nxt = next_market_open_datetime()
    log(f"Sleeping until {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
    while True:
        rem = (nxt - now_ist()).total_seconds()
        if rem <= 1:
            return
        time.sleep(min(60, max(1, int(rem))))

def get_reference_symbol():
    for sym in SYMBOLS:
        if sym.endswith("-INDEX"):
            return sym
    return SYMBOLS[0]



def get_last_available_session_date():
    ref_symbol = get_reference_symbol()
    candles = get_history(ref_symbol, 5, 10)

    if not candles:
        return now_ist().strftime("%Y-%m-%d")

    try:
        ts = int(candles[-1][0])
        last_dt = candle_dt(ts)
        return last_dt.strftime("%Y-%m-%d")
    except Exception as e:
        log(f"Session date parse error: {e}")
        return now_ist().strftime("%Y-%m-%d")


def analysis_date_str():
    return get_last_available_session_date()


def log_analysis_date_debug():
    ref_symbol = get_reference_symbol()
    candles = get_history(ref_symbol, 5, 10)

    if candles and isinstance(candles[-1], list) and len(candles[-1]) > 0:
        try:
            ts = int(candles[-1][0])
            last_dt = candle_dt(ts).strftime("%Y-%m-%d %H:%M")

            log(f"Reference symbol: {ref_symbol}")
            log(f"Latest candle from FYERS: {last_dt}")
            log(f"Analysis date selected: {analysis_date_str()}")

        except Exception as e:
            log(f"Debug parse error: {e}")
    else:
        log(f"Reference symbol: {ref_symbol}")
        log("No valid candles returned from FYERS")

# ================= FYERS =================
fyers = fyersModel.FyersModel(
    client_id=CLIENT_ID,
    token=ACCESS_TOKEN,
    is_async=False,
    log_path=""
)

def check_auth():
    profile = fyers.get_profile()
    if profile.get("s") != "ok":
        raise Exception(f"FYERS auth failed: {profile}")
    return profile

def get_history(symbol, resolution, days=20):
    payload = {
        "symbol": symbol,
        "resolution": str(resolution),
        "date_format": "1",
        "range_from": (now_ist() - timedelta(days=days)).strftime("%Y-%m-%d"),
        "range_to": now_ist().strftime("%Y-%m-%d"),
        "cont_flag": "1"
    }
    try:
        data = fyers.history(data=payload)
    except TypeError:
        data = fyers.history(payload)
    except Exception as e:
        log(f"HISTORY ERROR {symbol} {resolution}: {e}")
        return []
    return dedupe_candles_by_ts(data.get("candles", []))

def get_analysis_day_candles(symbol, resolution, days=20):
    candles = get_history(symbol, resolution, days)
    target_day = analysis_date_str()
    out = []
    for c in candles:
        try:
            if candle_dt(c[0]).strftime("%Y-%m-%d") == target_day:
                out.append(c)
        except Exception:
            pass
    out = dedupe_candles_by_ts(out)
    out.sort(key=lambda x: x[0])
    return out

def get_previous_daily(symbol):
    daily = get_history(symbol, "D", 40)
    today_str = now_ist().strftime("%Y-%m-%d")

    prev = []
    for c in daily:
        try:
            c_day = candle_dt(c[0]).strftime("%Y-%m-%d")
            if c_day < today_str:
                prev.append(c)
        except Exception:
            pass

    prev.sort(key=lambda x: x[0])
    return prev[-1] if prev else None


def get_previous_weekly(symbol):
    weekly = get_history(symbol, "W", 80)
    today_str = now_ist().strftime("%Y-%m-%d")

    prev = []
    for c in weekly:
        try:
            c_day = candle_dt(c[0]).strftime("%Y-%m-%d")
            if c_day < today_str:
                prev.append(c)
        except Exception:
            pass

    prev.sort(key=lambda x: x[0])
    return prev[-1] if prev else None


def fetch_quotes(symbol):
    payload = {"symbols": symbol}
    try:
        resp = fyers.quotes(data=payload)
    except TypeError:
        resp = fyers.quotes(payload)
    except Exception as e:
        log(f"QUOTES ERROR {symbol}: {e}")
        return {}

    items = resp.get("d") or []
    if not items:
        return {}

    item = items[0] if isinstance(items[0], dict) else {}
    vals = item.get("v") or {}

    return {  
    "ltp": safe_float(vals.get("lp") or vals.get("ltp") or vals.get("last_price"), 0.0),  
    "open": safe_float(vals.get("open_price") or vals.get("open") or vals.get("openPrice"), 0.0),  
    "high": safe_float(vals.get("high_price") or vals.get("high") or vals.get("highPrice"), 0.0),  
    "low": safe_float(vals.get("low_price") or vals.get("low") or vals.get("lowPrice"), 0.0),  
    "prev_close": safe_float(vals.get("prev_close_price") or vals.get("prev_close") or vals.get("prevClose"), 0.0),  
    }

def fetch_option_chain(symbol, strikecount=10, timestamp=""):
    payload = {"symbol": symbol, "strikecount": strikecount, "timestamp": timestamp}
    try:
        return fyers.optionchain(data=payload)
    except TypeError:
        return fyers.optionchain(payload)
    except Exception as e:
        log(f"OPTIONCHAIN ERROR {symbol}: {e}")
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

def normalize_chain_fast(options_list):
    call_map = {}
    put_map = {}

    for x in options_list:
        if not isinstance(x, dict):
            continue

        strike = (
            x.get("strike_price")
            or x.get("strikePrice")
            or x.get("strike")
            or x.get("sp")
        )
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

        sym = str(x.get("symbol", "")).upper()

        row = {
            "ltp": safe_float(x.get("ltp") or x.get("last_price") or x.get("lastPrice"), 0.0),
            "chg": safe_float(x.get("chg") or x.get("change") or x.get("ch"), 0.0),
            "iv": safe_float(x.get("iv") or x.get("implied_volatility") or x.get("impliedVolatility"), 0.0),
            "oi": safe_float(x.get("oi") or x.get("open_interest") or x.get("openInterest"), 0.0),
            "oi_change": safe_float(x.get("oich") or x.get("oi_change") or x.get("oiChange"), 0.0),
            "volume": safe_float(x.get("volume") or x.get("vol") or x.get("tradedVolume") or x.get("tot_vol"), 0.0),
        }

        if option_type in ("CE", "CALL", "C") or sym.endswith("CE"):
            call_map[int(strike)] = row
        elif option_type in ("PE", "PUT", "P") or sym.endswith("PE"):
            put_map[int(strike)] = row

    strikes = sorted(set(call_map.keys()) | set(put_map.keys()))
    rows = []
    for strike in strikes:
        c = call_map.get(strike, {})
        p = put_map.get(strike, {})
        rows.append({
            "strike": int(strike),
            "call_oi": c.get("oi", 0.0),
            "call_oich": c.get("oi_change", 0.0),
            "put_oi": p.get("oi", 0.0),
            "put_oich": p.get("oi_change", 0.0),
        })
    return rows



def convert_gapup_summary_for_dashboard(items):
    cards = []
    for x in items:
        cards.append({
            "symbol": short_name(x.get("symbol", "")),
            "strategy": x.get("strategy", ""),
            "range_pct": x.get("gap_pct", ""),
            "buy": {},
            "sell": {}
        })
    return cards


def convert_inside_summary_for_dashboard(items):
    cards = []
    for x in items:
        cards.append({
            "symbol": short_name(x.get("symbol", "")),
            "strategy": x.get("strategy", ""),
            "range_pct": x.get("range_pct", ""),
            "buy": {},
            "sell": {}
        })
    return cards


def convert_pivot_summary_for_dashboard(items):
    cards = []
    for x in items:
        level_text = f"{x.get('pivot_name', '')}={x.get('pivot_value', '')}"
        cards.append({
            "symbol": f"{short_name(x.get('symbol', ''))} [{level_text}]",
            "range_pct": "",
            "buy": {},
            "sell": {}
        })
    return cards
# ================= OI =================
def get_oi_snapshot(symbol, ltp):
    resp = fetch_option_chain(symbol, strikecount=10, timestamp="")
    option_rows = extract_options_chain_list(resp)
    parsed = normalize_chain_fast(option_rows)

    if not parsed:
        return [], "NEUTRAL"

    strikes = [r["strike"] for r in parsed]
    atm = min(strikes, key=lambda x: abs(x - ltp))
    atm_idx = strikes.index(atm)

    start = max(0, atm_idx - 1)
    end = min(len(parsed), start + 4)
    selected = parsed[start:end]

    ce_total = sum(r["call_oich"] for r in selected)
    pe_total = sum(r["put_oich"] for r in selected)

    bias = "NEUTRAL"
    if pe_total > ce_total:
        bias = "BULLISH"
    elif ce_total > pe_total:
        bias = "BEARISH"

    return selected, bias

def format_oi_snapshot(rows):
    if not rows:
        return "Strike    PE       | CE"
    out = ["Strike    PE       | CE"]
    for r in rows:
        out.append(
            f"{r['strike']:<8} {human_format(r['put_oich'])}{arrow(r['put_oich'])} | "
            f"{human_format(r['call_oich'])}{arrow(r['call_oich'])}"
        )
    return "\n".join(out)

def hold_status(side, bias):
    if side == "BUY":
        return "Buy Hold 🟢" if bias == "BULLISH" else "Exit ⚪"
    if side == "SELL":
        return "Sell Hold 🔴" if bias == "BEARISH" else "Exit ⚪"
    return "Exit ⚪"

# ================= POSITION SIZE =================
def calc_position(entry, stoploss):
    risk_per_share = abs(entry - stoploss)
    if risk_per_share <= 0:
        return 0, 0.0, 0.0, 0.0

    qty = max(1, int(round(RISK_AMOUNT / risk_per_share)))
    qty = min(qty, MAX_QTY)

    exposure = entry * qty
    margin = exposure / LEVERAGE if LEVERAGE > 0 else exposure

    return qty, exposure, margin, risk_per_share

# ================= RESULT ENGINE =================
def evaluate_sell_result(candles_after_entry, entry, target, stoploss):
    for c in candles_after_entry:
        high = float(c[2]); low = float(c[3])

        if high >= stoploss and low <= target:
            return "Stoploss 🛑", stoploss
        if high >= stoploss:
            return "Stoploss 🛑", stoploss
        if low <= target:
            return "Target 🎯", target

    if candles_after_entry:
        return "Day End ⚪", float(candles_after_entry[-1][4])

    return "No Data", entry

def evaluate_buy_result(candles_after_entry, entry, target, stoploss):
    for c in candles_after_entry:
        high = float(c[2]); low = float(c[3])

        if low <= stoploss and high >= target:
            return "Stoploss 🛑", stoploss
        if low <= stoploss:
            return "Stoploss 🛑", stoploss
        if high >= target:
            return "Target 🎯", target

    if candles_after_entry:
        return "Day End ⚪", float(candles_after_entry[-1][4])

    return "No Data", entry

# ================= PATTERN SCANNERS =================
def scan_gapup_pattern(symbol):
    if symbol in closed_for_day:
        return None

    prev_day = get_previous_daily(symbol)
    day_5m = get_analysis_day_candles(symbol, 5, 7)
    if prev_day is None or len(day_5m) < 1:
        return None

    first = day_5m[0]
    prev_high = float(prev_day[2])
    o = float(first[1]); h = float(first[2]); l = float(first[3]); c = float(first[4])

    gap_pct = ((o - prev_high) / prev_high) * 100 if prev_high else 0.0
    candle_pct = pct_range(h, l, c)

    if not (o > prev_high and gap_pct >= GAPUP_MIN_PCT and candle_pct <= GAPUP_CANDLE_MAX_PCT):
        return None

    entry = round(l, 2)
    sl = round(h * (1 + SL_BUFFER_PCT), 2)
    target = round(entry - (sl - entry) * TARGET_RR, 2)

    return {
        "symbol": symbol,
        "strategy": "GAPUP_PLUS",
        "side": "SELL",
        "gap_pct": round(gap_pct, 2),
        "entry": entry,
        "stoploss": sl,
        "target": target,
        "pattern_time": candle_dt(first[0]).strftime("%H:%M")
    }

def scan_15m_inside_pattern(symbol):
    if symbol in closed_for_day:
        return None

    day_15m = get_analysis_day_candles(symbol, 15, 7)
    if len(day_15m) < 2:
        return None

    c1 = day_15m[0]
    c2 = day_15m[1]

    h1 = float(c1[2]); l1 = float(c1[3]); c1_close = float(c1[4])
    h2 = float(c2[2]); l2 = float(c2[3])

    if c1_close <= 0:
        return None

    range_pct = pct_range(h1, l1, c1_close)
    inside = h2 <= h1 and l2 >= l1

    if not (INSIDE15_FIRST_CANDLE_MIN_PCT <= range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    return {
        "symbol": symbol,
        "strategy": "INSIDE_15M",
        "range_pct": round(range_pct, 2),
        "buy_entry": round(h1, 2),
        "buy_stoploss": round(l1 * (1 - SL_BUFFER_PCT), 2),
        "buy_target": round(h1 + (h1 - (l1 * (1 - SL_BUFFER_PCT))) * TARGET_RR, 2),
        "sell_entry": round(l1, 2),
        "sell_stoploss": round(h1 * (1 + SL_BUFFER_PCT), 2),
        "sell_target": round(l1 - ((h1 * (1 + SL_BUFFER_PCT)) - l1) * TARGET_RR, 2),
        "pattern_time": candle_dt(c2[0]).strftime("%H:%M")
    }

def compute_weekly_r_levels(prev_week):
    h = float(prev_week[2]); l = float(prev_week[3]); c = float(prev_week[4])

    p = (h + l + c) / 3.0
    r1 = 2 * p - l
    r2 = p + (h - l)
    r3 = h + 2 * (p - l)
    step = r2 - r1
    r4 = r3 + step
    r5 = r4 + step

    return {
        "R1": round(r1, 2),
        "R2": round(r2, 2),
        "R3": round(r3, 2),
        "R4": round(r4, 2),
        "R5": round(r5, 2),
    }

def candle_touches_level(candle, level):
    high = float(candle[2]); low = float(candle[3])
    return low <= level <= high

def eligible_for_pivot_scan(symbol):
    prev_day = get_previous_daily(symbol)
    q = fetch_quotes(symbol)
    if prev_day is None or not q:
        return False

    yesterday_close = float(prev_day[4])
    today_ltp = q.get("ltp", 0.0)
    prev_turnover = safe_float(prev_day[5], 0.0) * yesterday_close if len(prev_day) > 5 else 0.0

    if PIVOT_MIN_YDAY_TURNOVER > 0 and prev_turnover < PIVOT_MIN_YDAY_TURNOVER:
        return False

    return today_ltp >= yesterday_close * (1 + PIVOT_LTP_FILTER_PCT)

def scan_30m_pivot_sell(symbol):
    if symbol in closed_for_day:
        return None

    if not eligible_for_pivot_scan(symbol):
        return None

    prev_week = get_previous_weekly(symbol)
    day_30m = get_analysis_day_candles(symbol, 30, 21)

    if prev_week is None or len(day_30m) < 3:
        return None

    c1 = day_30m[0]; c2 = day_30m[1]
    c1_open = float(c1[1]); c1_close = float(c1[4])
    c2_open = float(c2[1]); c2_close = float(c2[4])
    c2_high = float(c2[2]); c2_low = float(c2[3])

    if not (c1_close > c1_open and c2_close < c2_open):
        return None

    r_levels = compute_weekly_r_levels(prev_week)
    touched_levels = []
    for name, value in r_levels.items():
        if candle_touches_level(c1, value) and candle_touches_level(c2, value):
            touched_levels.append((name, value))

    if not touched_levels:
        return None

    pivot_name, pivot_value = touched_levels[-1]

    entry = round(c2_low, 2)
    stoploss = round(c2_high, 2)
    if stoploss <= entry:
        return None
    target = round(entry - (stoploss - entry) * TARGET_RR, 2)

    return {
        "symbol": symbol,
        "strategy": "PIVOT_30M_WEEKLY_SELL",
        "side": "SELL",
        "pivot_name": pivot_name,
        "pivot_value": pivot_value,
        "entry": entry,
        "stoploss": stoploss,
        "target": target,
        "pattern_time": candle_dt(c2[0]).strftime("%H:%M")
    }

# ================= SUMMARY FORMATTERS =================
def format_gapup_summary(items):
    if not items:
        return "⚡ GAP UP PLUS STOCKS (%) ⚡\n\nNone"
    lines = ["⚡ GAP UP PLUS STOCKS (%) ⚡", ""]
    for i, x in enumerate(sorted(items, key=lambda z: z["gap_pct"], reverse=True), 1):
        lines.append(f"{i}. {short_name(x['symbol'])} ({x['gap_pct']}%)")
    return "\n".join(lines)

def format_inside_summary(items):
    if not items:
        return "🕯️ 15 MIN INSIDE CANDLE STOCKS (%) 🕯️\n\nNone"
    lines = ["🕯️ 15 MIN INSIDE CANDLE STOCKS (%) 🕯️", ""]
    for i, x in enumerate(sorted(items, key=lambda z: z["range_pct"]), 1):
        lines.append(f"{i}. {short_name(x['symbol'])} ({x['range_pct']}%)")
    return "\n".join(lines)

def format_pivot_summary(items):
    if not items:
        return "📍 30 MIN WEEKLY PIVOT SELL STOCKS\n\nNone"
    lines = ["📍 30 MIN WEEKLY PIVOT SELL STOCKS", ""]
    for i, x in enumerate(items, 1):
        lines.append(f"{i}. {short_name(x['symbol'])} ({x['pivot_name']}={x['pivot_value']})")
    return "\n".join(lines)

# ================= WATCH / ENTRY =================
def add_watch_candidate(symbol, payload):
    if symbol in closed_for_day:
        return
    if symbol in active_trades:
        return

    prev = watch_candidates.get(symbol)
    if prev and prev.get("strategy") == payload.get("strategy"):
        return

    watch_candidates[symbol] = payload

def block_trade(symbol, strategy, side, reason):
    blocked_entries.append({
        "symbol": short_name(symbol),
        "strategy": strategy,
        "side": side,
        "reason": reason,
        "time": now_ist().strftime("%H:%M:%S")
    })
    eod_stats["blocked"].append({
        "symbol": short_name(symbol),
        "strategy": strategy,
        "side": side,
        "reason": reason
    })

def send_entry_alert(symbol, trade, oi_rows, oi_bias):
    qty, exposure, margin, risk_per_share = calc_position(trade["entry"], trade["stoploss"])
    trade["qty"] = qty
    trade["exposure"] = round(exposure, 2)
    trade["margin"] = round(margin, 2)
    trade["risk_per_share"] = round(risk_per_share, 2)

    reason = f"Risk ₹{int(RISK_AMOUNT)} | Qty {qty} | Margin ~{round(margin)} | OI {oi_bias}"
    send_live_trade_image(
        trade,
        ltp=trade["entry"],
        status="ENTRY CONFIRMED",
        oi_rows=oi_rows,
        header_title="LIVE + OI + RISK + ENTRY",
        reason_text=reason,
        caption=f"{short_name(symbol)} Entry Confirmed"
    )

def try_entry_for_candidate(symbol):
    if symbol in closed_for_day:
        return
    if symbol in active_trades:
        return
    if symbol not in watch_candidates:
        return

    q = fetch_quotes(symbol)
    ltp = q.get("ltp", 0.0)
    if ltp <= 0:
        return

    c = watch_candidates[symbol]
    strategy = c["strategy"]

    if strategy == "GAPUP_PLUS":
        if ltp <= c["entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BEARISH":
                if throttle_ok(f"{symbol}|blocked|SELL"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nSELL trigger hit, but OI is against SELL")
                block_trade(symbol, strategy, "SELL", "OI against SELL")
                del watch_candidates[symbol]
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "SELL",
                "entry": c["entry"],
                "target": c["target"],
                "stoploss": c["stoploss"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "SELL"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]
        return

    if strategy == "INSIDE_15M":
        if ltp >= c["buy_entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BULLISH":
                if throttle_ok(f"{symbol}|blocked|BUY"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nBUY trigger hit, but OI is against BUY")
                block_trade(symbol, strategy, "BUY", "OI against BUY")
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "BUY",
                "entry": c["buy_entry"],
                "target": c["buy_target"],
                "stoploss": c["buy_stoploss"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "BUY"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]
            return

        if ltp <= c["sell_entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BEARISH":
                if throttle_ok(f"{symbol}|blocked|SELL"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nSELL trigger hit, but OI is against SELL")
                block_trade(symbol, strategy, "SELL", "OI against SELL")
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "SELL",
                "entry": c["sell_entry"],
                "target": c["sell_target"],
                "stoploss": c["sell_stoploss"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "SELL"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]
            return

    if strategy == "PIVOT_30M_WEEKLY_SELL":
        if ltp <= c["entry"]:
            oi_rows, bias = get_oi_snapshot(symbol, ltp)
            if bias != "BEARISH":
                if throttle_ok(f"{symbol}|blocked|PIVOTSELL"):
                    send(f"⚠️ ENTRY BLOCKED\n{short_name(symbol)}\nStrategy: {strategy}\nSELL trigger hit, but OI is against SELL")
                block_trade(symbol, strategy, "SELL", "OI against SELL")
                del watch_candidates[symbol]
                return

            trade = {
                "symbol": symbol,
                "strategy": strategy,
                "side": "SELL",
                "entry": c["entry"],
                "target": c["target"],
                "stoploss": c["stoploss"],
                "pivot_name": c["pivot_name"],
                "pivot_value": c["pivot_value"],
                "entry_time": now_ist().strftime("%H:%M:%S"),
                "last_oi_check": 0,
                "last_oi_alert": 0
            }
            active_trades[symbol] = trade
            eod_stats["entries"].append({"symbol": short_name(symbol), "strategy": strategy, "side": "SELL"})
            send_entry_alert(symbol, trade, oi_rows, bias)
            del watch_candidates[symbol]

# ================= LIVE TRADE TRACKING =================
def close_trade(symbol, reason, exit_price):
    if symbol not in active_trades:
        return
    trade = active_trades.pop(symbol)
    trade["close_reason"] = reason
    trade["exit_price"] = round(exit_price, 2)
    trade["close_time"] = now_ist().strftime("%H:%M:%S")
    closed_for_day.add(symbol)

    qty = trade.get("qty", 0)
    if trade["side"] == "BUY":
        pnl = round((exit_price - trade["entry"]) * qty, 2)
    else:
        pnl = round((trade["entry"] - exit_price) * qty, 2)
    trade["pnl"] = pnl

    if reason.startswith("Target"):
        eod_stats["targets"].append({"symbol": short_name(symbol), "strategy": trade["strategy"], "pnl": pnl})
    elif reason.startswith("Stoploss"):
        eod_stats["stoplosses"].append({"symbol": short_name(symbol), "strategy": trade["strategy"], "pnl": pnl})
    else:
        eod_stats["dayend"].append({"symbol": short_name(symbol), "strategy": trade["strategy"], "pnl": pnl})

    eod_stats["closed"].append({
        "symbol": short_name(symbol),
        "strategy": trade["strategy"],
        "side": trade["side"],
        "entry": trade["entry"],
        "exit": trade["exit_price"],
        "pnl": pnl,
        "reason": reason
    })

    send_live_trade_image(
        trade,
        ltp=trade["exit_price"],
        status=reason,
        oi_rows=[],
        header_title="TRADE CLOSED",
        reason_text=f"Exit {trade['exit_price']} | Qty {qty} | P/L {pnl}",
        caption=f"{short_name(symbol)} {reason}"
    )

def track_active_trade(symbol):
    if symbol not in active_trades:
        return

    trade = active_trades[symbol]
    q = fetch_quotes(symbol)
    ltp = q.get("ltp", 0.0)
    if ltp <= 0:
        return

    if trade["side"] == "BUY":
        if ltp <= trade["stoploss"]:
            close_trade(symbol, "Stoploss 🛑", trade["stoploss"])
            return
        if ltp >= trade["target"]:
            close_trade(symbol, "Target 🎯", trade["target"])
            return
    else:
        if ltp >= trade["stoploss"]:
            close_trade(symbol, "Stoploss 🛑", trade["stoploss"])
            return
        if ltp <= trade["target"]:
            close_trade(symbol, "Target 🎯", trade["target"])
            return

    if now_epoch() - trade.get("last_oi_check", 0) >= OI_INTERVAL_SECONDS:
        oi_rows, bias = get_oi_snapshot(symbol, ltp)
        status = hold_status(trade["side"], bias)
        trade["last_oi_check"] = now_epoch()

        if throttle_ok(f"{symbol}|live_oi"):
            send_live_trade_image(
                trade,
                ltp=ltp,
                status=status,
                oi_rows=oi_rows,
                header_title="LIVE + OI + RISK + RANKING",
                reason_text=f"Strategy {trade['strategy']} | OI bias {bias}",
                caption=f"{short_name(symbol)} {status}"
            )

        if status == "Exit ⚪" and throttle_ok(f"{symbol}|oi_exit"):
            send_live_trade_image(
                trade,
                ltp=ltp,
                status=status,
                oi_rows=oi_rows,
                header_title="OI EXIT SIGNAL",
                reason_text="OI turned against trade",
                caption=f"{short_name(symbol)} OI Exit"
            )


# ================= SCAN SCHEDULERS =================
def scan_gapup_once():
    items = []
    for sym in SYMBOLS:
        try:
            r = scan_gapup_pattern(sym)
            if r:
                items.append(r)
                add_watch_candidate(sym, r)
        except Exception as e:
            log(f"GAP SCAN ERROR {sym}: {e}")
    pattern_summary["gapup"] = items
    send_rich_summary_image(convert_gapup_summary_for_dashboard(items), title="STOCKS TO WATCH", subtitle="GAP UP PLUS", caption="Gap Up Plus")

def scan_inside15_once():
    items = []
    for sym in SYMBOLS:
        try:
            r = scan_15m_inside_pattern(sym)
            if r:
                items.append(r)
                add_watch_candidate(sym, r)
        except Exception as e:
            log(f"15M SCAN ERROR {sym}: {e}")
    pattern_summary["inside15"] = items
    send_rich_summary_image(convert_inside_summary_for_dashboard(items), title="STOCKS TO WATCH", subtitle="15 MIN INSIDE CANDLE", caption="15 Min Inside")

def pivot_scan_key():
    now = now_ist()
    return f"{now.strftime('%Y-%m-%d')}-{now.hour:02d}-{now.minute:02d}"

def should_run_pivot_scan():
    now = now_ist().time()
    valid_times = {
        dtime(9,45), dtime(10,15), dtime(10,45), dtime(11,15), dtime(11,45),
        dtime(12,15), dtime(12,45), dtime(13,15), dtime(13,45), dtime(14,15),
        dtime(14,45), dtime(15,15)
    }
    current = dtime(now.hour, now.minute)
    return current in valid_times

def scan_pivot_30m_once():
    key = pivot_scan_key()
    if key in pivot_scan_done_keys:
        return
    pivot_scan_done_keys.add(key)

    items = []
    for sym in SYMBOLS:
        try:
            r = scan_30m_pivot_sell(sym)
            if r:
                items.append(r)
                add_watch_candidate(sym, r)
        except Exception as e:
            log(f"PIVOT SCAN ERROR {sym}: {e}")
    pattern_summary["pivot30"] = items
    send_rich_summary_image(convert_pivot_summary_for_dashboard(items), title="STOCKS TO WATCH", subtitle="30 MIN WEEKLY PIVOT SELL", caption="30 Min Weekly Pivot Sell")

# ================= LIVE LOOP =================
def run_live_day():
    gap_summary_sent = False
    inside_summary_sent = False
    eod_sent = False

    while True:
        if not is_market_open():
            return

        nowt = now_ist().time()

        if not gap_summary_sent and nowt >= dtime(9, 20):
            scan_gapup_once()
            gap_summary_sent = True

        if not inside_summary_sent and nowt >= dtime(9, 45):
            scan_inside15_once()
            inside_summary_sent = True

        if should_run_pivot_scan():
            scan_pivot_30m_once()

        for sym in list(watch_candidates.keys()):
            if sym in closed_for_day or sym in active_trades:
                continue
            try:
                try_entry_for_candidate(sym)
            except Exception as e:
                log(f"ENTRY ERROR {sym}: {e}")
            time.sleep(1)

        for sym in list(active_trades.keys()):
            try:
                track_active_trade(sym)
            except Exception as e:
                log(f"TRACK ERROR {sym}: {e}")
            time.sleep(LTP_INTERVAL_PER_STOCK)

        if not eod_sent and nowt >= dtime(15, 28):
            send_after_market_summary_image(caption="End of Day Report")
            nxt = next_market_open_datetime()
            send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
            eod_sent = True

        time.sleep(POLL_SECONDS)

# ================= AFTER MARKET SUMMARY =================
def evaluate_gapup_after_market(symbol):
    prev_day = get_previous_daily(symbol)
    day_5m = get_analysis_day_candles(symbol, 5, 7)
    if prev_day is None or len(day_5m) < 1:
        return None

    first = day_5m[0]
    prev_high = float(prev_day[2])

    o = float(first[1]); h = float(first[2]); l = float(first[3]); c = float(first[4])
    gap_pct = ((o - prev_high) / prev_high) * 100 if prev_high else 0.0
    candle_pct = pct_range(h, l, c)

    if not (o > prev_high and gap_pct >= GAPUP_MIN_PCT and candle_pct <= GAPUP_CANDLE_MAX_PCT):
        return None

    entry = round(l, 2)
    stoploss = round(h * (1 + SL_BUFFER_PCT), 2)
    target = round(entry - (stoploss - entry) * TARGET_RR, 2)
    later = day_5m[1:]
    result, exit_price = evaluate_sell_result(later, entry, target, stoploss)
    pl = round(entry - exit_price, 2)

    return {
        "symbol": short_name(symbol),
        "gap_pct": round(gap_pct, 2),
        "entry": entry,
        "target": target,
        "stoploss": stoploss,
        "result": result,
        "exit_price": round(exit_price, 2),
        "pl": pl
    }

def evaluate_inside_after_market(symbol):
    day_15m = get_analysis_day_candles(symbol, 15, 7)
    if len(day_15m) < 2:
        return None

    c1 = day_15m[0]; c2 = day_15m[1]
    h1 = float(c1[2]); l1 = float(c1[3]); c1_close = float(c1[4])
    h2 = float(c2[2]); l2 = float(c2[3])

    if c1_close <= 0:
        return None

    range_pct = pct_range(h1, l1, c1_close)
    inside = h2 <= h1 and l2 >= l1
    if not (INSIDE15_FIRST_CANDLE_MIN_PCT <= range_pct <= INSIDE15_FIRST_CANDLE_MAX_PCT and inside):
        return None

    later = day_15m[2:]

    buy_entry = round(h1, 2)
    buy_sl = round(l1 * (1 - SL_BUFFER_PCT), 2)
    buy_target = round(buy_entry + (buy_entry - buy_sl) * TARGET_RR, 2)
    buy_result, buy_exit = evaluate_buy_result(later, buy_entry, buy_target, buy_sl)
    buy_pl = round(buy_exit - buy_entry, 2)

    sell_entry = round(l1, 2)
    sell_sl = round(h1 * (1 + SL_BUFFER_PCT), 2)
    sell_target = round(sell_entry - (sell_sl - sell_entry) * TARGET_RR, 2)
    sell_result, sell_exit = evaluate_sell_result(later, sell_entry, sell_target, sell_sl)
    sell_pl = round(sell_entry - sell_exit, 2)

    return {
        "symbol": short_name(symbol),
        "range_pct": round(range_pct, 2),
        "buy": {"entry": buy_entry, "target": buy_target, "stoploss": buy_sl, "result": buy_result, "exit_price": round(buy_exit, 2), "pl": buy_pl},
        "sell": {"entry": sell_entry, "target": sell_target, "stoploss": sell_sl, "result": sell_result, "exit_price": round(sell_exit, 2), "pl": sell_pl},
    }

def evaluate_pivot_after_market(symbol):
    prev_week = get_previous_weekly(symbol)
    day_30m = get_analysis_day_candles(symbol, 30, 21)
    if prev_week is None or len(day_30m) < 3:
        return None

    c1 = day_30m[0]; c2 = day_30m[1]; c3 = day_30m[2]
    c1_open = float(c1[1]); c1_close = float(c1[4])
    c2_open = float(c2[1]); c2_close = float(c2[4])
    c2_high = float(c2[2]); c2_low = float(c2[3])
    c3_low = float(c3[3]); c3_high = float(c3[2])

    if not (c1_close > c1_open and c2_close < c2_open):
        return None

    r_levels = compute_weekly_r_levels(prev_week)
    touched = []
    for name, value in r_levels.items():
        if candle_touches_level(c1, value) and candle_touches_level(c2, value):
            touched.append((name, value))
    if not touched:
        return None

    pivot_name, pivot_value = touched[-1]
    entry = round(c2_low, 2)
    stoploss = round(c2_high, 2)
    if stoploss <= entry:
        return None
    target = round(entry - (stoploss - entry) * TARGET_RR, 2)

    if c3_low > entry:
        result = "No Entry"
        exit_price = entry
        pl = 0.0
    elif c3_high >= stoploss and c3_low <= target:
        result = "Stoploss 🛑"
        exit_price = stoploss
        pl = round(entry - exit_price, 2)
    elif c3_high >= stoploss:
        result = "Stoploss 🛑"
        exit_price = stoploss
        pl = round(entry - exit_price, 2)
    elif c3_low <= target:
        result = "Target 🎯"
        exit_price = target
        pl = round(entry - exit_price, 2)
    else:
        result = "Day End ⚪"
        exit_price = float(c3[4])
        pl = round(entry - exit_price, 2)

    return {
        "symbol": short_name(symbol),
        "pivot_name": pivot_name,
        "pivot_value": pivot_value,
        "entry": entry,
        "target": target,
        "stoploss": stoploss,
        "result": result,
        "exit_price": round(exit_price, 2),
        "pl": pl
    }

def format_gapup_results(items):
    if not items:
        return "📘 GAP UP PLUS - IF ENTRY TAKEN\n\nNone"
    lines = ["📘 GAP UP PLUS - IF ENTRY TAKEN", ""]
    for x in items:
        sign = "+" if x["pl"] > 0 else ""
        lines += [
            x["symbol"],
            f"SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
            f"Result:{x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

def format_inside_results(items):
    if not items:
        return "📘 15 MIN INSIDE CANDLE - IF ENTRY TAKEN\n\nNone"
    lines = ["📘 15 MIN INSIDE CANDLE - IF ENTRY TAKEN", ""]
    for x in items:
        b = x["buy"]; s = x["sell"]
        bsign = "+" if b["pl"] > 0 else ""
        ssign = "+" if s["pl"] > 0 else ""
        lines += [
            f"{x['symbol']} ({x['range_pct']}%)",
            f"🟢 BUY  Entry:{b['entry']} Target:{b['target']} SL:{b['stoploss']}",
            f"      {b['result']} Exit:{b['exit_price']} P/L:{bsign}{b['pl']}",
            f"🔴 SELL Entry:{s['entry']} Target:{s['target']} SL:{s['stoploss']}",
            f"      {s['result']} Exit:{s['exit_price']} P/L:{ssign}{s['pl']}",
            ""
        ]
    return "\n".join(lines).strip()

def format_pivot_results(items):
    if not items:
        return "📘 30 MIN WEEKLY PIVOT SELL - IF ENTRY TAKEN\n\nNone"
    lines = ["📘 30 MIN WEEKLY PIVOT SELL - IF ENTRY TAKEN", ""]
    for x in items:
        sign = "+" if x["pl"] > 0 else ""
        lines += [
            x["symbol"],
            f"Level:{x['pivot_name']} ({x['pivot_value']})",
            f"🔴 SELL Entry:{x['entry']} Target:{x['target']} SL:{x['stoploss']}",
            f"      {x['result']} Exit:{x['exit_price']} P/L:{sign}{x['pl']}",
            ""
        ]
    return "\n".join(lines).strip()


def chunk_list(items, size):
    items = list(items or [])
    for i in range(0, len(items), size):
        yield items[i:i + size]


def build_after_market_cards_for_category(items, category_name):
    cards = []

    if category_name == "GAPUP PLUS":
        for x in items:
            result = str(x.get("result", ""))
            score = 94 if "Target" in result else 84 if "Stoploss" in result else 88
            cards.append({
                "symbol": x.get("symbol", ""),
                "ltp": x.get("exit_price", ""),
                "score": score,
                "strategy": "GAP UP",
                "side": "SELL",
                "result": result,
                "entry": x.get("entry", ""),
                "stoploss": x.get("stoploss", ""),
                "target": x.get("target", ""),
                "qty": "-",
                "pl": f"{safe_float(x.get('pl', 0.0), 0.0):+}",
                "pnl_value": safe_float(x.get("pl", 0.0), 0.0),
                "oi_rows": []
            })

    elif category_name == "15 MIN INSIDE":
        for x in items:
            for side_key, side_name in [("buy", "BUY"), ("sell", "SELL")]:
                side = x.get(side_key, {}) or {}
                result = str(side.get("result", ""))
                score = 96 if "Target" in result else 84 if "Stoploss" in result else 89
                cards.append({
                    "symbol": x.get("symbol", ""),
                    "ltp": side.get("exit_price", ""),
                    "score": score,
                    "strategy": "15M INSIDE",
                    "side": side_name,
                    "result": result,
                    "entry": side.get("entry", ""),
                    "stoploss": side.get("stoploss", ""),
                    "target": side.get("target", ""),
                    "qty": "-",
                    "pl": f"{safe_float(side.get('pl', 0.0), 0.0):+}",
                    "pnl_value": safe_float(side.get("pl", 0.0), 0.0),
                    "oi_rows": []
                })

    elif category_name == "PIVOT":
        for x in items:
            result = str(x.get("result", ""))
            score = 92 if "Target" in result else 83 if "Stoploss" in result else 87
            cards.append({
                "symbol": x.get("symbol", ""),
                "ltp": x.get("exit_price", ""),
                "score": score,
                "strategy": f"PIVOT {x.get('pivot_name', '')}",
                "side": "SELL",
                "result": result,
                "entry": x.get("entry", ""),
                "stoploss": x.get("stoploss", ""),
                "target": x.get("target", ""),
                "qty": "-",
                "pl": f"{safe_float(x.get('pl', 0.0), 0.0):+}",
                "pnl_value": safe_float(x.get("pl", 0.0), 0.0),
                "oi_rows": []
            })

    cards.sort(key=lambda z: safe_float(z.get("score", 0), 0), reverse=True)
    return cards


def send_after_market_category_images(items, category_name, per_image=8):
    if not items:
        log(f"No after-market items for {category_name}")
        return

    cards = build_after_market_cards_for_category(items, category_name)
    pages = list(chunk_list(cards, per_image))

    for idx, page_cards in enumerate(pages, 1):
        caption = f"After Market Summary • {category_name}"
        if len(pages) > 1:
            caption += f" ({idx}/{len(pages)})"

        try:
            img_bytes = _load_font(
                page_cards,
                title="STOCKS TO WATCH",
                subtitle=f"AFTER MARKET SUMMARY • {category_name}",
                analysis_dt=analysis_date_str().upper()
            )
            files = {"photo": (f"after_market_{category_name}_{idx}.png", img_bytes, "image/png")}
            data = {"chat_id": CHAT_ID, "caption": caption[:1024]}
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                data=data,
                files=files,
                timeout=60
            )
            log(f"Sent after-market {category_name} image {idx}/{len(pages)}")
        except Exception as e:
            log(f"After-market {category_name} image send error: {e}")


def run_after_market_once():
    send("📡 Running after-market scan...")

    gap_items = []
    inside_items = []
    pivot_items = []

    for sym in SYMBOLS:
        try:
            g = evaluate_gapup_after_market(sym)
            if g:
                gap_items.append(g)
        except Exception as e:
            log(f"GAP AFTER ERROR {sym}: {e}")

        try:
            i = evaluate_inside_after_market(sym)
            if i:
                inside_items.append(i)
        except Exception as e:
            log(f"15M AFTER ERROR {sym}: {e}")

        try:
            p = evaluate_pivot_after_market(sym)
            if p:
                pivot_items.append(p)
        except Exception as e:
            log(f"PIVOT AFTER ERROR {sym}: {e}")

    send_after_market_category_images(gap_items, "GAPUP PLUS")
    send_after_market_category_images(inside_items, "15 MIN INSIDE")
    send_after_market_category_images(pivot_items, "PIVOT")

    nxt = next_market_open_datetime()
    send(f"🌙 Market Closed\nNext open {nxt.strftime('%Y-%m-%d %H:%M:%S IST')}")
# ================= EOD REPORT =================
def build_eod_report():
    total_pnl = round(sum(x.get("pnl", 0.0) for x in eod_stats["closed"]), 2)

    lines = ["📊 END OF DAY REPORT", ""]

    lines += [
        f"Patterns Found:",
        f"Gap Up Plus: {len(pattern_summary['gapup'])}",
        f"15 Min Inside: {len(pattern_summary['inside15'])}",
        f"30 Min Weekly Pivot Sell: {len(pattern_summary['pivot30'])}",
        "",
        f"Entries Triggered: {len(eod_stats['entries'])}",
        f"Blocked by OI: {len(eod_stats['blocked'])}",
        f"Target Hit: {len(eod_stats['targets'])}",
        f"Stoploss Hit: {len(eod_stats['stoplosses'])}",
        f"Day End / Others: {len(eod_stats['dayend'])}",
        "",
        f"Total Closed Trades: {len(eod_stats['closed'])}",
        f"Net P/L: {total_pnl}",
        ""
    ]

    if eod_stats["closed"]:
        lines.append("Closed Trades:")
        for x in eod_stats["closed"]:
            sign = "+" if x["pnl"] > 0 else ""
            lines.append(
                f"{x['symbol']} | {x['strategy']} | {x['side']} | "
                f"Entry:{x['entry']} Exit:{x['exit']} | {x['reason']} | P/L:{sign}{x['pnl']}"
            )
        lines.append("")

    if eod_stats["blocked"]:
        lines.append("Blocked Entries:")
        for x in eod_stats["blocked"]:
            lines.append(f"{x['symbol']} | {x['strategy']} | {x['side']} | {x['reason']}")

    return "\n".join(lines).strip()

# ================= MAIN =================
def main():
    profile = check_auth()
    log_analysis_date_debug()
    send(
        f"🚀 BOT STARTED\n"
        f"Profile status: {profile.get('s')}\n"
        f"AFTER_MARKET_RUN={AFTER_MARKET_RUN}\n"
        f"Analysis day={analysis_date_str()}\n"
        f"WATCHLIST={WATCHLIST_RAW}\n"
        f"Risk={RISK_AMOUNT} | Leverage={LEVERAGE}X"
    )

    while True:
        if is_market_open():
            run_live_day()
        else:
            if AFTER_MARKET_RUN:
                run_after_market_once()
            sleep_until_next_market_open()
            

if __name__ == "__main__":
    main()

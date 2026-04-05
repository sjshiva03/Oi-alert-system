from io import BytesIO
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
import os


# =========================================================
# FINAL DASHBOARD RENDERER
# LIVE DASHBOARD + AFTER MARKET DASHBOARD
# =========================================================

# -------------------------
# COLORS
# -------------------------
BG_COLOR = (242, 244, 247)
WHITE = (255, 255, 255)
BLACK = (15, 15, 15)
GRAY = (110, 110, 110)
LIGHT_GRAY = (245, 245, 245)
BORDER = (205, 205, 205)

RED_HEADER = (239, 58, 50)
GREEN_HEAD = (55, 184, 126)
RED_HEAD = (223, 72, 72)

SOFT_GREEN = (225, 245, 225)
SOFT_RED = (250, 230, 230)
SOFT_NEUTRAL = (240, 240, 240)

DARK_GREEN = (0, 120, 0)
DARK_RED = (180, 0, 0)

# -------------------------
# SIZE CONFIG
# -------------------------
DASH_W = 1080
LIVE_DASH_H = 2000
AFTER_DASH_H = 2000

PAGE_PAD = 20
CARD_GAP_X = 18
CARD_GAP_Y = 18

LIVE_TOP_Y = 160
AFTER_TOP_Y = 160

LIVE_COLS = 2
LIVE_ROWS = 4
LIVE_CARD_H = 400

AFTER_COLS = 2
AFTER_ROWS = 4
AFTER_CARD_H = 400


# -------------------------
# FONT LOADING
# -------------------------
def _load_single_font(size: int, bold: bool = False):
    candidates = []
    if bold:
        candidates += [
            "./fonts/DejaVuSans-Bold.ttf",
            "fonts/DejaVuSans-Bold.ttf",
            "/app/fonts/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        ]
    else:
        candidates += [
            "./fonts/DejaVuSans.ttf",
            "fonts/DejaVuSans.ttf",
            "/app/fonts/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        ]

    for path in candidates:
        try:
            if os.path.exists(path):
                return ImageFont.truetype(path, size)
        except Exception:
            pass

    return ImageFont.load_default()


def _get_fonts():
    return {
        "title": _load_single_font(34, True),
        "top_right": _load_single_font(18, True),
        "top_perf": _load_single_font(16, True),

        "card_title": _load_single_font(24, True),
        "pill": _load_single_font(18, True),

        "strategy": _load_single_font(20, True),
        "body": _load_single_font(18, True),
        "small": _load_single_font(16, True),
        "oi": _load_single_font(16, True),
    }


# -------------------------
# HELPERS
# -------------------------
def _rrect(draw, box, radius, fill, outline=None, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def _txt_w(draw, text, font):
    return draw.textbbox((0, 0), str(text), font=font)[2]


def _fit_text(draw, text, font, max_w):
    text = str(text or "")
    if _txt_w(draw, text, font) <= max_w:
        return text
    while len(text) > 1:
        text = text[:-1]
        cand = text.rstrip() + "..."
        if _txt_w(draw, cand, font) <= max_w:
            return cand
    return "..."


def _safe_float(v, default=0.0):
    try:
        if v in ("", None):
            return default
        return float(v)
    except Exception:
        return default


def _fmt_ltp(v):
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)


def _fmt_day_pct(v):
    try:
        return f"{float(v):.2f}%"
    except Exception:
        return "0.00%"


def _day_color(day_pct):
    return DARK_GREEN if _safe_float(day_pct, 0) >= 0 else DARK_RED


def _result_fill(result: str):
    result = str(result or "").upper()
    if "TARGET" in result:
        return DARK_GREEN
    if "STOPLOSS" in result:
        return DARK_RED
    return BLACK


def _side_head_fill(side: str):
    return GREEN_HEAD if str(side).upper() == "BUY" else RED_HEAD


def _soft_fill(side: str):
    return SOFT_GREEN if str(side).upper() == "BUY" else SOFT_RED


def _save_png_bytes(img, name="dashboard"):
    bio = BytesIO()
    bio.name = f"{name}.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def save_png_file(img_bytes: BytesIO, output_path: str):
    with open(output_path, "wb") as f:
        f.write(img_bytes.getvalue())


def _build_top_performers_text(cards, top_n=3):
    ranked = sorted(cards, key=lambda x: _safe_float(x.get("pnl_value", 0), 0), reverse=True)[:top_n]
    parts = []
    for c in ranked:
        sym = str(c.get("symbol", ""))
        pnl = _safe_float(c.get("pnl_value", 0), 0)
        parts.append(f"{sym} {pnl:+.0f}")
    return "   ".join(parts) if parts else "No top performers"


# -------------------------
# COMMON HEADER
# -------------------------
def _draw_dashboard_header(draw, fonts, title_text, dt_text, top_perf_text):
    _rrect(draw, (20, 20, DASH_W - 20, 90), 28, RED_HEADER)
    draw.text((38, 36), title_text, fill=WHITE, font=fonts["title"])

    dt_w = _txt_w(draw, dt_text, fonts["top_right"])
    draw.text((DASH_W - 30 - dt_w, 40), dt_text, fill=WHITE, font=fonts["top_right"])

    _rrect(draw, (20, 100, DASH_W - 20, 145), 18, WHITE, BORDER, 1)
    draw.text((30, 113), f"Top Performers: {top_perf_text}", fill=DARK_GREEN, font=fonts["top_perf"])


# -------------------------
# LIVE CARD
# -------------------------
def _draw_live_card(draw, fonts, x, y, card_w, card_h, item):
    side = str(item.get("side", "BUY")).upper()
    symbol = str(item.get("symbol", ""))
    ltp = _fmt_ltp(item.get("ltp", 0))
    day_pct = _safe_float(item.get("day_pct", 0), 0.0)
    strategy = str(item.get("strategy", "15M INSIDE"))
    status = str(item.get("status", "HOLD")).upper()
    confidence = str(item.get("confidence", "")).strip()

    entry = str(item.get("entry", ""))
    sl = str(item.get("stoploss", ""))
    qty = str(item.get("qty", ""))
    target = str(item.get("target", ""))
    pl_text = str(item.get("pl_text", item.get("pl", "")))
    pl_value = _safe_float(item.get("pnl_value", 0), 0)

    strikes = item.get("strikes", [])

    _rrect(draw, (x, y, x + card_w, y + card_h), 22, WHITE, BORDER, 2)

    # Header strip
    _rrect(draw, (x + 10, y + 10, x + card_w - 10, y + 55), 14, _side_head_fill(side))
    head_text = _fit_text(draw, f"{symbol}-{ltp}", fonts["card_title"], card_w - 180)
    draw.text((x + 16, y + 18), head_text, fill=WHITE, font=fonts["card_title"])

    # Day % pill
    pill_w = 92
    _rrect(draw, (x + card_w - pill_w - 18, y + 14, x + card_w - 18, y + 44), 15, WHITE)
    draw.text((x + card_w - pill_w - 8, y + 18), _fmt_day_pct(day_pct), fill=_day_color(day_pct), font=fonts["pill"])

    # Strategy block
    _rrect(draw, (x + 14, y + 68, x + card_w - 14, y + 106), 10, _soft_fill(side))
    conf_text = f" • {confidence}%" if confidence not in ("", None) else ""
    strat_text = _fit_text(draw, f"{strategy} • {side} • {status}{conf_text}", fonts["strategy"], card_w - 40)
    draw.text((x + 20, y + 76), strat_text, fill=DARK_GREEN if side == "BUY" else DARK_RED, font=fonts["strategy"])

    # Entry block
    _rrect(draw, (x + 14, y + 112, x + card_w - 14, y + 148), 10, SOFT_GRAY)
    draw.text((x + 20, y + 119), f"Entry: {entry}   SL: {sl}   Qty: {qty}", fill=BLACK, font=fonts["body"])

    # Target / P&L block
    _rrect(draw, (x + 14, y + 154, x + card_w - 14, y + 190), 10, _soft_fill(side))
    pl_fill = DARK_GREEN if pl_value >= 0 else DARK_RED
    draw.text((x + 20, y + 161), f"Target: {target}   P/L: {pl_text}", fill=pl_fill, font=fonts["body"])

    # Exit line
    exit_text = str(item.get("exit_type", "")).upper()
    if exit_text:
        exit_fill = _result_fill(exit_text)
        _rrect(draw, (x + 14, y + 196, x + card_w - 14, y + 232), 10, SOFT_GRAY)
        draw.text((x + 20, y + 203), f"Exit: {exit_text}", fill=exit_fill, font=fonts["body"])
        table_top = y + 248
    else:
        table_top = y + 208

    # OI table
    xs = [x + 20, x + 100, x + 180, x + 250, x + 320]
    headers = ["Strike", "PE", "Chg", "CE", "Chg"]
    for xp, h in zip(xs, headers):
        draw.text((xp, table_top), h, fill=GRAY, font=fonts["oi"])

    yy = table_top + 28
    for row in strikes[:5]:
        strike = str(row.get("strike", ""))
        pe_oi = str(row.get("pe_oi", ""))
        pe_chg = str(row.get("pe_chg", ""))
        ce_oi = str(row.get("ce_oi", ""))
        ce_chg = str(row.get("ce_chg", ""))

        vals = [strike, pe_oi, pe_chg, ce_oi, ce_chg]
        fills = [
            BLACK,
            BLACK,
            DARK_GREEN if "-" not in pe_chg else DARK_RED,
            BLACK,
            DARK_GREEN if "-" not in ce_chg else DARK_RED
        ]
        for xp, val, fc in zip(xs, vals, fills):
            draw.text((xp, yy), val, fill=fc, font=fonts["oi"])
        yy += 28


# -------------------------
# AFTER MARKET CARD
# -------------------------
def _draw_after_card(draw, fonts, x, y, card_w, card_h, item):
    side = str(item.get("side", "BUY")).upper()
    symbol = str(item.get("symbol", ""))
    ltp = _fmt_ltp(item.get("ltp", 0))
    day_pct = _safe_float(item.get("day_pct", 0), 0.0)
    strategy = str(item.get("strategy", "15M INSIDE"))
    status = str(item.get("status", "HOLD")).upper()
    confidence = str(item.get("confidence", "")).strip()
    exit_type = str(item.get("exit_type", "DAY END")).upper()

    entry = str(item.get("entry", ""))
    sl = str(item.get("stoploss", ""))
    qty = str(item.get("qty", ""))
    target = str(item.get("target", ""))
    pl_text = str(item.get("pl_text", item.get("pl", "")))
    pl_value = _safe_float(item.get("pnl_value", 0), 0)
    close_price = str(item.get("close_price", ""))

    _rrect(draw, (x, y, x + card_w, y + card_h), 22, WHITE, BORDER, 2)

    # Header strip
    _rrect(draw, (x + 10, y + 10, x + card_w - 10, y + 55), 14, _side_head_fill(side))
    head_text = _fit_text(draw, f"{symbol}-{ltp}", fonts["card_title"], card_w - 180)
    draw.text((x + 16, y + 18), head_text, fill=WHITE, font=fonts["card_title"])

    # Day % pill
    pill_w = 92
    _rrect(draw, (x + card_w - pill_w - 18, y + 14, x + card_w - 18, y + 44), 15, WHITE)
    draw.text((x + card_w - pill_w - 8, y + 18), _fmt_day_pct(day_pct), fill=_day_color(day_pct), font=fonts["pill"])

    # Strategy block
    _rrect(draw, (x + 14, y + 68, x + card_w - 14, y + 106), 10, _soft_fill(side))
    conf_text = f" • {confidence}%" if confidence not in ("", None) else ""
    strat_text = _fit_text(draw, f"{strategy} • {side} • {status}{conf_text}", fonts["strategy"], card_w - 40)
    draw.text((x + 20, y + 76), strat_text, fill=DARK_GREEN if side == "BUY" else DARK_RED, font=fonts["strategy"])

    # Entry block
    _rrect(draw, (x + 14, y + 112, x + card_w - 14, y + 148), 10, SOFT_GRAY)
    draw.text((x + 20, y + 119), f"Entry: {entry}   SL: {sl}   Qty: {qty}", fill=BLACK, font=fonts["body"])

    # Target / P&L block
    block_fill = SOFT_GREEN if exit_type == "TARGET" else SOFT_RED if exit_type == "STOPLOSS" else SOFT_NEUTRAL
    pl_fill = DARK_GREEN if pl_value >= 0 else DARK_RED if pl_value < 0 else BLACK
    _rrect(draw, (x + 14, y + 154, x + card_w - 14, y + 190), 10, block_fill)
    draw.text((x + 20, y + 161), f"Target: {target}   P/L: {pl_text}", fill=pl_fill, font=fonts["body"])

    # Exit block
    exit_fill = _result_fill(exit_type)
    _rrect(draw, (x + 14, y + 196, x + card_w - 14, y + 232), 10, SOFT_GRAY)
    draw.text((x + 20, y + 203), f"Exit: {exit_type}", fill=exit_fill, font=fonts["body"])
    draw.text((x + 220, y + 203), f"Close: {close_price}", fill=GRAY, font=fonts["body"])


# -------------------------
# PUBLIC BUILD FUNCTIONS
# -------------------------
def build_live_dashboard_image(cards, top_performers=None, dt_text=None):
    """
    LIVE cards required keys:
      symbol, ltp, day_pct, side, strategy, status, confidence,
      entry, stoploss, qty, target, pl_text, pnl_value,
      exit_type (optional),
      strikes=[{strike, pe_oi, pe_chg, ce_oi, ce_chg}]
    """
    fonts = _get_fonts()
    img = Image.new("RGB", (DASH_W, LIVE_DASH_H), BG_COLOR)
    draw = ImageDraw.Draw(img)

    dt_text = dt_text or datetime.now().strftime("%d-%b-%Y %I:%M %p").upper()
    top_text = top_performers or _build_top_performers_text(cards)
    _draw_dashboard_header(draw, fonts, "LIVE DASHBOARD", dt_text, top_text)

    card_w = (DASH_W - 60) // 2
    for i, item in enumerate((cards or [])[:8]):
        row = i // LIVE_COLS
        col = i % LIVE_COLS
        x = PAGE_PAD + col * (card_w + CARD_GAP_X)
        y = LIVE_TOP_Y + row * (LIVE_CARD_H + CARD_GAP_Y)
        _draw_live_card(draw, fonts, x, y, card_w, LIVE_CARD_H, item)

    return _save_png_bytes(img, "live_dashboard")


def build_after_market_dashboard_image(cards, top_performers=None, dt_text=None):
    """
    AFTER MARKET cards required keys:
      symbol, ltp, day_pct, side, strategy, status, confidence,
      entry, stoploss, qty, target, pl_text, pnl_value,
      exit_type, close_price
    """
    fonts = _get_fonts()
    img = Image.new("RGB", (DASH_W, AFTER_DASH_H), BG_COLOR)
    draw = ImageDraw.Draw(img)

    dt_text = dt_text or datetime.now().strftime("%d-%b-%Y %I:%M %p").upper()
    top_text = top_performers or _build_top_performers_text(cards)
    _draw_dashboard_header(draw, fonts, "AFTER MARKET SUMMARY", dt_text, top_text)

    card_w = (DASH_W - 60) // 2
    for i, item in enumerate((cards or [])[:8]):
        row = i // AFTER_COLS
        col = i % AFTER_COLS
        x = PAGE_PAD + col * (card_w + CARD_GAP_X)
        y = AFTER_TOP_Y + row * (AFTER_CARD_H + CARD_GAP_Y)
        _draw_after_card(draw, fonts, x, y, card_w, AFTER_CARD_H, item)

    return _save_png_bytes(img, "after_market_dashboard")


# -------------------------
# SAMPLE DATA
# -------------------------
def sample_live_cards():
    names = ["RELIANCE", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "TCS", "AXISBANK", "ITC"]
    cards = []
    for i, name in enumerate(names):
        side = "BUY" if i % 2 == 0 else "SELL"
        ltp = round(1210.94 + i * 148.37, 2)
        entry = int(ltp)
        stoploss = entry - 20 if side == "BUY" else entry + 20
        target = entry + 40 if side == "BUY" else entry - 40
        pnl_value = -363 if i == 0 else 730 if i == 1 else -397 if i == 2 else 1141 if i == 3 else 740 if i == 4 else 562 if i == 5 else 430 if i == 6 else -210
        strikes = []
        for j in range(5):
            strike = entry + (j - 2) * 20
            strikes.append({
                "strike": str(strike),
                "pe_oi": "1.2L",
                "pe_chg": "↑20K",
                "ce_oi": "90K",
                "ce_chg": "↓10K",
            })
        cards.append({
            "symbol": name,
            "ltp": ltp,
            "day_pct": round((-1.68 + i * 0.58), 2),
            "side": side,
            "strategy": "15M INSIDE",
            "status": "HOLD",
            "confidence": 84 + (i % 4) * 3,
            "entry": entry,
            "stoploss": stoploss,
            "qty": 123 + i * 71,
            "target": target,
            "pl_text": f"{pnl_value:+.0f}",
            "pnl_value": pnl_value,
            "exit_type": ["TARGET", "STOPLOSS", "DAY END", "TARGET", "STOPLOSS", "DAY END", "TARGET", "STOPLOSS"][i],
            "strikes": strikes,
        })
    return cards


def sample_after_market_cards():
    names = ["RELIANCE", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "TCS", "AXISBANK", "ITC"]
    exits = ["TARGET", "STOPLOSS", "DAY END", "TARGET", "STOPLOSS", "DAY END", "TARGET", "STOPLOSS"]
    cards = []
    for i, name in enumerate(names):
        side = "BUY" if i % 2 == 0 else "SELL"
        ltp = round(1320.41 + i * 132.25, 2)
        entry = int(ltp)
        stoploss = entry - 20 if side == "BUY" else entry + 20
        target = entry + 40 if side == "BUY" else entry - 40
        pnl_value = 640 if exits[i] == "TARGET" else -420 if exits[i] == "STOPLOSS" else 110
        cards.append({
            "symbol": name,
            "ltp": ltp,
            "day_pct": round((-1.25 + i * 0.44), 2),
            "side": side,
            "strategy": "15M INSIDE",
            "status": "HOLD",
            "confidence": 82 + (i % 5) * 3,
            "entry": entry,
            "stoploss": stoploss,
            "qty": 120 + i * 63,
            "target": target,
            "pl_text": f"{pnl_value:+.0f}",
            "pnl_value": pnl_value,
            "exit_type": exits[i],
            "close_price": round(ltp + (8 if exits[i] == "TARGET" else -7 if exits[i] == "STOPLOSS" else 2), 2),
        })
    return cards


# -------------------------
# SAVE SAMPLE FILES
# -------------------------
if __name__ == "__main__":
    live_png = build_live_dashboard_image(
        cards=sample_live_cards(),
        top_performers="RELIANCE +2.1%   INFY +1.8%   TCS +1.5%",
        dt_text="04-APR-2026 10:15 AM"
    )
    save_png_file(live_png, "live_dashboard_final.png")

    after_png = build_after_market_dashboard_image(
        cards=sample_after_market_cards(),
        top_performers="RELIANCE +2.1%   INFY +1.8%   TCS +1.5%",
        dt_text="04-APR-2026 03:35 PM"
    )
    save_png_file(after_png, "after_market_dashboard_final.png")

    print("Created: live_dashboard_final.png")
    print("Created: after_market_dashboard_final.png")

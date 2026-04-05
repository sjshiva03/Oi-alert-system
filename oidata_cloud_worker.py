from PIL import Image, ImageDraw, ImageFont
import requests
from datetime import datetime
import os
import PIL

def _load_fonts():
    import os
    from PIL import ImageFont

    bold_candidates = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    ]
    regular_candidates = [
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]

    def load_first(paths, size):
        for p in paths:
            try:
                if os.path.exists(p):
                    f = ImageFont.truetype(p, size)
                    print(f"[FONT OK] {p} size={size}", flush=True)
                    return f
            except Exception as e:
                print(f"[FONT FAIL] {p}: {e}", flush=True)
        print(f"[FONT FALLBACK] default size={size}", flush=True)
        return ImageFont.load_default()

    return {
        "title": load_first(bold_candidates, 56),
        "sub": load_first(bold_candidates, 24),
        "card": load_first(bold_candidates, 22),
        "text": load_first(regular_candidates, 18),
        "text_bold": load_first(bold_candidates, 18),
        "small": load_first(regular_candidates, 16),
        "tiny": load_first(regular_candidates, 15),
    }


def _dashboard_pages(items, page_size=6):
    items = list(items or [])
    if not items:
        return [[]]
    return [items[i:i + page_size] for i in range(0, len(items), page_size)]


def make_dashboard_image(items, title="STOCKS TO WATCH", subtitle="LIVE + OI + RISK", page_no=1, total_pages=1):
    fonts = _load_fonts()
    W, H = 1080, 1700
    img = Image.new("RGB", (W, H), (244, 247, 252))
    draw = ImageDraw.Draw(img)

    header_bg = (229, 57, 53)
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
    dark_panel = (33, 43, 54)
    accent = (255, 193, 7)
    white = (255, 255, 255)

    def tsize(val, font):
        bbox = draw.textbbox((0, 0), str(val), font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]

    def human(x):
        x = safe_float(x, 0.0)
        sign = "-" if x < 0 else ""
        x = abs(x)
        if x >= 100000:
            return f"{sign}{x/100000:.1f}L".rstrip("0").rstrip(".")
        if x >= 1000:
            return f"{sign}{x/1000:.0f}K"
        return f"{sign}{int(x)}"

    def arrow(v):
        v = safe_float(v, 0.0)
        if v > 0:
            return "↑"
        if v < 0:
            return "↓"
        return "→"

    def result_color(txt):
        t = str(txt).lower()
        if "target" in t:
            return profit
        if "stoploss" in t:
            return loss
        if "exit" in t:
            return profit
        if "hold" in t and "buy" in t:
            return profit
        if "hold" in t and "sell" in t:
            return loss
        return text_dark

    def normalize_side_data(item):
        buy = item.get("buy", {}) or {}
        sell = item.get("sell", {}) or {}

        if not buy and item.get("side") == "BUY":
            buy = {
                "entry": item.get("entry", ""),
                "target": item.get("target", ""),
                "stoploss": item.get("stoploss", ""),
                "result": item.get("result", ""),
                "exit_price": item.get("exit_price", ""),
                "pl": item.get("pl", ""),
                "qty": item.get("qty", ""),
            }

        if not sell and item.get("side") == "SELL":
            sell = {
                "entry": item.get("entry", ""),
                "target": item.get("target", ""),
                "stoploss": item.get("stoploss", ""),
                "result": item.get("result", ""),
                "exit_price": item.get("exit_price", ""),
                "pl": item.get("pl", ""),
                "qty": item.get("qty", ""),
            }

        return buy, sell

    def infer_status(data, side):
        res = str(data.get("result", "") or "")
        if not res:
            return f"{side} • WATCH"
        if "Target" in res:
            return f"{side} • TARGET"
        if "Stoploss" in res:
            return f"{side} • STOPLOSS"
        if "Exit" in res:
            return f"{side} • EXIT"
        if "Day End" in res:
            return f"{side} • DAY END"
        if "Hold" in res:
            return res
        return f"{side} • HOLD"

    def compute_score(item):
        score = 84
        buy, sell = normalize_side_data(item)
        for side_data in (buy, sell):
            if side_data:
                result = str(side_data.get("result", ""))
                if "Target" in result:
                    score = max(score, 98)
                elif "Stoploss" in result:
                    score = max(score, 88)
                elif "Day End" in result or "Exit" in result:
                    score = max(score, 91)
                elif side_data.get("entry", "") != "":
                    score = max(score, 94)
        return score

    # Header
    draw.rounded_rectangle((24, 24, W - 24, 150), radius=28, fill=header_bg)
    draw.text((42, 36), title, font=fonts["title"], fill=white)
    draw.text((42, 98), subtitle, font=fonts["sub"], fill=white)

    right_txt = now_ist().strftime("%b %d %H:%M").upper()
    if total_pages > 1:
        right_txt += f"  P{page_no}/{total_pages}"
    rw, _ = tsize(right_txt, fonts["sub"])
    draw.text((W - 42 - rw, 98), right_txt, font=fonts["sub"], fill=(255, 235, 235))

    # Stats
    watch = len(pattern_summary.get("gapup", [])) + len(pattern_summary.get("inside15", [])) + len(pattern_summary.get("pivot30", []))
    active_n = len(active_trades)
    target_n = len(eod_stats.get("targets", []))
    sl_n = len(eod_stats.get("stoplosses", []))
    blocked_n = len(eod_stats.get("blocked", []))
    net_pnl = round(sum(x.get("pnl", 0.0) for x in eod_stats.get("closed", [])), 2)

    stats = [
        ("Watch", str(watch)),
        ("Active", str(active_n)),
        ("Target", str(target_n)),
        ("Stoploss", str(sl_n)),
        ("Blocked", str(blocked_n)),
        ("P/L", f"₹{net_pnl:+,.0f}")
    ]

    draw.rounded_rectangle((24, 170, W - 24, 255), radius=22, fill=dark_panel)
    x = 40
    for label, val in stats:
        draw.text((x, 184), label, font=fonts["small"], fill=(190, 205, 220))
        fill = white if label != "P/L" else ((124, 255, 183) if net_pnl >= 0 else (255, 170, 170))
        draw.text((x, 212), val, font=fonts["card"], fill=fill)
        x += 160

    # Top ranked
    ranked = sorted([(str(item.get("symbol", "")), compute_score(item)) for item in items], key=lambda z: z[1], reverse=True)[:3]
    draw.rounded_rectangle((24, 275, W - 24, 355), radius=18, fill=panel_bg, outline=border, width=2)
    draw.text((42, 298), "TOP RANKED SETUPS", font=fonts["card"], fill=text_dark)

    rx = 286
    for idx, (name, score) in enumerate(ranked, 1):
        color = profit if idx != 2 else loss
        draw.text((rx, 304), f"{idx}) {name} {score}%", font=fonts["small"], fill=color)
        rx += 190

    draw.text((42, 328), "Smart filter: only strongest OI-confirmed setups shown first", font=fonts["tiny"], fill=muted)

    def draw_card(x, y, item):
        buy, sell = normalize_side_data(item)

        if buy and buy.get("entry"):
            side = "BUY"
            data = buy
            rows = item.get("buy_oi_rows", []) or []
        elif sell and sell.get("entry"):
            side = "SELL"
            data = sell
            rows = item.get("sell_oi_rows", []) or []
        else:
            side = "WATCH"
            data = {}
            rows = []

        score = compute_score(item)

        draw.rounded_rectangle((x, y, x + 500, y + 390), radius=24, fill=panel_bg, outline=border, width=2)

        header_fill = buy_bar if side == "BUY" else sell_bar
        title_fill = text_dark if side == "BUY" else white
        draw.rounded_rectangle((x + 14, y + 14, x + 486, y + 56), radius=14, fill=header_fill)
        draw.text((x + 26, y + 20), str(item.get("symbol", "")), font=fonts["card"], fill=title_fill)
        draw.text((x + 405, y + 20), f"{score}%", font=fonts["card"], fill=title_fill)

        soft_fill = buy_box if side == "BUY" else sell_box
        draw.rounded_rectangle((x + 14, y + 68, x + 486, y + 106), radius=12, fill=soft_fill)

        strategy = str(item.get("strategy", "")).replace("_", " ").upper()
        status_txt = infer_status(data, side)
        full_status = f"{strategy} • {status_txt}" if strategy else status_txt
        draw.text((x + 26, y + 76), full_status, font=fonts["text_bold"], fill=result_color(full_status))

        draw.text((x + 26, y + 122), "Entry:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 92, y + 122), str(data.get("entry", "")), font=fonts["text"], fill=text_dark)

        draw.text((x + 175, y + 122), "SL:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 212, y + 122), str(data.get("stoploss", "")), font=fonts["text"], fill=text_dark)

        draw.text((x + 280, y + 122), "Target:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 362, y + 122), str(data.get("target", "")), font=fonts["text"], fill=text_dark)

        draw.text((x + 26, y + 158), "Qty:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 74, y + 158), str(data.get("qty", "-")), font=fonts["text"], fill=text_dark)

        pl_text = str(data.get("pl", ""))
        pl_fill = profit if pl_text.startswith("+") else loss if pl_text.startswith("-") else neutral
        draw.text((x + 150, y + 158), "P/L:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 198, y + 158), pl_text, font=fonts["text"], fill=pl_fill)
        draw.text((x + 322, y + 158), f"{int(LEVERAGE)}X", font=fonts["text_bold"], fill=accent)

        draw.rounded_rectangle((x + 14, y + 200, x + 486, y + 370), radius=12, fill=(248, 250, 252))
        draw.text((x + 26, y + 214), "Strike", font=fonts["small"], fill=muted)
        draw.text((x + 120, y + 214), "PE OI", font=fonts["small"], fill=muted)
        draw.text((x + 210, y + 214), "PE Chg", font=fonts["small"], fill=muted)
        draw.text((x + 300, y + 214), "|", font=fonts["small"], fill=muted)
        draw.text((x + 335, y + 214), "CE OI", font=fonts["small"], fill=muted)
        draw.text((x + 420, y + 214), "CE Chg", font=fonts["small"], fill=muted)

        yy = y + 246
        for r in rows[:5]:
            draw.text((x + 26, yy), str(r.get("strike", "")), font=fonts["small"], fill=text_dark)
            draw.text((x + 120, yy), human(r.get("put_oi", 0)), font=fonts["small"], fill=text_dark)
            draw.text((x + 210, yy), f"{human(r.get('put_oich', 0))}{arrow(r.get('put_oich', 0))}", font=fonts["small"],
                      fill=profit if safe_float(r.get("put_oich", 0), 0) > 0 else loss if safe_float(r.get("put_oich", 0), 0) < 0 else muted)
            draw.text((x + 300, yy), "|", font=fonts["small"], fill=muted)
            draw.text((x + 335, yy), human(r.get("call_oi", 0)), font=fonts["small"], fill=text_dark)
            draw.text((x + 420, yy), f"{human(r.get('call_oich', 0))}{arrow(r.get('call_oich', 0))}", font=fonts["small"],
                      fill=profit if safe_float(r.get("call_oich", 0), 0) > 0 else loss if safe_float(r.get("call_oich", 0), 0) < 0 else muted)
            yy += 28

    positions = [
        (40, 390), (540, 390),
        (40, 800), (540, 800),
        (40, 1210), (540, 1210),
    ]
    for pos, item in zip(positions, items[:6]):
        draw_card(pos[0], pos[1], item)

    bio = BytesIO()
    bio.name = f"ultimate_dashboard_p{page_no}.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio


def test_dashboard_send():
    gap_items = convert_gapup_items_for_dashboard(pattern_summary.get("gapup", []))
    inside_items = convert_inside_items_for_dashboard(pattern_summary.get("inside15", []))
    pivot_items = convert_pivot_items_for_dashboard(pattern_summary.get("pivot30", []))

    items = gap_items + inside_items + pivot_items
    send_dashboard_image(items, title="STOCKS TO WATCH", subtitle="LIVE + OI + RISK", caption="Live Dashboard")
# ================= TELEGRAM SEND =================
def send_to_telegram(image_path):
    BOT_TOKEN = "8619123498:AAGmqno7hYGsDcTjMPpFHKQ-Ps7rvtrHyx0"
    CHAT_ID = 7641895913

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    with open(image_path, "rb") as img:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"photo": img})

# ================= RUN =================
file = test_dashboard_send()
send_to_telegram(file)

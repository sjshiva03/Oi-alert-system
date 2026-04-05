from PIL import Image, ImageDraw, ImageFont
import requests
from datetime import datetime
import os
import PIL

# ================= FONT LOADER =================
def load_fonts():
    pil_dir = os.path.dirname(PIL.__file__)
    font_path_bold = os.path.join(pil_dir, "fonts/DejaVuSans-Bold.ttf")
    font_path = os.path.join(pil_dir, "fonts/DejaVuSans.ttf")

    return {
        "title": ImageFont.truetype(font_path_bold, 80),
        "header": ImageFont.truetype(font_path_bold, 40),
        "card_title": ImageFont.truetype(font_path_bold, 38),
        "text": ImageFont.truetype(font_path, 32),
        "small": ImageFont.truetype(font_path, 28),
        "tiny": ImageFont.truetype(font_path, 26),
    }

# ================= DRAW IMAGE =================
def create_dashboard(data, file_name="dashboard.png"):
    fonts = load_fonts()

    W, H = 1920, 2200
    img = Image.new("RGB", (W, H), "#f2f3f5")
    d = ImageDraw.Draw(img)

    # ===== HEADER =====
    d.rounded_rectangle([40, 40, W-40, 200], radius=40, fill="#e53935")
    d.text((80, 70), "STOCKS TO WATCH", font=fonts["title"], fill="white")
    d.text((80, 140), "LIVE + OI + RISK", font=fonts["header"], fill="white")

    d.text((W-400, 90), datetime.now().strftime("%b %d %H:%M"),
           font=fonts["header"], fill="white")

    # ===== STATS BAR =====
    d.rounded_rectangle([40, 230, W-40, 340], radius=30, fill="#1f2a38")

    stats = ["Watch", "Active", "Target", "Stoploss", "Blocked", "P/L"]
    vals = ["18", "6", "7", "3", "2", "+₹4280"]

    x = 100
    for i in range(len(stats)):
        d.text((x, 250), stats[i], font=fonts["small"], fill="white")
        color = "#00c853" if i == 5 else "white"
        d.text((x, 290), vals[i], font=fonts["header"], fill=color)
        x += 300

    # ===== TOP PERFORMERS =====
    d.rounded_rectangle([40, 370, W-40, 460], radius=25, fill="#e0e0e0")
    d.text((80, 395), "TOP RANKED SETUPS", font=fonts["header"], fill="#333")

    d.text((500, 395), "1) RELIANCE 98%", font=fonts["small"], fill="#00c853")
    d.text((850, 395), "2) INFY 94%", font=fonts["small"], fill="#e53935")
    d.text((1150, 395), "3) HDFCBANK 91%", font=fonts["small"], fill="#00c853")

    # ===== CARD DRAW FUNCTION =====
    def draw_card(x, y, item):
        w, h = 860, 360

        d.rounded_rectangle([x, y, x+w, y+h], radius=30, fill="white")

        # header color
        header_color = "#1faa80" if item["type"] == "BUY" else "#e53935"

        d.rounded_rectangle([x+10, y+10, x+w-10, y+80],
                            radius=25, fill=header_color)

        d.text((x+30, y+20), item["name"],
               font=fonts["card_title"], fill="white")

        d.text((x+w-140, y+20), item["score"],
               font=fonts["card_title"], fill="white")

        # strategy
        d.rounded_rectangle([x+20, y+100, x+w-20, y+160],
                            radius=20, fill="#e8f5e9")

        d.text((x+40, y+110), item["strategy"],
               font=fonts["text"], fill="#333")

        # details
        d.text((x+40, y+180),
               f"Entry: {item['entry']}   SL: {item['sl']}   Target: {item['target']}",
               font=fonts["text"], fill="#333")

        pl_color = "#00c853" if "+" in item["pl"] else "#e53935"

        d.text((x+40, y+230),
               f"Qty: {item['qty']}    P/L: {item['pl']}",
               font=fonts["text"], fill=pl_color)

    # ===== SAMPLE DATA =====
    sample = [
        {"name":"RELIANCE-1340","score":"98%","type":"BUY","strategy":"15M INSIDE • BUY HOLD","entry":2400,"sl":2380,"target":2420,"qty":25,"pl":"+₹1200"},
        {"name":"INFY-1452","score":"94%","type":"SELL","strategy":"PIVOT • SELL HOLD","entry":1450,"sl":1470,"target":1430,"qty":20,"pl":"-₹500"},
        {"name":"HDFC-1608","score":"91%","type":"BUY","strategy":"GAP UP • BUY EXIT","entry":1600,"sl":1588,"target":1624,"qty":40,"pl":"+₹720"},
        {"name":"SBIN-782","score":"88%","type":"SELL","strategy":"15M INSIDE • STOPLOSS","entry":780,"sl":792,"target":756,"qty":42,"pl":"-₹504"},
        {"name":"ICICI-1210","score":"86%","type":"BUY","strategy":"PIVOT • BUY HOLD","entry":1210,"sl":1198,"target":1228,"qty":38,"pl":"+₹684"},
        {"name":"TCS-3910","score":"84%","type":"SELL","strategy":"GAP UP • SELL HOLD","entry":3910,"sl":3942,"target":3860,"qty":15,"pl":"+₹750"},
    ]

    # ===== DRAW GRID =====
    x1, x2 = 40, 1000
    y = 500

    for i, item in enumerate(sample):
        if i % 2 == 0:
            draw_card(x1, y, item)
        else:
            draw_card(x2, y, item)
            y += 400

    img.save(file_name)
    return file_name

# ================= TELEGRAM SEND =================
def send_to_telegram(image_path):
    BOT_TOKEN = "8619123498:AAGmqno7hYGsDcTjMPpFHKQ-Ps7rvtrHyx0"
    CHAT_ID = 7641895913

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    with open(image_path, "rb") as img:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"photo": img})

# ================= RUN =================
file = create_dashboard()
send_to_telegram(file)

import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

TELEGRAM_TOKEN = "YOUR_BOT_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

def load_fonts():
    try:
        return {
            "title": ImageFont.truetype("DejaVuSans-Bold.ttf", 44),
            "sub": ImageFont.truetype("DejaVuSans-Bold.ttf", 20),
            "card": ImageFont.truetype("DejaVuSans-Bold.ttf", 20),
            "text": ImageFont.truetype("DejaVuSans.ttf", 17),
            "text_bold": ImageFont.truetype("DejaVuSans-Bold.ttf", 17),
            "small": ImageFont.truetype("DejaVuSans.ttf", 15),
        }
    except Exception:
        f = ImageFont.load_default()
        return {
            "title": f,
            "sub": f,
            "card": f,
            "text": f,
            "text_bold": f,
            "small": f,
        }

def build_live_dashboard_image():
    fonts = load_fonts()

    W, H = 1080, 1700
    img = Image.new("RGB", (W, H), (245, 247, 252))
    draw = ImageDraw.Draw(img)

    # Colors
    header_bg = (233, 57, 50)
    dark_panel = (31, 41, 55)
    card_bg = (255, 255, 255)
    border = (224, 228, 235)
    text_dark = (28, 33, 40)
    muted = (108, 117, 125)
    buy_bar = (43, 201, 160)
    sell_bar = (239, 83, 80)
    buy_soft = (232, 249, 241)
    sell_soft = (252, 236, 234)
    profit = (25, 135, 84)
    loss = (220, 53, 69)
    accent = (255, 193, 7)
    white = (255, 255, 255)

    def text_width(txt, font):
        box = draw.textbbox((0, 0), str(txt), font=font)
        return box[2] - box[0]

    # Header
    draw.rounded_rectangle((24, 24, W - 24, 150), radius=28, fill=header_bg)
    draw.text((42, 40), "STOCKS TO WATCH", font=fonts["title"], fill=white)
    draw.text((42, 96), "LIVE + OI + RISK", font=fonts["sub"], fill=white)
    right_txt = "APR 04 10:15"
    draw.text((W - 42 - text_width(right_txt, fonts["sub"]), 96), right_txt, font=fonts["sub"], fill=(255, 235, 235))

    # Stats
    stats = [
        ("Watch", "18"),
        ("Active", "6"),
        ("Target", "7"),
        ("Stoploss", "3"),
        ("Blocked", "2"),
        ("P/L", "+₹4280"),
    ]
    draw.rounded_rectangle((24, 170, W - 24, 255), radius=22, fill=dark_panel)

    x = 40
    for label, value in stats:
        draw.text((x, 186), label, font=fonts["small"], fill=(195, 205, 220))
        value_color = white if label != "P/L" else (124, 255, 183)
        draw.text((x, 212), value, font=fonts["card"], fill=value_color)
        x += 160

    # Ranked row
    draw.rounded_rectangle((24, 275, W - 24, 355), radius=18, fill=card_bg, outline=border, width=2)
    draw.text((42, 300), "TOP RANKED SETUPS", font=fonts["card"], fill=text_dark)
    draw.text((286, 304), "1) RELIANCE 98%", font=fonts["small"], fill=profit)
    draw.text((485, 304), "2) INFY 94%", font=fonts["small"], fill=loss)
    draw.text((655, 304), "3) HDFCBANK 91%", font=fonts["small"], fill=profit)
    draw.text((42, 328), "Smart filter: only strongest OI-confirmed setups shown first", font=fonts["small"], fill=muted)

    cards = [
        {
            "symbol": "RELIANCE-1340.3", "score": "98%", "side": "BUY",
            "status": "15M INSIDE • BUY • BUY HOLD",
            "entry": "2400", "sl": "2380", "target": "2420", "qty": "25", "pl": "+₹1200",
            "rows": [("2960","1.2L","18K↑","57K","12K↓"),("2980","1.5L","24K↑","71K","16K↓"),
                     ("3000","2.4L","45K↑","1.1L","30K↓"),("3020","1.8L","38K↑","92K","25K↓"),
                     ("3040","1.6L","41K↑","84K","22K↓")]
        },
        {
            "symbol": "INFY-1452.8", "score": "94%", "side": "SELL",
            "status": "PIVOT • SELL • SELL HOLD",
            "entry": "1450", "sl": "1470", "target": "1430", "qty": "20", "pl": "-₹500",
            "rows": [("1410","62K","24K↓","1.4L","42K↑"),("1430","71K","28K↓","1.6L","48K↑"),
                     ("1450","83K","30K↓","1.9L","50K↑"),("1470","55K","21K↓","1.2L","37K↑"),
                     ("1490","44K","16K↓","98K","29K↑")]
        },
        {
            "symbol": "HDFCBANK-1608.5", "score": "91%", "side": "BUY",
            "status": "GAP UP • BUY • EXIT",
            "entry": "1600", "sl": "1588", "target": "1624", "qty": "40", "pl": "+₹720",
            "rows": [("1560","48K","11K↑","31K","08K↓"),("1580","61K","16K↑","44K","12K↓"),
                     ("1600","82K","20K↑","59K","14K↓"),("1620","77K","19K↑","52K","12K↓"),
                     ("1640","69K","17K↑","46K","10K↓")]
        },
        {
            "symbol": "SBIN-782.4", "score": "88%", "side": "SELL",
            "status": "15M INSIDE • SELL • STOPLOSS",
            "entry": "780", "sl": "792", "target": "756", "qty": "42", "pl": "-₹504",
            "rows": [("740","29K","08K↓","91K","44K↑"),("760","34K","10K↓","84K","39K↑"),
                     ("780","41K","12K↓","72K","35K↑"),("800","28K","09K↓","58K","27K↑"),
                     ("820","22K","07K↓","47K","22K↑")]
        },
        {
            "symbol": "ICICIBANK-1210.2", "score": "86%", "side": "BUY",
            "status": "PIVOT • BUY • BUY HOLD",
            "entry": "1210", "sl": "1198", "target": "1228", "qty": "38", "pl": "+₹684",
            "rows": [("1170","33K","09K↑","21K","06K↓"),("1190","49K","14K↑","38K","11K↓"),
                     ("1210","88K","26K↑","64K","18K↓"),("1230","61K","19K↑","47K","13K↓"),
                     ("1250","42K","12K↑","31K","09K↓")]
        },
        {
            "symbol": "TCS-3910.7", "score": "84%", "side": "SELL",
            "status": "GAP UP • SELL • SELL HOLD",
            "entry": "3910", "sl": "3942", "target": "3860", "qty": "15", "pl": "+₹750",
            "rows": [("3820","24K","08K↓","57K","21K↑"),("3860","31K","09K↓","88K","36K↑"),
                     ("3900","42K","14K↓","79K","31K↑"),("3940","28K","10K↓","66K","26K↑"),
                     ("3980","19K","07K↓","49K","19K↑")]
        },
    ]

    def draw_card(x, y, d):
        is_buy = d["side"] == "BUY"
        header_fill = buy_bar if is_buy else sell_bar
        header_text = text_dark if is_buy else white
        soft_fill = buy_soft if is_buy else sell_soft

        low_status = d["status"].lower()
        if "stoploss" in low_status:
            status_color = loss
        elif "exit" in low_status:
            status_color = profit
        elif "buy hold" in low_status or "sell hold" in low_status:
            status_color = profit if is_buy else loss
        else:
            status_color = text_dark

        pl_color = profit if str(d["pl"]).startswith("+") else loss

        draw.rounded_rectangle((x, y, x + 500, y + 390), radius=24, fill=card_bg, outline=border, width=2)
        draw.rounded_rectangle((x + 14, y + 14, x + 486, y + 56), radius=14, fill=header_fill)
        draw.text((x + 26, y + 23), d["symbol"], font=fonts["card"], fill=header_text)
        draw.text((x + 406, y + 23), d["score"], font=fonts["card"], fill=header_text)

        draw.rounded_rectangle((x + 14, y + 68, x + 486, y + 106), radius=12, fill=soft_fill)
        draw.text((x + 26, y + 77), d["status"], font=fonts["text_bold"], fill=status_color)

        draw.text((x + 26, y + 124), "Entry:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 90, y + 124), d["entry"], font=fonts["text"], fill=text_dark)
        draw.text((x + 170, y + 124), "SL:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 208, y + 124), d["sl"], font=fonts["text"], fill=text_dark)
        draw.text((x + 280, y + 124), "Target:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 358, y + 124), d["target"], font=fonts["text"], fill=text_dark)

        draw.text((x + 26, y + 160), "Qty:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 74, y + 160), d["qty"], font=fonts["text"], fill=text_dark)
        draw.text((x + 146, y + 160), "P/L:", font=fonts["text_bold"], fill=text_dark)
        draw.text((x + 194, y + 160), d["pl"], font=fonts["text"], fill=pl_color)
        draw.text((x + 320, y + 160), "5X", font=fonts["text_bold"], fill=accent)

        draw.rounded_rectangle((x + 14, y + 200, x + 486, y + 370), radius=12, fill=(248, 250, 252))
        draw.text((x + 26, y + 214), "Strike", font=fonts["small"], fill=muted)
        draw.text((x + 120, y + 214), "PE OI", font=fonts["small"], fill=muted)
        draw.text((x + 210, y + 214), "PE Chg", font=fonts["small"], fill=muted)
        draw.text((x + 300, y + 214), "|", font=fonts["small"], fill=muted)
        draw.text((x + 335, y + 214), "CE OI", font=fonts["small"], fill=muted)
        draw.text((x + 420, y + 214), "CE Chg", font=fonts["small"], fill=muted)

        yy = y + 244
        for strike, peoi, pechg, ceoi, cechg in d["rows"]:
            draw.text((x + 26, yy), strike, font=fonts["small"], fill=text_dark)
            draw.text((x + 120, yy), peoi, font=fonts["small"], fill=text_dark)
            draw.text((x + 210, yy), pechg, font=fonts["small"], fill=profit if "↑" in pechg else loss)
            draw.text((x + 300, yy), "|", font=fonts["small"], fill=muted)
            draw.text((x + 335, yy), ceoi, font=fonts["small"], fill=text_dark)
            draw.text((x + 420, yy), cechg, font=fonts["small"], fill=profit if "↑" in cechg else loss)
            yy += 28

    positions = [
        (40, 390), (540, 390),
        (40, 800), (540, 800),
        (40, 1210), (540, 1210),
    ]

    for pos, card in zip(positions, cards):
        draw_card(pos[0], pos[1], card)

    bio = BytesIO()
    bio.name = "dashboard.png"
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio

def send_dashboard_to_telegram():
    img = build_live_dashboard_image()
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    files = {
        "photo": ("dashboard.png", img, "image/png")
    }
    data = {
        "chat_id": CHAT_ID,
        "caption": "Live Dashboard"
    }
    r = requests.post(url, data=data, files=files, timeout=60)
    print(r.status_code, r.text)

if __name__ == "__main__":
    send_dashboard_to_telegram()

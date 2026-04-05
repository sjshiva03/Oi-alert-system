import requests
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

TELEGRAM_TOKEN = "YOUR_BOT_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

# ---------------- FONT ----------------
def load_fonts():
    try:
        return {
            "title": ImageFont.truetype("DejaVuSans-Bold.ttf", 44),
            "sub": ImageFont.truetype("DejaVuSans-Bold.ttf", 20),
            "card": ImageFont.truetype("DejaVuSans-Bold.ttf", 20),
            "text": ImageFont.truetype("DejaVuSans.ttf", 17),
            "small": ImageFont.truetype("DejaVuSans.ttf", 15),
        }
    except:
        f = ImageFont.load_default()
        return {k: f for k in ["title","sub","card","text","small"]}

# ---------------- IMAGE ----------------
def build_dashboard():
    fonts = load_fonts()

    W, H = 1080, 1200
    img = Image.new("RGB", (W, H), (245, 247, 252))
    draw = ImageDraw.Draw(img)

    # COLORS
    red = (233, 57, 50)
    dark = (31, 41, 55)
    white = (255,255,255)
    green = (40,180,120)
    light_green = (230,250,240)
    light_red = (252,236,234)
    border = (224,228,235)

    # HEADER
    draw.rounded_rectangle((20,20,W-20,140), radius=25, fill=red)
    draw.text((40,50),"STOCKS TO WATCH",font=fonts["title"],fill=white)
    draw.text((40,100),"LIVE + OI + RISK",font=fonts["sub"],fill=white)

    # STATS
    draw.rounded_rectangle((20,160,W-20,240), radius=20, fill=dark)

    stats = ["Watch\n18","Active\n6","Target\n7","Stoploss\n3","Blocked\n2","P/L\n+₹4280"]
    x=40
    for s in stats:
        label,value=s.split("\n")
        draw.text((x,170),label,font=fonts["small"],fill=(180,200,220))
        draw.text((x,200),value,font=fonts["card"],fill=white)
        x+=170

    # SAMPLE CARD FUNCTION
    def draw_card(x,y,title,score,side):
        bar_color = green if side=="BUY" else (230,60,60)
        soft = light_green if side=="BUY" else light_red

        draw.rounded_rectangle((x,y,x+480,y+260),radius=20,fill=white,outline=border)

        # header
        draw.rounded_rectangle((x+10,y+10,x+470,y+50),radius=12,fill=bar_color)
        draw.text((x+20,y+20),title,font=fonts["card"],fill=white)
        draw.text((x+400,y+20),score,font=fonts["card"],fill=white)

        # status
        draw.rounded_rectangle((x+10,y+60,x+470,y+95),radius=10,fill=soft)
        draw.text((x+20,y+68),f"{side} • HOLD",font=fonts["text"],fill=bar_color)

        # details
        draw.text((x+20,y+110),"Entry: 2400",font=fonts["text"],fill=(0,0,0))
        draw.text((x+200,y+110),"SL: 2380",font=fonts["text"],fill=(0,0,0))
        draw.text((x+320,y+110),"Target: 2420",font=fonts["text"],fill=(0,0,0))

        draw.text((x+20,y+140),"Qty: 25",font=fonts["text"],fill=(0,0,0))
        draw.text((x+140,y+140),"P/L: +₹1200",font=fonts["text"],fill=green)

    # DRAW 6 CARDS
    draw_card(40,260,"RELIANCE", "98%", "BUY")
    draw_card(540,260,"INFY", "94%", "SELL")
    draw_card(40,540,"HDFCBANK", "91%", "BUY")
    draw_card(540,540,"SBIN", "88%", "SELL")
    draw_card(40,820,"ICICIBANK", "86%", "BUY")
    draw_card(540,820,"TCS", "84%", "SELL")

    bio = BytesIO()
    bio.name = "dashboard.png"
    img.save(bio, format="PNG")
    bio.seek(0)

    return bio

# ---------------- TELEGRAM ----------------
def send_to_telegram(image):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    files = {"photo": ("dashboard.png", image, "image/png")}
    data = {"chat_id": CHAT_ID, "caption": "Live Dashboard"}
    r = requests.post(url, data=data, files=files)
    print(r.text)

# ---------------- RUN ----------------
if __name__ == "__main__":
    img = build_dashboard()
    send_to_telegram(img)

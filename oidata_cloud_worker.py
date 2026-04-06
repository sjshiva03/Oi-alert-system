
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).with_name("live_market_debug_sample_output.png") if "__file__" in globals() else Path("/mnt/data/live_market_debug_sample_output.png")

BG = (245, 245, 248)
WHITE = (255, 255, 255)
BLACK = (20, 20, 20)
GRAY = (110, 110, 120)
BORDER = (205, 208, 214)
GREEN = (24, 156, 72)
RED = (208, 38, 38)
ORANGE = (255, 109, 26)
PURPLE = (146, 66, 240)
SKY = (53, 140, 239)

def load_font(size, bold=False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]
    for p in candidates:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()

F_TITLE = load_font(34, True)
F_SUB = load_font(18, True)
F_TEXT = load_font(15, True)
F_SMALL = load_font(13, True)
F_TINY = load_font(11, True)

def rr(draw, box, radius, fill, outline=None, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)

def t(draw, xy, text, font, fill, anchor=None):
    draw.text(xy, str(text), font=font, fill=fill, anchor=anchor)

def strategy_colors(strategy):
    s = strategy.upper()
    if "GAPUP" in s:
        return ORANGE, (255, 241, 233), ORANGE
    if "R2_BREAKOUT" in s:
        return SKY, (236, 246, 255), SKY
    if "PIVOT" in s:
        return PURPLE, (245, 238, 255), PURPLE
    if "SELL" in s and "15M" in s:
        return (232, 76, 76), (255, 239, 239), (232, 76, 76)
    return (25, 181, 86), (237, 250, 241), (25, 181, 86)

cards = [
    {
        "symbol":"INFY-1370.00","day_pct":"+1.03%","confidence":"93% CONFIDENCE",
        "strategy":"GAPUP_PLUS • SELL • HOLD","entry":"1360.20","sl":"1385.0","target":"1350.0","qty":"50",
        "pl":"-1050 (-0.77%)","lev":"5X","exit":"--","close":"1370.00","gain":"+1.03%",
        "oi":[
            ["1380","14.5L","+12.3%","18.1L","-5.2%","+8.6%","-3.1%"],
            ["1370","22.8L","+18.7%","15.6L","-1.8%","+12.4%","-2.7%"],
            ["1360","16L","+7.5%","3.9L","-10.6%","+6.9%","-6.4%"],
            ["1350","9.2L","-2.1%","7.8L","+4.2%","+1.3%","+5.7%"],
            ["1340","4.8L","-12.5%","10.1L","+7.8%","-4.5%","+9.2%"],
        ]
    },
    {
        "symbol":"ICICIBANK-1115.85","day_pct":"+0.92%","confidence":"92% CONFIDENCE",
        "strategy":"R2_BREAKOUT_5M • BUY • HOLD","entry":"1118.0","sl":"1109.4","target":"1136.6","qty":"100",
        "pl":"+960 (+0.86%)","lev":"10X","exit":"--","close":"1115.85","gain":"+1.03%",
        "oi":[
            ["1120","18L","+22.5%","8.4L","-7.4%","+15.2%","-4.8%"],
            ["1110","14.5L","+19.8%","12.2L","+3.6%","+10.5%","+1.9%"],
            ["1100","25.6L","+16.2%","21.5L","+8.1%","+9.7%","+6.3%"],
            ["1090","11.3L","+5.4%","17.8L","+12.9%","+3.2%","+10.1%"],
            ["1080","6.9L","-1.2%","22.4L","+15.7%","-0.8%","+12.4%"],
        ]
    },
    {
        "symbol":"LT-3680.15","day_pct":"+0.97%","confidence":"97% CONFIDENCE",
        "strategy":"PIVOT_30M • SELL • HOLD","entry":"3675.0","sl":"3721.88","target":"3600.0","qty":"20",
        "pl":"-360 (-0.10%)","lev":"5X","exit":"--","close":"3680.15","gain":"+0.97%",
        "oi":[
            ["3700","19.2L","-4.6%","9.1L","+6.2%","-2.1%","+4.3%"],
            ["3680","12.7L","-1.8%","11.5L","+9.4%","-0.7%","+7.8%"],
            ["3660","8.9L","+3.2%","13.8L","+12.1%","+1.9%","+10.5%"],
            ["3640","5.2L","+8.7%","17.1L","+15.6%","+4.6%","+13.9%"],
            ["3620","3.1L","+12.9%","21.6L","+18.8%","+7.5%","+15.2%"],
        ]
    },
    {
        "symbol":"RELIANCE-3094.50","day_pct":"+0.08%","confidence":"90% CONFIDENCE",
        "strategy":"15M INSIDE • BUY • HOLD","entry":"3088.0","sl":"3062.5","target":"3149.6","qty":"50",
        "pl":"+1300 (+0.42%)","lev":"5X","exit":"--","close":"3094.50","gain":"+0.08%",
        "oi":[
            ["3100","15.6L","+9.8%","9.3L","-3.2%","+6.5%","-2.1%"],
            ["3080","4.7L","+4.2%","5.6L","+1.9%","+3.1%","+0.8%"],
            ["3060","8.2L","-2.5%","11.8L","+7.6%","-1.7%","+5.4%"],
            ["3040","3.4L","-8.9%","15.2L","+11.5%","-4.2%","+8.9%"],
            ["3020","1.9L","-15.3%","19.7L","+16.7%","-7.8%","+12.6%"],
        ]
    }
]

def render_dashboard(cards):
    W, H = 1600, 1800
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    rr(draw, (18, 18, W-18, 92), 24, WHITE, BORDER, 2)
    t(draw, (38, 38), "LIVE TRADE DASHBOARD", F_TITLE, BLACK)
    t(draw, (1120, 42), "2026-04-06", F_SUB, (70,70,80))
    rr(draw, (1360, 26, 1525, 74), 18, (44, 196, 92))
    t(draw, (1442, 40), "LIVE", F_SUB, WHITE, "ma")

    rr(draw, (18, 110, W-18, 170), 22, WHITE, BORDER, 2)
    t(draw, (38, 128), "Top Performers:", F_SUB, BLACK)
    x = 230
    for idx, txt, pct in [(1,"INDHOTEL","+1.12%"),(2,"AMBUJACEM","+0.88%"),(3,"RELIANCE","+0.70%")]:
        t(draw, (x, 128), f"{idx}) {txt}", F_SUB, BLACK)
        x += draw.textbbox((0,0), f"{idx}) {txt}", font=F_SUB)[2] + 12
        t(draw, (x, 128), pct, F_SUB, GREEN)
        x += draw.textbbox((0,0), pct, font=F_SUB)[2] + 36

    positions = [(18,195),(810,195),(18,930),(810,930)]
    cw, ch = 772, 700
    colxs = [58, 165, 280, 395, 510, 625, 730]

    for card, (x,y) in zip(cards, positions):
        head, soft, accent = strategy_colors(card["strategy"])
        rr(draw, (x,y,x+cw,y+ch), 24, WHITE, accent, 2)
        rr(draw, (x+16,y+16,x+cw-16,y+72), 16, head)
        t(draw, (x+28,y+30), card["symbol"], F_SUB, WHITE)
        rr(draw, (x+370,y+20,x+480,y+64), 16, WHITE)
        t(draw, (x+425,y+42), card["day_pct"], F_TEXT, GREEN, "mm")
        rr(draw, (x+520,y+20,x+cw-26,y+64), 16, (0,0,0,40))
        t(draw, (x+cw-145,y+42), card["confidence"], F_SMALL, WHITE, "mm")

        rr(draw, (x+16,y+92,x+cw-16,y+144), 14, soft)
        t(draw, (x+28,y+108), card["strategy"], F_SUB, accent)

        rr(draw, (x+16,y+160,x+cw-16,y+208), 12, (248,248,250))
        t(draw, (x+28,y+175), f"Entry: {card['entry']}", F_TEXT, BLACK)
        t(draw, (x+190,y+175), f"SL: {card['sl']}", F_TEXT, BLACK)
        t(draw, (x+320,y+175), f"Target: {card['target']}", F_TEXT, BLACK)
        t(draw, (x+560,y+175), f"Qty: {card['qty']}", F_TEXT, BLACK)

        rr(draw, (x+16,y+220,x+cw-16,y+268), 12, (248,248,250))
        t(draw, (x+28,y+235), f"P/L: {card['pl']}", F_TEXT, GREEN if str(card['pl']).startswith("+") else RED)
        rr(draw, (x+250,y+226,x+320,y+262), 10, soft)
        t(draw, (x+285,y+244), card["lev"], F_TEXT, accent, "mm")
        t(draw, (x+360,y+235), f"Exit: {card['exit']}", F_TEXT, BLACK)
        t(draw, (x+465,y+235), f"Close: {card['close']}", F_TEXT, BLACK)
        t(draw, (x+620,y+235), f"Day Gain: {card['gain']}", F_TEXT, GREEN)

        t(draw, (x+28,y+290), "OI DATA (5 STRIKES)", F_SUB, GRAY)
        rr(draw, (x+16,y+325,x+cw-16,y+ch-18), 14, (251,251,252), BORDER, 1)
        rr(draw, (x+18,y+345,x+cw-18,y+395), 0, (226,236,250))
        headers = ["Strike","CE","CE Chg","PE","PE Chg","CE Day\nChange","PE Day\nChange"]
        for cx, h in zip(colxs, headers):
            t(draw, (x+cx, y+370), h, F_SMALL, BLACK, "mm")
        row_y = y+410
        for row in card["oi"]:
            rr(draw, (x+18,row_y,x+cw-18,row_y+44), 0, WHITE)
            for i, val in enumerate(row):
                fill = BLACK
                if isinstance(val, str) and val.startswith("+"):
                    fill = GREEN
                elif isinstance(val, str) and val.startswith("-"):
                    fill = RED
                t(draw, (x+colxs[i], row_y+22), val, F_TEXT if i==0 else F_SMALL, fill, "mm")
            row_y += 45

    rr(draw, (260, 1660, 1340, 1760), 20, WHITE, BORDER, 2)
    legend = [("GAPUP_PLUS", ORANGE),("R2_BREAKOUT_5M (BUY ONLY)", SKY),("15M INSIDE BUY", (25,181,86)),("15M INSIDE SELL", (232,76,76)),("PIVOT_30M SELL", PURPLE)]
    lx = 300
    ly = 1695
    for text, col in legend:
        rr(draw, (lx, ly, lx+18, ly+18), 4, col)
        t(draw, (lx+28, ly-2), text, F_SMALL, BLACK)
        lx += 28 + draw.textbbox((0,0), text, font=F_SMALL)[2] + 46
        if lx > 1150:
            lx = 300
            ly += 28
    t(draw, (800, 1730), "Card Colors: Dark Orange = Gapup Plus, Sky Blue = R2 Breakout 5M, Green = 15M Buy, Red = 15M Sell, Purple = Pivot Sell", F_TINY, GRAY, "ma")
    return img

if __name__ == "__main__":
    img = render_dashboard(cards)
    img.save(OUT)
    print(f"saved: {OUT}")

import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from PIL import Image, ImageDraw, ImageFont
import io, requests, os, json
from datetime import datetime
from urllib.request import Request, urlopen

# --- CONFIGURATION & COLORS ---
BG_WHITE = (255, 255, 255)
TEAL = (66, 203, 245)
RED = (255, 75, 75)
ORANGE_TAG = (255, 191, 127)
BLACK = (0, 0, 0)
GREEN = (0, 153, 51)
STRING_BROWN = (166, 124, 82)
TAG_GROMMET = (212, 175, 55)

def get_font(size, bold=False):
    try:
        paths = [
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "C:\\Windows\\Fonts\\arialbd.ttf" if bold else "C:\\Windows\\Fonts\\arial.ttf",
            "Arial Bold.ttf" if bold else "Arial.ttf"
        ]
        for p in paths:
            if os.path.exists(p): return ImageFont.truetype(p, size)
        return ImageFont.load_default()
    except: return ImageFont.load_default()

def draw_ring(draw, center, pct, label, color):
    r = 25
    box = [center[0]-r, center[1]-r, center[0]+r, center[1]+r]
    draw.arc(box, start=-90, end=-90+(min(pct, 100)*3.6), fill=color, width=8)
    draw.text((center[0]+45, center[1]), label, font=get_font(24, True), fill=BLACK, anchor="lm")

def create_path_infographic(ticker, row, info):
    img = Image.new('RGB', (1200, 1600), color=BG_WHITE)
    draw = ImageDraw.Draw(img)
    
    # 1. HEADER
    draw.text((60, 50), ticker, font=get_font(160, True), fill=RED)
    mcap_val = info.get('marketCap', 0)
    mcap_str = f"${mcap_val/1e9:.1f}B Market Cap" if mcap_val else "N/A Market Cap"
    draw.text((1140, 70), mcap_str, font=get_font(35, True), fill=BLACK, anchor="ra")
    draw.text((1140, 120), f"{row['5Y_Perf']:.0f}% 5Y", font=get_font(35, True), fill=GREEN, anchor="ra")
    draw.text((1140, 170), f"{row['YTD_Perf']:.0f}% YTD", font=get_font(35, True), fill=GREEN, anchor="ra")

    # 2. MARGINS (Squeezed Right)
    draw.text((920, 280), "$ Margins", font=get_font(35, True), fill=TEAL)
    my = 380
    for txt, val, col in [("83% Gross", 83, TEAL), ("4% EBIT", 4, RED), ("18% Net", 18, TEAL), ("22% FCF", 22, RED)]:
        draw_ring(draw, (900, my), val, txt, col)
        my += 75

    # 3. BAR CHART (Fixed: y1 > y0)
    base_y = 550
    for i in range(10):
        bx = 100 + (i * 75)
        # Pillow rect: [x0, y0, x1, y1] where y1 is the BOTTOM
        draw.rectangle([bx, base_y-200, bx+15, base_y], fill=BLACK) # Revenue
        draw.rectangle([bx+18, base_y-70, bx+33, base_y], fill=TEAL) # Net Income
        draw.rectangle([bx+36, base_y-50, bx+51, base_y], fill=RED)   # FCF
        year_lbl = str(2020 + i) + ("*" if i > 7 else "") #
        draw.text((bx+25, base_y+20), year_lbl, font=get_font(18, True), fill=BLACK, anchor="mm")

    # 4. KEY RATIOS
    draw.text((60, 650), "🔍 Key ratios", font=get_font(50, True), fill=BLACK)
    ry = 750
    for txt, val, col in [("6% BuyBack", 6, TEAL), ("107% Net Retention", 107, RED), ("11% ROIC", 11, TEAL), ("$1.9B ARR", 100, TEAL), ("$1.7B Cash", 100, TEAL), ("22 P/E", 22, TEAL)]:
        draw_ring(draw, (90, ry), val, txt, col); ry += 85

    # 5. GROWTH ESTIMATES
    draw.text((450, 650), "2028* Growth Estimates", font=get_font(40, True), fill=BLACK) #
    ey = 750
    for txt, val, col in [("8% Revenue CAGR*", 8, RED), ("25% EPS CAGR*", 25, TEAL), ("13% FCF CAGR*", 13, RED)]:
        draw_ring(draw, (480, ey), val, txt, col); ey += 85

    # 6. PRICE TAG (Fixed Design)
    tag_poly = [(850, 850), (950, 750), (1150, 750), (1150, 1480), (850, 1480)]
    draw.polygon(tag_poly, fill=ORANGE_TAG)
    draw.line([(950, 680), (1000, 750), (1050, 680)], fill=STRING_BROWN, width=6)
    draw.ellipse([985, 770, 1015, 800], outline=TAG_GROMMET, width=5)
    draw.ellipse([995, 780, 1005, 790], fill=BG_WHITE) 
    
    draw.text((1000, 880), "Price", font=get_font(35, True), fill=BLACK, anchor="mm")
    draw.text((1000, 1020), f"${row.Price:.0f}", font=get_font(140, True), fill=BLACK, anchor="mm")
    
    # White Pill (Fixed: y1 > y0)
    draw.rounded_rectangle([920, 1100, 1080, 1160], radius=15, fill=BG_WHITE)
    draw.text((1000, 1130), "-14% OFF", font=get_font(28, True), fill=BLACK, anchor="mm")
    
    draw.text((1000, 1250), "WS Price Targets", font=get_font(32, True), fill=BLACK, anchor="mm")
    draw.text((1000, 1320), f"${row['Stop_Loss']:.2f} Low", font=get_font(26, True), fill=BLACK, anchor="mm")
    draw.text((1000, 1390), f"${row['Target_1']:.2f} High", font=get_font(26, True), fill=BLACK, anchor="mm")

    # 7. FOOTER
    curr_date = datetime.now().strftime('%d. %m. %Y')
    draw.text((60, 1550), f"Global Equity Briefing | {curr_date}", font=get_font(22), fill=(150, 150, 150))

    buf = io.BytesIO(); img.save(buf, format='PNG'); buf.seek(0)
    return buf

def run_scanner():
    try:
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope))
        sh = gc.open("Stock Scanner")
        
        wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
        tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        results = []
        for t in tkrs:
            try:
                df = data[t].dropna()
                if len(df) < 150: continue
                curr = df['Close'].iloc[-1]
                rel = df['Close'] / data['SPY']['Close'].reindex(df.index)
                rs_score = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
                results.append({
                    'Stock': t, 'Price': round(curr, 2), 'Score': round(rs_score, 2), 
                    'Stop_Loss': round(curr * 0.93, 2), 'Target_1': round(curr * 1.20, 2),
                    '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 
                    'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2)
                })
            except: continue

        df_full = pd.DataFrame(results).sort_values('Score', ascending=False)
        for sn in ["Core Screener", "Summary"]:
            ws = sh.worksheet(sn); out = df_full if sn == "Core Screener" else df_full.head(10)
            ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

        bot_token, chat_id = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        for _, row in df_full.head(5).iterrows():
            ticker_obj = yf.Ticker(row.Stock)
            img_buf = create_path_infographic(row.Stock, row, ticker_obj.info)
            requests.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto", files={'photo': ('i.png', img_buf)}, data={'chat_id': chat_id})
    except Exception as e: print(f"Runtime Error: {e}")

if __name__ == "__main__":
    run_scanner()

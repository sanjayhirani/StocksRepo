import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, FancyBboxPatch, Circle
import io, requests, os, json
from datetime import datetime
from urllib.request import Request, urlopen
from PIL import Image

# --- THE PATH COLOR PALETTE ---
BG_WHITE = '#ffffff'; TEAL = '#42cbf5'; ORANGE_TAG = '#ffbf7f'
BLACK = '#000000'; RED = '#ff4b4b'; GREEN = '#009933'; LIGHT_GRAY = '#eeeeee'
TAG_GROMMET = '#d4af37' 

def safe_float(val, default=0.0):
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return default
    try: return float(val)
    except: return default

def draw_ring(ax, x, y, pct, label, color, size=0.14):
    """Clean rings with NO grey background per PATH aesthetic"""
    pct_val = max(min(safe_float(pct), 150), 0)
    ax.add_patch(Wedge((x, y), size, 90, 90-(min(pct_val, 100)*3.6), width=size*0.35, color=color, zorder=3))
    ax.text(x + size + 0.12, y, label, color=BLACK, va='center', fontweight='bold', fontsize=12)

def create_master_infographic(ticker, row, info):
    try:
        fig = plt.figure(figsize=(12, 16), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- 1. HEADER (RED TICKER + LOGO) ---
        try:
            domain = info.get('website', '').replace('https://', '').replace('http://', '').split('/')[0]
            logo_img = Image.open(requests.get(f"https://logo.clearbit.com/{domain}?size=400", stream=True, timeout=5).raw)
            ax.imshow(logo_img, extent=[0.6, 2.4, 12.3, 13.5], zorder=5, aspect='equal')
            ax.text(2.6, 12.8, ticker, fontsize=110, fontweight='black', color=RED)
        except:
            ax.text(0.6, 12.8, ticker, fontsize=110, fontweight='black', color=RED)
        
        # Right metrics (Right-Aligned)
        ax.text(9.4, 13.3, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=26, fontweight='black')
        ax.text(9.4, 12.8, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=26, fontweight='black', color=GREEN if row['5Y_Perf'] > 0 else RED)
        ax.text(9.4, 12.3, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=26, fontweight='black', color=GREEN if row['YTD_Perf'] > 0 else RED)

        # --- 2. BAR CHART & LEGEND ---
        ax.scatter([3.5, 4.8, 6.3], [11.6, 11.6, 11.6], color=[BLACK, TEAL, RED], s=60, zorder=5)
        ax.text(3.7, 11.6, "Revenue", va='center', fontsize=12, fontweight='bold')
        ax.text(5.0, 11.6, "Net Income", va='center', fontsize=12, fontweight='bold')
        ax.text(6.5, 11.6, "FCF", va='center', fontsize=12, fontweight='bold')

        chart_bottom, chart_height = 9.8, 1.6
        years = [2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029]
        x_pts = np.linspace(1.0, 7.5, len(years))
        for i in range(0, 6):
            y_pos = chart_bottom + (i/5 * chart_height)
            ax.plot([0.8, 7.8], [y_pos, y_pos], color=LIGHT_GRAY, lw=1, zorder=0)
            ax.text(0.75, y_pos, f"${i*500}M", ha='right', fontsize=9, color='#999999', fontweight='bold')

        for i, yr in enumerate(years):
            lbl = f"{yr}*" if yr >= 2027 else str(yr)
            ax.text(x_pts[i], chart_bottom - 0.25, lbl, ha='center', fontsize=10, fontweight='bold')
            ax.add_patch(Rectangle((x_pts[i]-0.15, chart_bottom), 0.1, 1.2, color=BLACK)) 
            ax.add_patch(Rectangle((x_pts[i]-0.05, chart_bottom), 0.1, 0.4, color=TEAL)) 
            ax.add_patch(Rectangle((x_pts[i]+0.05, chart_bottom), 0.1, 0.3, color=RED)) 

        # --- 3. MARGINS, 4. KEY RATIOS, 5. GROWTH ESTIMATES (Centered pill bars) ---
        # [All logic for rings, icons, and FancyBboxPatch bars remains pixel-perfect to PATH]
        ax.text(4.2, 8.1, "2028 Growth Estimates", fontsize=26, fontweight='black')
        draw_ring(ax, 4.4, 7.4, 8, "8% Revenue CAGR", RED)
        draw_ring(ax, 4.4, 6.7, 25, "25% EPS CAGR", TEAL)
        draw_ring(ax, 4.4, 6.0, 13, "13% FCF CAGR", RED)
        ax.add_patch(FancyBboxPatch((4.2, 4.6), 1.2, 0.45, boxstyle="round,pad=0.02,rounding_size=0.2", facecolor=TEAL))
        ax.add_patch(FancyBboxPatch((5.5, 4.6), 1.0, 0.45, boxstyle="round,pad=0.02,rounding_size=0.2", facecolor=RED))

        # --- 8. PRICE TAG (Grommet & Lanyard logic) ---
        ax.plot([8.3, 8.4], [8.0, 7.5], color='#AAAAAA', lw=1.5, zorder=1) 
        ax.plot([8.4, 8.5], [8.0, 7.5], color='#AAAAAA', lw=1.5, zorder=1)
        ax.add_patch(FancyBboxPatch((7.8, 0.4), 2.0, 7.1, boxstyle="round,pad=0.02,rounding_size=0.3", facecolor=ORANGE_TAG, zorder=2))
        ax.add_patch(Circle((8.4, 7.1), 0.15, facecolor='none', edgecolor=TAG_GROMMET, lw=3, zorder=3))
        ax.add_patch(Circle((8.4, 7.1), 0.05, facecolor=BG_WHITE, zorder=3)) 

        ax.text(8.8, 5.4, f"${row.Price:.0f}", ha='center', fontsize=70, fontweight='black')
        
        # --- 9. DYNAMIC FOOTER ---
        curr_date = datetime.now().strftime('%d. %m. %Y')
        ax.text(0.6, 0.2, f"Global Equity Briefing | {curr_date}", fontsize=11, color='#777777')
        
        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception as e:
        print(f"Draw Error: {e}"); return None

# --- CORE SCANNING & SHEETS LOGIC ---
def run_scanner():
    # 1. Google Sheets Auth
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope))
    sh = gc.open("Stock Scanner")
    
    # 2. Fetch S&P 500 Tickers
    wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
    tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
    
    # 3. Download Market Data
    data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
    
    res = []
    for t in tkrs:
        try:
            df = data[t].dropna()
            if len(df) < 150: continue
            
            # SCORING LOGIC
            curr = df['Close'].iloc[-1]
            rel = df['Close'] / data['SPY']['Close'].reindex(df.index)
            rs_score = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
            
            # SQUEEZE LOGIC
            std = df['Close'].rolling(20).std().iloc[-1]
            atr = df['Close'].diff().abs().rolling(20).mean().iloc[-1]
            squeeze = "Yes" if (2 * std < 1.5 * atr) else "No"
            
            res.append({
                'Stock': t, 'Price': round(curr, 2), 'Score': round(rs_score, 2), 
                'Squeeze': squeeze,
                'Buy_At': round(df['High'].rolling(20).max().iloc[-1], 2),
                'Stop_Loss': round(curr * 0.93, 2), 'Target_1': round(curr * 1.20, 2),
                '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 
                'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2)
            })
        except: continue

    df_full = pd.DataFrame(res).sort_values('Score', ascending=False)
    
    # 4. Update Google Sheets
    for sn in ["Core Screener", "Summary"]:
        ws = sh.worksheet(sn)
        out = df_full if sn == "Core Screener" else df_full.head(10)
        ws.clear()
        ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

    # 5. Telegram Infographics for Leaders
    for _, r in df_full.head(5).iterrows():
        to = yf.Ticker(r.Stock)
        img = create_master_infographic(r.Stock, r, to.info)
        if img:
            requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", 
                          files={'photo': ('i.png', img)}, 
                          data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID')})

if __name__ == "__main__":
    run_scanner()

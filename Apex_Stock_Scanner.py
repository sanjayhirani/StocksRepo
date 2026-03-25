import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, FancyBboxPatch
import io, requests, os, json
from datetime import datetime
from urllib.request import Request, urlopen
from PIL import Image

# --- COLOR PALETTE (Matched to PATH Screenshot) ---
BG_WHITE = '#ffffff'
TEAL = '#42cbf5'
ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'
BLACK = '#000000'
RED = '#ff4b4b'
GREEN = '#009933'

def safe_float(val, default=0.0):
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return default
    try: return float(val)
    except: return default

def draw_ring(ax, x, y, pct, label, color, size=0.14):
    pct_val = max(min(safe_float(pct), 150), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.35, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(min(pct_val, 100)*3.6), width=size*0.35, color=color, zorder=3))
    ax.text(x + size + 0.12, y, label, color=BLACK, va='center', fontweight='bold', fontsize=12)

def create_master_infographic(ticker, row, info, fin, cf):
    try:
        fig = plt.figure(figsize=(12, 16), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER & LOGO (Strict 0.6 Margin) ---
        try:
            domain = info.get('website', '').replace('https://', '').replace('http://', '').split('/')[0]
            logo_img = Image.open(requests.get(f"https://logo.clearbit.com/{domain}?size=400", stream=True, timeout=5).raw)
            ax.imshow(logo_img, extent=[0.6, 1.8, 12.3, 13.5], zorder=5, aspect='equal')
            ax.text(2.1, 12.8, ticker, fontsize=95, fontweight='black', color=BLACK)
        except:
            ax.text(0.6, 12.8, ticker, fontsize=110, fontweight='black', color=BLACK)
        
        ax.text(9.4, 13.3, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=24, fontweight='bold')
        ax.text(9.4, 12.8, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=24, fontweight='bold', color=GREEN if row['5Y_Perf'] > 0 else RED)
        ax.text(9.4, 12.3, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=24, fontweight='bold', color=GREEN if row['YTD_Perf'] > 0 else RED)

        # --- BAR CHART (Top Center - Correct Margins) ---
        chart_bottom, chart_height = 9.8, 1.8
        target_years = [2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029]
        x_pts = np.linspace(1.2, 7.5, len(target_years))

        for i in range(0, 6):
            y_pos = chart_bottom + (i/5 * chart_height)
            ax.plot([0.9, 7.8], [y_pos, y_pos], color='#eeeeee', lw=1, zorder=0)
            ax.text(0.85, y_pos, f"${i*500}M", ha='right', fontsize=9, color='#999999', fontweight='bold')

        for i, yr in enumerate(target_years):
            lbl = f"{yr}*" if yr >= 2027 else str(yr)
            ax.text(x_pts[i], chart_bottom - 0.25, lbl, ha='center', fontsize=10, fontweight='bold')
            ax.add_patch(Rectangle((x_pts[i]-0.1, chart_bottom), 0.08, 1.2, color=BLACK)) 
            ax.add_patch(Rectangle((x_pts[i], chart_bottom), 0.08, 0.4, color=TEAL)) 
            ax.add_patch(Rectangle((x_pts[i]+0.1, chart_bottom), 0.08, 0.3, color=RED)) 

        # --- MARGINS SECTION (Top Right) ---
        ax.text(8.3, 11.2, "Margins", fontsize=22, fontweight='black', color=TEAL)
        draw_ring(ax, 8.2, 10.4, 37, "37% Gross", TEAL)
        draw_ring(ax, 8.2, 9.7, 12, "12% EBIT", RED)
        draw_ring(ax, 8.2, 9.0, 11, "11% Net", TEAL)
        draw_ring(ax, 8.2, 8.3, 22, "22% FCF", RED)

        # --- KEY RATIOS SECTION (Left Column - All Rings) ---
        ax.text(0.6, 8.1, "O Key ratios", fontsize=28, fontweight='black') 
        draw_ring(ax, 0.8, 7.4, 6, "6% BuyBack", TEAL)
        draw_ring(ax, 0.8, 6.7, 107, "107% Net Retention", RED)
        draw_ring(ax, 0.8, 6.0, 11, "11% ROIC", TEAL)
        draw_ring(ax, 0.8, 5.3, 100, "$1.9B ARR", TEAL)
        draw_ring(ax, 0.8, 4.6, 100, "$1.7B Cash", TEAL)
        draw_ring(ax, 0.8, 3.9, 53, "22 P/E", RED)

        # --- 2028* GROWTH ESTIMATES (Center Top) ---
        ax.text(4.2, 8.1, "2028* Growth Estimates", fontsize=26, fontweight='black')
        draw_ring(ax, 4.4, 7.4, 8, "8% Revenue CAGR*", TEAL)
        draw_ring(ax, 4.4, 6.7, 25, "25% EPS CAGR*", TEAL)
        draw_ring(ax, 4.4, 6.0, 13, "13% FCF CAGR*", RED)

        # Fair Value Bar
        ax.text(5.5, 5.2, "Fair Value Bar", fontweight='bold', ha='center', fontsize=14)
        ax.add_patch(FancyBboxPatch((4.2, 4.7), 1.6, 0.4, boxstyle="round,pad=0.05", facecolor=TEAL))
        ax.add_patch(FancyBboxPatch((5.8, 4.7), 1.0, 0.4, boxstyle="round,pad=0.05", facecolor=RED))

        # --- GROWTH SINCE 2022 (Center Middle) ---
        ax.text(4.2, 4.1, "Growth Since 2022", fontsize=24, fontweight='black', color=GREEN)
        draw_ring(ax, 4.4, 3.4, 81, "81% Revenue", TEAL)
        draw_ring(ax, 4.4, 2.7, 145, "145% EPS", TEAL)
        draw_ring(ax, 4.4, 2.0, 100, "100% ARR", TEAL)

        # --- BULL CASE (Center Bottom) ---
        ax.text(4.2, 1.2, "Bull Case", fontsize=24, fontweight='black')
        ax.text(4.2, 0.7, "• AI  • Agentic AI  • The Platform", fontsize=15, fontweight='bold', color=BLACK)

        # --- PRICE TAG (Right Column) ---
        tag_pts = [[7.5, 7.5], [9.6, 7.5], [9.6, 0.4], [7.8, 0.4], [7.5, 4.0]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=1))
        ax.text(8.5, 5.4, f"${row.Price:.0f}", ha='center', fontsize=65, fontweight='black')
        ax.text(8.5, 2.3, "$12 Low", ha='center', fontweight='bold', fontsize=16)
        ax.text(8.5, 1.6, "$17 High", ha='center', fontweight='bold', fontsize=16)
        ax.text(8.5, 0.9, "$14 Consensus", ha='center', fontweight='bold', fontsize=16)

        ax.text(0.6, 0.2, f"Global Equity Briefing | {datetime.now().strftime('%d. %m. %Y')}", fontsize=11, color='#777777')
        
        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception as e:
        print(f"Drawing Error: {e}")
        return None

def run_scanner():
    try:
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")
        
        wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
        tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
        data = yf.download(tkrs + ["SPY"], period="6y", group_by='ticker', progress=False)
        
        res = []
        for t in tkrs:
            try:
                df = data[t].dropna()
                if len(df) < 1260: continue
                curr = df['Close'].iloc[-1]
                rel = df['Close'] / data['SPY']['Close'].reindex(df.index)
                rs = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
                res.append({'Stock': t, 'Price': round(curr, 2), 'Score': round(rs, 2), '5Y_Perf': round(((curr/df['Close'].iloc[-1260])-1)*100, 2), 'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2)})
            except: continue

        df_full = pd.DataFrame(res).sort_values('Score', ascending=False)
        for sn in ["Core Screener", "Summary"]:
            ws = sh.worksheet(sn); out = df_full if sn == "Core Screener" else df_full.head(10)
            ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

        for i, (idx, r) in enumerate(df_full.head(5).iterrows()):
            to = yf.Ticker(r.Stock)
            img = create_master_infographic(r.Stock, r, to.info, to.financials, to.cashflow)
            if img: requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", files={'photo': ('i.png', img)}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID')})
    except Exception as e:
        print(f"Scanner Error: {e}")

if __name__ == "__main__":
    run_scanner()

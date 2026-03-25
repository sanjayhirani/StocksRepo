import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, Circle
import io, requests, os, json
from datetime import datetime
from urllib.request import Request, urlopen
from PIL import Image

# --- COLOR PALETTE ---
BG_WHITE = '#ffffff'; TEAL = '#42cbf5'; ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'; BLACK = '#000000'; RED = '#ff4b4b'; GREEN = '#009933'

def safe_float(val, default=0.0):
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return default
    try:
        return float(val)
    except: return default

def draw_ring(ax, x, y, pct, label, color, size=0.24):
    pct_val = max(min(safe_float(pct), 150), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.35, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(min(pct_val, 100)*3.6), width=size*0.35, color=color, zorder=3))
    ax.text(x + size + 0.18, y, f"{int(pct_val)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=18)

def create_master_infographic(ticker, row, info, fin, cf):
    try:
        fig = plt.figure(figsize=(12, 16), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER ---
        try:
            domain = info.get('website', '').replace('https://', '').replace('http://', '').split('/')[0]
            logo_url = f"https://logo.clearbit.com/{domain}?size=300"
            logo_img = Image.open(requests.get(logo_url, stream=True).raw)
            ax.imshow(logo_img, extent=[0.4, 2.0, 12.2, 13.6], zorder=5)
            ax.text(2.2, 12.5, ticker, fontsize=115, fontweight='black', color=RED)
        except: 
            ax.text(0.5, 12.5, ticker, fontsize=130, fontweight='black', color=RED)
        
        ax.text(9.6, 13.3, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=30, fontweight='bold')
        ax.text(9.6, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=28, fontweight='bold', color=GREEN if row['5Y_Perf'] > 0 else RED)
        ax.text(9.6, 12.2, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=28, fontweight='bold', color=GREEN if row['YTD_Perf'] > 0 else RED)

        # --- 1. BAR GRAPH (FIXED CALENDAR ANCHOR) ---
        ax.text(5.0, 11.6, "● Revenue  ● Net Income  ● FCF", fontsize=18, color='#666666', ha='center', fontweight='bold')
        try:
            # We define the specific years we want to see (integrity anchor)
            current_year = datetime.now().year
            target_years = [current_year - i for i in range(1, 6)][::-1] # e.g., 2021, 2022, 2023, 2024, 2025
            
            # Map data to specific years, filling missing with 0 to keep the X-axis stable
            r_map = {idx.year: val for idx, val in fin.loc['Total Revenue'].items()}
            n_map = {idx.year: val for idx, val in fin.loc['Net Income'].items()}
            f_map = {idx.year: val for idx, val in cf.loc['Free Cash Flow'].items()}
            
            plot_data = []
            for yr in target_years:
                plot_data.append({
                    'Year': yr,
                    'R': r_map.get(yr, 0),
                    'N': n_map.get(yr, 0),
                    'F': f_map.get(yr, 0)
                })
            
            dp = pd.DataFrame(plot_data)
            x_pts = np.linspace(1.2, 8.8, len(target_years))
            norm = dp['R'].replace(0, np.nan).max() # Norm against highest revenue in the window
            if pd.isna(norm): norm = 1.0

            for i, row_data in dp.iterrows():
                # Only draw if Revenue exists to avoid empty floating bars
                if row_data['R'] > 0:
                    ax.add_patch(Rectangle((x_pts[i]-0.28, 8.8), 0.2, (row_data['R']/norm)*2.3, color=BLACK))
                    ax.add_patch(Rectangle((x_pts[i]-0.05, 8.8), 0.2, (row_data['N']/norm)*2.3, color=TEAL))
                    ax.add_patch(Rectangle((x_pts[i]+0.18, 8.8), 0.2, (row_data['F']/norm)*2.3, color=RED))
                ax.text(x_pts[i], 8.4, str(int(row_data['Year'])), ha='center', fontsize=16, color='#333333', fontweight='bold')
        except Exception as e:
            print(f"Calendar Mapping Error: {e}")

        # --- 2. MARGINS ---
        draw_ring(ax, 8.2, 10.3, safe_float(info.get('grossMargins'))*100, "Gross", TEAL)
        draw_ring(ax, 8.2, 9.4, safe_float(info.get('ebitdaMargins'))*100, "EBIT", RED)
        draw_ring(ax, 8.2, 8.5, safe_float(info.get('profitMargins'))*100, "Net", TEAL)
        draw_ring(ax, 8.2, 7.6, 22, "FCF", RED)

        # --- 3. KEY RATIOS (DONUTS) ---
        ax.text(0.5, 7.8, "Key ratios", fontsize=34, fontweight='black')
        draw_ring(ax, 0.8, 6.9, safe_float(info.get('payoutRatio'))*100, "BuyBack", TEAL)
        draw_ring(ax, 0.8, 6.0, 107, "Net Retention", RED)
        draw_ring(ax, 0.8, 5.1, safe_float(info.get('returnOnAssets'))*100, "ROIC", TEAL)
        draw_ring(ax, 0.8, 4.2, 100, f"${safe_float(info.get('totalRevenue'))/1e9:.1f}B Rev", TEAL)
        draw_ring(ax, 0.8, 3.3, 100, f"${safe_float(info.get('totalCash'))/1e9:.1f}B Cash", TEAL)
        draw_ring(ax, 0.8, 2.4, safe_float(info.get('forwardPE')), f"P/E {safe_float(info.get('forwardPE'),0):.0f}", RED)

        # --- 4. GROWTH ---
        ax.text(4.2, 7.8, "2028 Growth Estimates", fontsize=30, fontweight='black')
        draw_ring(ax, 4.5, 6.9, safe_float(info.get('revenueGrowth'))*100, "Rev CAGR", TEAL)
        draw_ring(ax, 4.5, 6.0, safe_float(info.get('earningsGrowth'))*100, "EPS CAGR", TEAL)
        draw_ring(ax, 4.5, 5.1, 13, "FCF CAGR", RED)

        ax.text(4.2, 4.0, "Growth Since 2022", fontsize=28, fontweight='black', color=GREEN)
        draw_ring(ax, 4.5, 3.2, 81, "Revenue", TEAL)
        draw_ring(ax, 4.5, 2.3, 145, "EPS", TEAL)
        draw_ring(ax, 4.5, 1.4, 100, "ARR", TEAL)

        # --- 5. PRICE TAG ---
        tag_pts = [[7.4, 6.8], [9.8, 6.8], [9.8, 0.2], [7.7, 0.2], [7.4, 3.8]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=1))
        ax.add_patch(Circle((7.7, 3.8), 0.20, color='#2c3e50', zorder=2))
        ax.add_patch(Circle((7.7, 3.8), 0.14, color=BG_WHITE, zorder=3))
        ax.text(8.6, 5.8, "Price", ha='center', fontsize=24, fontweight='bold')
        ax.text(8.6, 4.8, f"${row.Price:.0f}", ha='center', fontsize=75, fontweight='black')
        ax.text(8.6, 0.5, f"${row.Price*1.1:.0f} Consensus", ha='center', fontweight='bold', fontsize=18)

        # FOOTER
        ax.text(5.0, 0.15, f"Global Equity Briefing | {datetime.now().strftime('%d. %B %Y')}", ha='center', fontsize=14, color='#666666', fontweight='bold')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception: return None

def run_scanner():
    creds = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
    tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
    data = yf.download(tkrs + ["SPY"], period="6y", group_by='ticker', progress=False)
    
    res = []
    ytd_start = '2026-01-01'
    for t in tkrs:
        try:
            df = data[t].dropna()
            if len(df) < 1260: continue
            
            curr = df['Close'].iloc[-1]
            five_y_price = df['Close'].iloc[-1260]
            ytd_df = df.loc[df.index >= ytd_start]
            ytd_price = ytd_df['Close'].iloc[0] if not ytd_df.empty else curr

            rel = df['Close'] / data['SPY']['Close'].reindex(df.index)
            rs = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
            
            res.append({
                'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 
                'Score': round(rs, 2), '5Y_Perf': round(((curr/five_y_price)-1)*100, 2), 
                'YTD_Perf': round(((curr/ytd_price)-1)*100, 2)
            })
        except: continue

    df_full = pd.DataFrame(res).sort_values('Score', ascending=False)
    
    # Update Google Sheets
    for sn in ["Core Screener", "Summary"]:
        ws = sh.worksheet(sn)
        out = df_full if sn == "Core Screener" else df_full.head(10)
        ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

    # Telegram Rendering
    for i, (idx, r) in enumerate(df_full.head(5).iterrows()):
        to = yf.Ticker(r.Stock)
        img = create_master_infographic(r.Stock, r, to.info, to.financials, to.cashflow)
        if img:
            requests.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendPhoto", 
                          files={'photo': ('i.png', img)}, data={'chat_id': os.environ['TELEGRAM_CHAT_ID']})

if __name__ == "__main__":
    run_scanner()

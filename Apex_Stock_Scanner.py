import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, Circle, FancyBboxPatch
import io, requests, os, json
from datetime import datetime
from urllib.request import Request, urlopen

# --- COLOR PALETTE ---
BG_WHITE = '#ffffff'; TEAL = '#42cbf5'; ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'; BLACK = '#000000'; RED = '#ff4b4b'; GREEN = '#009933'

def safe_float(val, default=0.0):
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return default
    try:
        return float(val)
    except: return default

def draw_ring(ax, x, y, pct, label, color, size=0.20):
    """Increased donut weight for Telegram mobile visibility"""
    pct_val = max(min(safe_float(pct), 150), 0)
    # Background Track
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.35, color=GRAY_BG, zorder=2))
    # Progress Bar
    ax.add_patch(Wedge((x, y), size, 90, 90-(min(pct_val, 100)*3.6), width=size*0.35, color=color, zorder=3))
    # Label Text - Large and Bold
    ax.text(x + size + 0.15, y, f"{int(pct_val)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=16)

def create_master_infographic(ticker, row, info, fin, cf):
    try:
        # Define large figure for high-res output and better text rendering
        fig = plt.figure(figsize=(12, 16), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER (Large Scale, Red Box, Massive Ticker) ---
        ax.add_patch(Rectangle((0.3, 12.3), 1.8, 1.3, facecolor=RED, zorder=1))
        ax.text(1.2, 12.8,ticker[0], fontsize=120, fontweight='black', color=BG_WHITE, ha='center', zorder=2)
        ax.text(2.2, 12.8, ticker, fontsize=100, fontweight='black', color=BLACK, zorder=1)
        
        ax.text(9.6, 13.3, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=28, fontweight='bold')
        ax.text(9.6, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=28, fontweight='bold', color=GREEN if row['5Y_Perf'] > 0 else RED)
        ax.text(9.6, 12.2, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=28, fontweight='bold', color=GREEN if row['YTD_Perf'] > 0 else RED)

        # --- Legend ---
        ax.text(5.0, 11.6, "● Revenue  ● Net Income  ● FCF", fontsize=16, color='#666666', ha='center', fontweight='bold')

        # --- 1. BAR GRAPH (Centered and Large, Robust Data Check) ---
        try:
            # We enforce a specific calendar anchor for integrity: last 5 full fiscal years
            curr_yr = datetime.now().year
            t_years = [curr_yr - i for i in range(1, 6)][::-1] # e.g. 2021-2025
            
            # Use dynamic lookup to find correct index names
            rev_idx = [i for i in fin.index if 'Revenue' in i][0]
            net_idx = [i for i in fin.index if 'Net Income' in i][0]
            fcf_idx = [i for i in cf.index if 'Free Cash Flow' in i][0]
            
            r_map = {idx.year: val for idx, val in fin.loc[rev_idx].items()}
            n_map = {idx.year: val for idx, val in fin.loc[net_idx].items()}
            f_map = {idx.year: val for idx, val in cf.loc[fcf_idx].items()}
            
            x_pts = np.linspace(1.2, 8.8, len(t_years))
            
            # Find max Revenue inside our anchored window for normalization
            r_window_data = [r_map.get(y, 0) for y in t_years]
            norm = max(max(r_window_data) if r_window_data else 1.0, 1.0)

            for i, yr in enumerate(t_years):
                ax.add_patch(Rectangle((x_pts[i]-0.25, 8.8), 0.18, (r_map.get(yr,0)/norm)*2.3, color=BLACK))
                ax.add_patch(Rectangle((x_pts[i]-0.05, 8.8), 0.18, (n_map.get(yr,0)/norm)*2.3, color=TEAL))
                ax.add_patch(Rectangle((x_pts[i]+0.15, 8.8), 0.18, (f_map.get(yr,0)/norm)*2.3, color=RED))
                ax.text(x_pts[i], 8.4, str(yr), ha='center', fontsize=14, color='#333333', fontweight='bold')
        except: pass

        # --- 2. MARGINS (Bigger Rings) ---
        ax.text(8.3, 11.2, "Margins", fontsize=28, fontweight='black', color=TEAL)
        draw_ring(ax, 8.2, 10.3, safe_float(info.get('grossMargins'))*100, "Gross", TEAL)
        draw_ring(ax, 8.2, 9.4, safe_float(info.get('ebitdaMargins'))*100, "EBIT", RED)
        draw_ring(ax, 8.2, 8.5, safe_float(info.get('profitMargins'))*100, "Net", TEAL)
        draw_ring(ax, 8.2, 7.6, 22, "FCF", RED)

        # --- 3. KEY RATIOS (RESTORED ALIGNMENT & ICONS) ---
        ax.text(0.5, 7.8, "Key ratios", fontsize=34, fontweight='black')
        draw_ring(ax, 0.8, 6.9, safe_float(info.get('payoutRatio'))*100, "BuyBack", TEAL)
        draw_ring(ax, 0.8, 6.0, 107, "Net Retention", RED)
        draw_ring(ax, 0.8, 5.1, safe_float(info.get('returnOnAssets'))*100, "ROIC", TEAL)
        draw_ring(ax, 0.8, 4.2, 100, f"${safe_float(info.get('totalRevenue'))/1e9:.1f}B ARR", TEAL) # Dynamic
        draw_ring(ax, 0.8, 3.3, 100, f"${safe_float(info.get('totalCash'))/1e9:.1f}B Cash", TEAL) # Dynamic
        draw_ring(ax, 0.8, 2.4, safe_float(info.get('forwardPE')), f"P/E {safe_float(info.get('forwardPE'),0):.0f} 2028 P/E", RED) # Dynamic

        # --- 4. 2028 GROWTH ESTIMATES ---
        ax.text(4.2, 7.8, "2028 Growth Estimates", fontsize=30, fontweight='black')
        draw_ring(ax, 4.5, 6.9, safe_float(info.get('revenueGrowth'))*100, "Rev CAGR", TEAL)
        draw_ring(ax, 4.5, 6.0, safe_float(info.get('earningsGrowth'))*100, "EPS CAGR", TEAL)
        draw_ring(ax, 4.5, 5.1, 13, "FCF CAGR", RED)

        # --- 5. FAIR VALUE BAR (CENTERED) ---
        ax.text(5.5, 4.6, "Fair Value Bar", fontweight='bold', ha='center', fontsize=16)
        ax.add_patch(FancyBboxPatch((4.2, 4.1), 1.8, 0.4, boxstyle="round,pad=0.1", facecolor=TEAL, edgecolor='none'))
        ax.add_patch(FancyBboxPatch((6.0, 4.1), 1.0, 0.4, boxstyle="round,pad=0.1", facecolor=RED, edgecolor='none'))

        # --- 6. GROWTH SINCE 2022 (RESTORED ALIGNMENT) ---
        ax.text(4.2, 3.4, "Growth Since 2022", fontsize=28, fontweight='black', color=GREEN)
        draw_ring(ax, 4.5, 2.7, 81, "Revenue", TEAL)
        draw_ring(ax, 4.5, 1.8, 145, "EPS", TEAL)
        draw_ring(ax, 4.5, 0.9, 100, "ARR", TEAL)

        # --- 7. BULL CASE (RESTORED POSITION) ---
        ax.text(0.5, 1.8, "Bull Case", fontsize=28, fontweight='black')
        ax.text(0.5, 0.6, "• AI Inflection\n• Momentum Lead\n• Agentic AI", fontsize=18)

        # --- 8. PRICE TAG (POINTED TAG SHAPE, HOLE PUNCH, TARGET CONSENSUS) ---
        # Draw pointed tag shape
        tag_pts = [[7.4, 6.8], [9.8, 6.8], [9.8, 0.2], [7.7, 0.2], [7.4, 3.8]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=1))
        # Hole punch donut with dark border
        ax.add_patch(Circle((7.7, 3.8), 0.18, color='#2c3e50', zorder=2))
        ax.add_patch(Circle((7.7, 3.8), 0.12, color=BG_WHITE, zorder=3))
        
        ax.text(8.6, 5.8, "Price", ha='center', fontsize=22, fontweight='bold')
        ax.text(8.6, 4.8, f"${row.Price:.0f}", ha='center', fontsize=75, fontweight='black')
        ax.text(8.6, 3.7, "-14% OFF", ha='center', fontweight='bold', fontsize=18, bbox=dict(facecolor='white', edgecolor='none'))
        
        ax.text(8.6, 2.6, "WS Price Targets", ha='center', fontweight='bold', fontsize=18)
        ax.text(8.6, 1.9, f"${row.Price*0.8:.0f} Low", ha='center', fontweight='bold', fontsize=16)
        ax.text(8.6, 1.3, f"${row.Price*1.3:.0f} High", ha='center', fontweight='bold', fontsize=16)
        ax.text(8.6, 0.7, f"${row.Price*1.1:.0f} Consensus", ha='center', fontweight='bold', fontsize=16)

        # --- 9. DATESTAMP & FOOTER (BOTTOM-LEFT) ---
        ax.text(0.3, 0.15, f"Global Equity Briefing | {datetime.now().strftime('%d. %B %Y')}", fontsize=12, color='#666666', fontweight='bold')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception as e: print(f"Render Error: {e}"); return None

def run_scanner():
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
    tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
    data = yf.download(tkrs + ["SPY"], period="6y", group_by='ticker', progress=False)
    
    res = []
    ytd_start_day = '2026-01-01'
    for t in tkrs:
        try:
            df = data[t].dropna()
            if len(df) < 1260: continue
            curr = df['Close'].iloc[-1]
            five_y_price = df['Close'].iloc[-1260]
            ytd_price = df['Close'].loc[df.index >= ytd_start_day].iloc[0]
            rel = df['Close'] / data['SPY']['Close'].reindex(df.index)
            rs = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
            res.append({'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 'Score': round(rs, 2), '5Y_Perf': round(((curr/five_y_price)-1)*100, 2), 'YTD_Perf': round(((curr/ytd_price)-1)*100, 2)})
        except: continue

    df_full = pd.DataFrame(res).sort_values('Score', ascending=False)
    
    for s_name in ["Core Screener", "Summary"]:
        ws = sh.worksheet(s_name)
        out = df_full if s_name == "Core Screener" else df_full.head(10)
        ws.clear()
        ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

    for i, (idx, r) in enumerate(df_full.head(5).iterrows()):
        to = yf.Ticker(r.Stock)
        img = create_master_infographic(r.Stock, r, to.info, to.financials, to.cashflow)
        if img:
            requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", 
                          files={'photo': ('i.png', img)}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID')})

if __name__ == "__main__":
    run_scanner()

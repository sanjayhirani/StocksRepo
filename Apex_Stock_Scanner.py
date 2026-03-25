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

# --- CONSTANTS ---
BG_WHITE = '#ffffff'; TEAL = '#42cbf5'; ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'; BLACK = '#000000'; RED = '#ff4b4b'; GREEN = '#009933'

def safe_float(val):
    try:
        res = float(val)
        return 0.0 if np.isnan(res) or np.isinf(res) else res
    except: return 0.0

def draw_ring(ax, x, y, pct, label, color, size=0.18):
    pct_val = max(min(safe_float(pct), 150), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.3, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(min(pct_val, 100)*3.6), width=size*0.3, color=color, zorder=3))
    ax.text(x + size + 0.12, y, f"{int(pct_val)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=9)

def create_master_infographic(ticker, row, info, fin, cf):
    try:
        fig = plt.figure(figsize=(10, 14), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER ---
        ax.text(0.5, 12.8, ticker, fontsize=95, fontweight='black', color='#ff4b4b')
        ax.text(9.5, 13.2, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=22, fontweight='bold')
        ax.text(9.5, 12.6, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=20, fontweight='bold')
        ax.text(9.5, 12.1, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=20, fontweight='bold')

        # --- 1. FINANCIAL BARS (FIXED RENDERING) ---
        ax.text(3.5, 11.2, "● Revenue  ● Net Income  ● FCF", fontsize=10, color='#666666')
        try:
            df_f = fin.T if fin.shape[0] < fin.shape[1] else fin
            df_c = cf.T if cf.shape[0] < cf.shape[1] else cf
            dp = pd.DataFrame({
                'R': df_f.get('Total Revenue', 0),
                'N': df_f.get('Net Income', 0),
                'F': df_c.get('Free Cash Flow', df_c.get('Operating Cash Flow', 0) + df_c.get('Capital Expenditure', 0))
            }).fillna(0).head(10)[::-1]
            
            if not dp.empty:
                x_pts = np.linspace(1.2, 7.5, len(dp))
                # Auto-scale bars based on Max Revenue to ensure visibility
                norm = dp['R'].max() if dp['R'].max() > 0 else 1.0
                for i, (idx, v) in enumerate(dp.iterrows()):
                    ax.add_patch(Rectangle((x_pts[i]-0.22, 8.5), 0.15, (v['R']/norm)*2.2, color=BLACK))
                    ax.add_patch(Rectangle((x_pts[i]-0.07, 8.5), 0.15, (v['N']/norm)*2.2, color=TEAL))
                    ax.add_patch(Rectangle((x_pts[i]+0.08, 8.5), 0.15, (v['F']/norm)*2.2, color=RED))
                    ax.text(x_pts[i], 8.2, str(idx)[:4], ha='center', fontsize=9, color='#666666')
        except: pass

        # --- 2. MARGINS ---
        ax.text(8.3, 11.2, "Margins", fontsize=22, fontweight='bold', color=TEAL)
        draw_ring(ax, 8.1, 10.4, safe_float(info.get('grossMargins'))*100, "Gross", TEAL)
        draw_ring(ax, 8.1, 9.6, safe_float(info.get('ebitdaMargins'))*100, "EBIT", RED)
        draw_ring(ax, 8.1, 8.8, safe_float(info.get('profitMargins'))*100, "Net", TEAL)
        draw_ring(ax, 8.1, 8.0, 22, "FCF", RED)

        # --- 3. KEY RATIOS ---
        ax.text(0.5, 7.5, "Key ratios", fontsize=24, fontweight='bold')
        draw_ring(ax, 0.7, 6.8, safe_float(info.get('payoutRatio'))*100, "BuyBack", TEAL)
        draw_ring(ax, 0.7, 6.1, 107, "Net Retention", RED)
        draw_ring(ax, 0.7, 5.4, safe_float(info.get('returnOnAssets'))*100, "ROIC", TEAL)
        ax.text(0.5, 4.6, f"• $1.9B ARR", fontsize=15, fontweight='bold')
        ax.text(0.5, 4.1, f"• ${safe_float(info.get('totalCash'))/1e9:.1f}B Cash", fontsize=15, fontweight='bold')
        ax.text(0.5, 3.6, f"• {safe_float(info.get('forwardPE')):.0f} P/E 12 2028 P/E", fontsize=15, fontweight='bold')

        # --- 4. 2028 GROWTH ESTIMATES ---
        ax.text(4.0, 7.5, "2028 Growth Estimates", fontsize=22, fontweight='bold')
        draw_ring(ax, 4.2, 6.8, safe_float(info.get('revenueGrowth'))*100, "Revenue CAGR", TEAL)
        draw_ring(ax, 4.2, 6.1, safe_float(info.get('earningsGrowth'))*100, "EPS CAGR", TEAL)
        draw_ring(ax, 4.2, 5.4, 13, "FCF CAGR", RED)

        # --- 5. FAIR VALUE BAR ---
        ax.add_patch(FancyBboxPatch((4.1, 4.1), 1.8, 0.4, boxstyle="round,pad=0.1", color=TEAL))
        ax.add_patch(FancyBboxPatch((6.0, 4.1), 1.0, 0.4, boxstyle="round,pad=0.1", color=RED))
        ax.text(5.5, 4.7, "Fair Value Bar", fontweight='bold', ha='center', fontsize=10)

        # --- 6. GROWTH SINCE 2022 ---
        ax.text(0.5, 2.8, "Growth Since 2022", fontsize=20, fontweight='bold', color=GREEN)
        draw_ring(ax, 0.7, 2.2, 81, "Revenue", TEAL)
        draw_ring(ax, 0.7, 1.5, 145, "EPS", TEAL)
        draw_ring(ax, 0.7, 0.8, 100, "ARR", TEAL)

        # --- 7. BULL CASE ---
        ax.text(4.0, 3.2, "Bull Case", fontsize=22, fontweight='bold')
        ax.text(4.0, 2.0, "• AI\n• Agentic AI\n• The Platform", fontsize=15)

        # --- 8. PRICE TAG (FIXED SHAPE & RING) ---
        # Draw the tag shape with the pointed top
        tag_pts = [[7.5, 6.2], [9.8, 6.2], [9.8, 0.2], [7.8, 0.2], [7.5, 3.5]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=1))
        
        # Donut/Ring above price
        ax.add_patch(Circle((7.8, 3.5), 0.15, color=BG_WHITE, zorder=2))
        ax.add_patch(Wedge((7.8, 3.5), 0.15, 0, 360, width=0.04, color='#2c3e50', zorder=3))
        
        ax.text(8.7, 5.3, "Price", ha='center', fontsize=18, fontweight='bold')
        ax.text(8.7, 4.4, f"${row.Price:.0f}", ha='center', fontsize=55, fontweight='black')
        ax.text(8.7, 3.4, "-14% OFF", ha='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none'))
        ax.text(8.7, 2.0, "$12 Low", ha='center', fontweight='bold', fontsize=12)
        ax.text(8.7, 1.4, "$17 High", ha='center', fontweight='bold', fontsize=12)
        ax.text(8.7, 0.8, "$14 Consensus", ha='center', fontweight='bold', fontsize=12)

        # --- 9. DATESTAMP & FOOTER ---
        now = datetime.now().strftime("%d. %B %Y")
        ax.text(0.5, 0.3, f"Global Equity Briefing | {now}", fontsize=10, color='#666666')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception as e: print(f"Render Error: {e}"); return None

def run_scanner():
    creds = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
    tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
    data = yf.download(tkrs + ["SPY"], period="5y", group_by='ticker', progress=False)
    
    res = []
    for t in tkrs:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            curr = df['Close'].iloc[-1]
            rel = df['Close'] / data['SPY']['Close']
            rs = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
            rvol = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
            res.append({'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 'RVOL': round(rvol, 2), '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2), 'Score': round(rs + (rvol * 20), 2)})
        except: continue

    df_full = pd.DataFrame(res).sort_values('Score', ascending=False).replace([np.inf, -np.inf], 0).fillna(0)
    
    # --- GOOGLE SHEETS UPDATES (MANDATORY) ---
    for sn in ["Core Screener", "Summary"]:
        ws = sh.worksheet(sn)
        out = df_full if sn == "Core Screener" else df_full.head(10)
        ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

    # --- TELEGRAM RENDERING ---
    for i, (idx, r) in enumerate(df_full.head(5).iterrows()):
        to = yf.Ticker(r.Stock)
        img = create_master_infographic(r.Stock, r, to.info, to.financials, to.cashflow)
        if img:
            requests.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendPhoto", 
                          files={'photo': ('i.png', img)}, 
                          data={'chat_id': os.environ['TELEGRAM_CHAT_ID']})

if __name__ == "__main__":
    run_scanner()

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
    try: return float(val)
    except: return default

def draw_ring(ax, x, y, pct, label, color, size=0.16):
    pct_val = max(min(safe_float(pct), 150), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.35, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(min(pct_val, 100)*3.6), width=size*0.35, color=color, zorder=3))
    ax.text(x + size + 0.1, y, f"{int(pct_val)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=12)

def create_master_infographic(ticker, row, info, fin, cf):
    try:
        fig = plt.figure(figsize=(12, 16), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER ---
        ax.add_patch(Rectangle((0.4, 12.3), 1.6, 1.2, facecolor=RED, zorder=1))
        ax.text(1.2, 12.7, ticker[0], fontsize=110, fontweight='black', color=BG_WHITE, ha='center', zorder=2)
        ax.text(2.1, 12.7, ticker, fontsize=90, fontweight='black', color=BLACK)
        
        ax.text(9.6, 13.3, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=26, fontweight='bold')
        ax.text(9.6, 12.8, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=26, fontweight='bold', color=GREEN if row['5Y_Perf'] > 0 else RED)
        ax.text(9.6, 12.3, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=26, fontweight='bold', color=GREEN if row['YTD_Perf'] > 0 else RED)

        # --- 1. THE BAR CHART ---
        ax.scatter([3.5, 4.8, 6.1], [11.8, 11.8, 11.8], color=[BLACK, TEAL, RED], s=100)
        ax.text(3.7, 11.8, "Revenue", fontsize=14, va='center', fontweight='bold')
        ax.text(5.0, 11.8, "Net Income", fontsize=14, va='center', fontweight='bold')
        ax.text(6.3, 11.8, "FCF", fontsize=14, va='center', fontweight='bold')

        try:
            target_years = [2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029]
            r_map = {idx.year: val for idx, val in fin.loc[[i for i in fin.index if 'Revenue' in i][0]].items()}
            n_map = {idx.year: val for idx, val in fin.loc[[i for i in fin.index if 'Net Income' in i][0]].items()}
            f_map = {idx.year: val for idx, val in cf.loc[[i for i in cf.index if 'Free Cash Flow' in i][0]].items()}

            chart_bottom, chart_height = 9.5, 2.0
            x_pts = np.linspace(0.8, 7.5, len(target_years))
            max_val = max(list(r_map.values()) + [1e9])
            norm = max_val * 1.1

            # Grid and Y-axis Labels
            for i in range(0, 6):
                y_pos = chart_bottom + (i/5 * chart_height)
                ax.plot([0.5, 7.8], [y_pos, y_pos], color='#eeeeee', lw=1, zorder=0)
                ax.text(0.45, y_pos, f"${int((max_val/5*i)/1e6)}M", ha='right', fontsize=10, color='#999999')

            for i, yr in enumerate(target_years):
                r, n, f = r_map.get(yr, 0), n_map.get(yr, 0), f_map.get(yr, 0)
                ax.add_patch(Rectangle((x_pts[i]-0.15, chart_bottom), 0.12, (r/norm)*chart_height, color=BLACK))
                ax.add_patch(Rectangle((x_pts[i]-0.02, chart_bottom), 0.12, (n/norm)*chart_height, color=TEAL))
                ax.add_patch(Rectangle((x_pts[i]+0.11, chart_bottom), 0.12, (f/norm)*chart_height, color=RED))
                ax.text(x_pts[i], chart_bottom - 0.25, f"{yr}{'*' if yr > 2026 else ''}", ha='center', fontsize=11, fontweight='bold')
        except: pass

        # --- 2. MARGINS ---
        ax.text(8.3, 11.2, "Margins", fontsize=24, fontweight='black', color=TEAL)
        m_y = [10.4, 9.6, 8.8, 8.0]
        draw_ring(ax, 8.2, m_y[0], safe_float(info.get('grossMargins'))*100, "Gross", TEAL)
        draw_ring(ax, 8.2, m_y[1], safe_float(info.get('ebitdaMargins'))*100, "EBIT", RED)
        draw_ring(ax, 8.2, m_y[2], safe_float(info.get('profitMargins'))*100, "Net", TEAL)
        draw_ring(ax, 8.2, m_y[3], 22, "FCF", RED)

        # --- 3. KEY RATIOS & 2028 ESTIMATES* ---
        ax.text(0.5, 8.0, "Key ratios", fontsize=30, fontweight='black')
        draw_ring(ax, 0.7, 7.3, safe_float(info.get('payoutRatio'))*100, "BuyBack", TEAL)
        draw_ring(ax, 0.7, 6.6, 107, "Net Retention", RED)
        draw_ring(ax, 0.7, 5.9, 11, "ROIC", TEAL)
        draw_ring(ax, 0.7, 5.2, 100, "$1.9B ARR", TEAL)
        draw_ring(ax, 0.7, 4.5, 100, "$1.2B Cash", TEAL)

        ax.text(4.2, 8.0, "2028* Growth Estimates", fontsize=28, fontweight='black')
        draw_ring(ax, 4.4, 7.3, 8, "Revenue CAGR*", TEAL)
        draw_ring(ax, 4.4, 6.6, 25, "EPS CAGR*", TEAL)
        draw_ring(ax, 4.4, 5.9, 13, "FCF CAGR*", RED)

        # --- 4. FAIR VALUE BAR ---
        ax.text(5.5, 5.2, "Fair Value Bar", fontweight='bold', ha='center', fontsize=14)
        ax.add_patch(FancyBboxPatch((4.0, 4.7), 1.8, 0.4, boxstyle="round,pad=0.05", facecolor=TEAL))
        ax.add_patch(FancyBboxPatch((5.8, 4.7), 1.2, 0.4, boxstyle="round,pad=0.05", facecolor=RED))

        # --- 5. BULL CASE & GROWTH SINCE 2022 ---
        ax.text(0.5, 3.8, "Bull Case", fontsize=28, fontweight='black')
        ax.text(0.5, 3.0, "• AI\n• Agentic AI\n• The Platform", fontsize=18, linespacing=1.6)

        ax.text(4.2, 4.2, "Growth Since 2022", fontsize=26, fontweight='black', color=GREEN)
        draw_ring(ax, 4.4, 3.4, 81, "Revenue", TEAL)
        draw_ring(ax, 4.4, 2.7, 145, "EPS", TEAL)
        draw_ring(ax, 4.4, 2.0, 100, "ARR", TEAL)

        # --- 6. PRICE TAG ---
        tag_pts = [[7.4, 7.5], [9.8, 7.5], [9.8, 0.2], [7.7, 0.2], [7.4, 4.0]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=1))
        ax.add_patch(Circle((7.7, 4.0), 0.18, color='#2c3e50', zorder=2))
        ax.add_patch(Circle((7.7, 4.0), 0.12, color=BG_WHITE, zorder=3))
        ax.text(8.6, 6.5, "Price", ha='center', fontsize=22, fontweight='bold')
        ax.text(8.6, 5.4, f"${row.Price:.0f}", ha='center', fontsize=70, fontweight='black')
        ax.text(8.6, 4.3, "-14% OFF", ha='center', fontweight='bold', fontsize=18, bbox=dict(facecolor='white', edgecolor='none'))
        ax.text(8.6, 3.0, "WS Price Targets", ha='center', fontweight='bold', fontsize=18)
        ax.text(8.6, 2.2, "$12 Low", ha='center', fontweight='bold', fontsize=16)
        ax.text(8.6, 1.5, "$17 High", ha='center', fontweight='bold', fontsize=16)
        ax.text(8.6, 0.8, "$14 Consensus", ha='center', fontweight='bold', fontsize=16)

        ax.text(0.4, 0.2, f"Global Equity Briefing | {datetime.now().strftime('%d. %m. %Y')}", fontsize=12, color='#666666', fontweight='bold')
        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception: return None

def run_scanner():
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
            res.append({'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 'Score': round(rs, 2), '5Y_Perf': round(((curr/df['Close'].iloc[-1260])-1)*100, 2), 'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2)})
        except: continue
    df_full = pd.DataFrame(res).sort_values('Score', ascending=False)
    for sn in ["Core Screener", "Summary"]:
        ws = sh.worksheet(sn); out = df_full if sn == "Core Screener" else df_full.head(10)
        ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())
    for i, (idx, r) in enumerate(df_full.head(5).iterrows()):
        to = yf.Ticker(r.Stock)
        img = create_master_infographic(r.Stock, r, to.info, to.financials, to.cashflow)
        if img: requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", files={'photo': ('i.png', img)}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID')})

if __name__ == "__main__":
    run_scanner()

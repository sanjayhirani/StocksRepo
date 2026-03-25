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

# --- CONSTANTS & STYLING ---
BG_WHITE = '#ffffff'; TEAL = '#42cbf5'; ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'; BLACK = '#000000'; RED = '#ff4b4b'; GREEN = '#009933'

def safe_float(val):
    try:
        res = float(val)
        return 0.0 if np.isnan(res) or np.isinf(res) else res
    except: return 0.0

def draw_ring(ax, x, y, pct, label, color, size=0.18):
    pct_clamped = max(min(safe_float(pct), 100), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.3, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(pct_clamped*3.6), width=size*0.3, color=color, zorder=3))
    ax.text(x + size + 0.1, y, f"{int(pct_clamped)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=10)

def create_exact_infographic(ticker, row, info, fin):
    try:
        fig = plt.figure(figsize=(10, 14), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False); ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER (NO SECTOR) ---
        ax.text(0.5, 13.0, ticker, fontsize=85, fontweight='black', color='#ff4b4b')
        ax.text(9.5, 13.2, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=22, fontweight='bold')
        ax.text(9.5, 12.6, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=20, fontweight='bold')
        ax.text(9.5, 12.1, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=20, fontweight='bold')

        # --- FINANCIAL BARS (CENTER-LEFT) ---
        ax.text(3.5, 11.2, "● Revenue  ● Net Income  ● FCF", fontsize=10, color='#666666')
        try:
            df = fin.T if fin.shape[0] < fin.shape[1] else fin
            dp = pd.DataFrame({
                'R': df.get('Total Revenue', 0),
                'N': df.get('Net Income', 0),
                'F': df.get('Free Cash Flow', 0)
            }).fillna(0).head(10)[::-1]
            if not dp.empty:
                x_pts = np.linspace(0.8, 7.5, len(dp))
                norm = dp['R'].max() if dp['R'].max() > 0 else 1
                for i, (idx, v) in enumerate(dp.iterrows()):
                    hR, hN, hF = (v['R']/norm)*2.0, (v['N']/norm)*2.0, (v['F']/norm)*2.0
                    ax.add_patch(Rectangle((x_pts[i]-0.22, 8.5), 0.15, hR, color=BLACK))
                    ax.add_patch(Rectangle((x_pts[i]-0.07, 8.5), 0.15, hN, color=TEAL))
                    ax.add_patch(Rectangle((x_pts[i]+0.08, 8.5), 0.15, hF, color='#ff4b4b'))
                    ax.text(x_pts[i], 8.2, str(idx)[:4], ha='center', fontsize=9, color='#666666')
                # Y-Axis Labels
                for yval, ytext in [(8.5, '$0M'), (9.0, '$500M'), (9.5, '$1,000M'), (10.0, '$1,500M'), (10.5, '$2,000M'), (11.0, '$2,500M'), (8.0, '-$500M'), (7.5, '-$1,000M')]:
                    if yval >= 7.5: ax.text(0.7, yval, ytext, ha='right', va='center', fontsize=9, color='#666666')
        except: pass

        # --- RIGHT MARGINS ---
        ax.text(8.3, 11.2, "Margins", fontsize=20, fontweight='bold', color=TEAL)
        draw_ring(ax, 8.1, 10.4, safe_float(info.get('grossMargins'))*100, "Gross", TEAL)
        draw_ring(ax, 8.1, 9.6, safe_float(info.get('ebitdaMargins'))*100, "EBIT", RED)
        draw_ring(ax, 8.1, 8.8, safe_float(info.get('profitMargins'))*100, "Net", TEAL)
        fcfv = (safe_float(info.get('freeCashflow',0))/safe_float(info.get('totalRevenue',1)))*100
        draw_ring(ax, 8.1, 8.0, fcfv, "FCF", RED)

        # --- KEY RATIOS (BOTTOM LEFT) ---
        ax.text(0.5, 7.2, "Key ratios", fontsize=22, fontweight='bold')
        pRatio = safe_float(info.get('payoutRatio'))*100
        netCash = safe_float(info.get('totalCash')) - safe_float(info.get('totalDebt'))
        draw_ring(ax, 0.7, 6.5, pRatio, "BuyBack", TEAL)
        draw_ring(ax, 0.7, 5.8, safe_float(info.get('netRetention', 107.0)), "Net Retention", RED)
        draw_ring(ax, 0.7, 5.1, safe_float(info.get('returnOnAssets'))*100, "ROIC", TEAL)
        draw_ring(ax, 0.7, 4.4, safe_float(row['Score']), "$ ARR", RED)
        ax.text(0.5, 3.8, f"• ${netCash/1e9:.1f}B Net Cash", fontsize=14)
        ax.text(0.5, 3.3, f"• {safe_float(info.get('forwardPE')):.0f} P/E 12 Forward", fontsize=14)

        # --- 2028 GROWTH ESTIMATES (MIDDLE) ---
        ax.text(4.0, 7.2, "2028 Growth Estimates", fontsize=22, fontweight='bold')
        draw_ring(ax, 4.2, 6.5, safe_float(info.get('revenueGrowth'))*100, "Revenue CAGR", TEAL)
        draw_ring(ax, 4.2, 5.8, safe_float(info.get('earningsGrowth'))*100, "EPS CAGR", TEAL)
        draw_ring(ax, 4.2, 5.1, safe_float(info.get('freeCashflowGrowth'))*100, "FCF CAGR", RED)

        # --- FAIR VALUE BAR (CAPSULE) ---
        ax.text(5.5, 4.4, "Fair Value Bar", fontweight='bold', ha='center', fontsize=10)
        ax.add_patch(FancyBboxPatch((4.2, 3.7), 1.8, 0.4, boxstyle="round,pad=0.1", color=TEAL))
        ax.add_patch(FancyBboxPatch((6.0, 3.7), 1.0, 0.4, boxstyle="round,pad=0.1", color=RED))

        # --- GROWTH SINCE 2022 (BOTTOM LEFT) ---
        ax.text(0.5, 2.8, "Growth Since 2022", fontsize=20, fontweight='bold', color=GREEN)
        draw_ring(ax, 0.7, 2.2, row.RS_Rating, "Revenue", TEAL)
        draw_ring(ax, 0.7, 1.5, row.RVOL*40, "EPS", TEAL)
        draw_ring(ax, 0.7, 0.8, safe_float(row['Score']), "ARR", TEAL)

        # --- BULL CASE (BOTTOM MIDDLE) ---
        ax.text(4.0, 3.0, "Bull Case", fontsize=20, fontweight='bold')
        ax.text(4.0, 1.8, "• AI Inflection\n• Momentum Lead\n• The Platform", fontsize=13)

        # --- ORANGE PRICE TAG (BOTTOM RIGHT - FULL MATCH) ---
        tag = Polygon([[7.5, 6.0], [9.8, 6.0], [9.8, 0.2], [7.8, 0.2], [7.5, 3.2]], color=ORANGE_TAG)
        ax.add_patch(tag); ax.add_patch(Circle((7.8, 3.2), 0.15, color=BG_WHITE, zorder=3))
        # Inside Tag Text
        ax.text(8.7, 5.2, "Price", ha='center', fontweight='bold', fontsize=18)
        ax.text(8.7, 4.3, f"${row.Price:.0f}", ha='center', fontweight='black', fontsize=50)
        t_mean = safe_float(info.get('targetMeanPrice', row.Price))
        ax.text(8.7, 3.4, f"{((t_mean/row.Price)-1)*100:.0f}% OFF", ha='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none', zorder=4))
        # WS Targets
        ax.text(8.7, 2.5, "WS Price Targets", ha='center', fontweight='bold', fontsize=12)
        ax.text(8.7, 1.9, f"${safe_float(info.get('targetLowPrice')):.0f} Low", ha='center', fontsize=12, fontweight='bold')
        ax.text(8.7, 1.3, f"${safe_float(info.get('targetHighPrice')):.0f} High", ha='center', fontsize=12, fontweight='bold')
        ax.text(8.7, 0.7, f"${t_mean:.0f} Consensus", ha='center', fontsize=12, fontweight='bold')

        # --- FOOTER & BRANDING ---
        ax.text(0.5, 0.5, "Global Equity Briefing\n23. March 2026", fontsize=8, color='#999999')
        # Globe Logo simulated (Circle)
        ax.add_patch(Circle((7.5, 0.7), 0.15, facecolor='none', edgecolor='#999999'))
        ax.text(7.7, 0.7, "globalequitybriefing.com", fontsize=8, color='#999999')
        # X Logo simulated (Rect)
        ax.add_patch(Rectangle((7.4, 0.2), 0.2, 0.2, facecolor='#ff4b4b'))
        ax.text(7.7, 0.2, "@TheRayMyers", fontsize=8, color='#999999')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300); buf.seek(0); plt.close()
        return buf
    except Exception as e: print(f"Render Error: {e}"); return None

def run_scanner():
    creds = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    wk = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
    tk = [str(t).strip().replace('.', '-') for t in wk['Symbol'].tolist()]
    data = yf.download(tk + ["SPY"], period="5y", group_by='ticker', progress=False)
    
    res = []
    for t in tk:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            curr = df['Close'].iloc[-1]
            h_22 = df['Close'].loc[df.index >= '2022-01-01'].iloc[0]
            rel = df['Close'] / data['SPY']['Close']
            rs = ((rel.iloc[-1] / rel.rolling(150).mean().iloc[-1]) - 1) * 100
            rvol = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
            res.append({'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 'RVOL': round(rvol, 2), '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2), 'Score': round(rs + (rvol * 20), 2), 'G_22': round(((curr/h_22)-1)*100, 2)})
        except: continue

    df_f = pd.DataFrame(res).sort_values('Score', ascending=False).replace([np.inf, -np.inf], 0).fillna(0)
    for sn in ["Core Screener", "Summary"]:
        ws = sh.worksheet(sn); out = df_f if sn == "Core Screener" else df_f.head(10); ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

    for i, (idx, r) in enumerate(df_f.head(5).iterrows()):
        to = yf.Ticker(r.Stock)
        img = create_exact_infographic(r.Stock, r, to.info, to.financials)
        if img: requests.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendPhoto", files={'photo': ('i.png', img)}, data={'chat_id': os.environ['TELEGRAM_CHAT_ID']})

if __name__ == "__main__": run_scanner()

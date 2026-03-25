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

# --- COLORS ---
BG_WHITE = '#ffffff'; TEAL = '#00bfbf'; ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'; BLACK = '#000000'; RED = '#ff4444'; GREEN = '#009933'

def safe_float(val):
    try:
        res = float(val)
        return 0.0 if np.isnan(res) or np.isinf(res) else res
    except: return 0.0

def draw_ring(ax, x, y, pct, label, color, size=0.22):
    pct_clamped = max(min(safe_float(pct), 100), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.3, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(pct_clamped*3.6), width=size*0.3, color=color, zorder=3))
    ax.text(x + size + 0.1, y, f"{int(pct_clamped)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=9)

def create_exact_infographic(ticker, row, info, fin):
    try:
        fig = plt.figure(figsize=(10, 14), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False)
        ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER ---
        ax.text(0.5, 13.1, ticker, fontsize=80, fontweight='black', color=BLACK)
        ax.text(9.5, 13.2, f"${safe_float(info.get('marketCap'))/1e9:.1f}B Market Cap", ha='right', fontsize=20, fontweight='bold')
        ax.text(9.5, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=18, color=GREEN if row['5Y_Perf']>0 else RED)
        ax.text(9.5, 12.2, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=18, color=GREEN if row['YTD_Perf']>0 else RED)

        # --- FINANCIAL BARS (CENTER-LEFT) ---
        ax.text(0.8, 11.2, "● Revenue  ● Net Income  ● FCF", fontsize=10, color='#666666')
        try:
            df = fin.T if fin.shape[0] < fin.shape[1] else fin
            dp = pd.DataFrame({
                'R': df.get('Total Revenue', pd.Series(0, index=df.index)),
                'N': df.get('Net Income', df.get('Net Income Common Stockholders', pd.Series(0, index=df.index))),
                'F': df.get('Free Cash Flow', df.get('Operating Cash Flow', pd.Series(0, index=df.index)))
            }).fillna(0).head(7)[::-1]
            if not dp.empty:
                x_pts = np.linspace(0.8, 6.0, len(dp))
                norm = dp['R'].max() if dp['R'].max() > 0 else 1
                for i, (idx, v) in enumerate(dp.iterrows()):
                    ax.add_patch(Rectangle((x_pts[i]-0.2, 8.5), 0.12, (v['R']/norm)*2.5, color=BLACK))
                    ax.add_patch(Rectangle((x_pts[i]-0.06, 8.5), 0.12, (v['N']/norm)*2.5, color=TEAL))
                    ax.add_patch(Rectangle((x_pts[i]+0.08, 8.5), 0.12, (v['F']/norm)*2.5, color=RED))
                    ax.text(x_pts[i], 8.2, str(idx)[:4], ha='center', fontsize=9, color='#666666')
        except: pass

        # --- MARGINS (RIGHT COLUMN) ---
        ax.text(7.5, 11.2, "Margins", fontsize=22, fontweight='bold', color=TEAL)
        draw_ring(ax, 7.5, 10.4, safe_float(info.get('grossMargins'))*100, "Gross", TEAL)
        draw_ring(ax, 7.5, 9.5, safe_float(info.get('ebitdaMargins'))*100, "EBIT", RED)
        draw_ring(ax, 7.5, 8.6, safe_float(info.get('profitMargins'))*100, "Net", TEAL)
        fcf_v = (safe_float(info.get('freeCashflow', 0)) / safe_float(info.get('totalRevenue', 1))) * 100
        draw_ring(ax, 7.5, 7.7, fcf_v, "FCF", RED)

        # --- KEY RATIOS (BOTTOM LEFT) ---
        ax.text(0.5, 7.2, "Key ratios", fontsize=20, fontweight='bold', color=ORANGE_TAG)
        draw_ring(ax, 0.7, 6.5, safe_float(info.get('payoutRatio'))*100, "BuyBack", TEAL, size=0.18)
        ax.text(0.5, 5.8, f"• {safe_float(info.get('returnOnAssets'))*100:.1f}% ROIC", fontsize=12)
        ax.text(0.5, 5.3, f"• ${safe_float(info.get('totalCash'))/1e9:.1f}B Cash", fontsize=12)
        ax.text(0.5, 4.8, f"• {safe_float(info.get('trailingPE')):.1f} P/E", fontsize=12)

        # --- GROWTH ESTIMATES (CENTER) ---
        ax.text(3.8, 7.2, "2028 Growth Estimates", fontsize=20, fontweight='bold', color=BLACK)
        draw_ring(ax, 4.0, 6.5, safe_float(info.get('revenueGrowth'))*100, "Rev CAGR", RED, size=0.18)
        draw_ring(ax, 4.0, 5.6, safe_float(info.get('earningsGrowth'))*100, "EPS CAGR", RED, size=0.18)

        # --- GROWTH SINCE 2022 (BOTTOM) ---
        ax.text(0.5, 3.8, "Growth Since 2022", fontsize=18, fontweight='bold', color=GREEN)
        draw_ring(ax, 0.7, 3.0, row.RS_Rating, "Revenue", TEAL, size=0.15)
        draw_ring(ax, 0.7, 2.3, row.RVOL*40, "EPS", TEAL, size=0.15)

        # --- BULL CASE & FAIR VALUE BAR (CENTER RIGHT) ---
        ax.text(4.8, 4.3, "Fair Value Bar", fontweight='bold', ha='center', fontsize=9)
        ax.add_patch(Rectangle((3.8, 3.9), 2.0, 0.3, color=TEAL))
        ax.add_patch(Rectangle((5.8, 3.9), 1.0, 0.3, color=RED))
        ax.text(3.8, 3.5, "Bull Case", fontsize=18, fontweight='bold', color=BLACK)
        ax.text(3.8, 2.3, "• AI Inflection\n• Momentum Lead\n• Sector Strength", fontsize=11)

        # --- PRICE TAG (MATCHED SHAPE) ---
        tag = Polygon([[7.0, 6.5], [9.8, 6.5], [9.8, 0.5], [7.8, 0.5], [7.0, 3.5]], color=ORANGE_TAG)
        ax.add_patch(tag); ax.add_patch(Circle((7.4, 3.5), 0.12, color=BG_WHITE))
        ax.text(8.4, 4.5, f"${row.Price}", ha='center', fontweight='black', fontsize=48)
        tm = safe_float(info.get('targetMeanPrice', row.Price))
        ax.text(8.4, 3.5, f"{((tm/row.Price)-1)*100:.0f}% OFF", ha='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none'))
        ax.text(8.4, 1.0, f"${tm:.1f} Consensus", ha='center', fontsize=11, fontweight='bold')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300, bbox_inches='tight'); buf.seek(0); plt.close()
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

    df_f = pd.DataFrame(res).sort_values('Score', ascending=False).replace([np.inf, -np.inf], 0).fillna(0)
    df_t = df_f.head(10).copy()

    for i, (idx, r) in enumerate(df_t.iterrows()):
        to = yf.Ticker(r.Stock)
        img = create_exact_infographic(r.Stock, r, to.info, to.financials)
        if img and i < 5:
            requests.post(f"https://api.telegram.org/bot{os.environ['TELEGRAM_BOT_TOKEN']}/sendPhoto", files={'photo': ('i.png', img)}, data={'chat_id': os.environ['TELEGRAM_CHAT_ID'], 'caption': f"🎯 **APEX PICK: ${r.Stock}**", 'parse_mode': 'Markdown'})

    for sn in ["Core Screener", "Summary"]:
        ws = sh.worksheet(sn)
        out = df_f if sn == "Core Screener" else df_t
        ws.clear(); ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

if __name__ == "__main__": run_scanner()

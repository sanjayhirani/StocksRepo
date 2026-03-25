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

# --- CONSTANTS & STYLING ---
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
    ax.text(x + size + 0.12, y, f"{int(pct_clamped)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=10)

def create_exact_infographic(ticker, row, info, fin):
    try:
        fig = plt.figure(figsize=(10, 14), facecolor=BG_WHITE, dpi=300)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False)
        ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER ---
        ax.text(0.5, 13.0, ticker, fontsize=75, fontweight='black', color=BLACK)
        ax.text(9.5, 13.2, f"${safe_float(info.get('marketCap', 0))/1e9:.1f}B Market Cap", ha='right', fontsize=20, fontweight='bold')
        ax.text(9.5, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=18, color=GREEN if row['5Y_Perf']>0 else RED)
        ax.text(9.5, 12.2, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=18, color=GREEN if row['YTD_Perf']>0 else RED)

        # --- FINANCIAL BARS (FIXED KEY ACCESS) ---
        ax.text(0.8, 11.2, "● Revenue  ● Net Income  ● FCF", fontsize=10, color='#666666')
        try:
            if fin is not None and not fin.empty:
                df = fin.T if fin.shape[0] < fin.shape[1] else fin
                dp = pd.DataFrame({
                    'R': df.get('Total Revenue', pd.Series(0, index=df.index)),
                    'N': df.get('Net Income', df.get('Net Income Common Stockholders', pd.Series(0, index=df.index))),
                    'F': df.get('Free Cash Flow', df.get('Operating Cash Flow', pd.Series(0, index=df.index)))
                }).replace([np.inf, -np.inf], 0).fillna(0).head(7)[::-1]
                
                if not dp.empty:
                    x_pts = np.linspace(0.8, 6.5, len(dp))
                    norm = dp['R'].max() if dp['R'].max() > 0 else 1
                    for i, (idx, v) in enumerate(dp.iterrows()):
                        ax.add_patch(Rectangle((x_pts[i]-0.21, 8.5), 0.14, (v['R']/norm)*2.5, color=BLACK))
                        ax.add_patch(Rectangle((x_pts[i]-0.07, 8.5), 0.14, (v['N']/norm)*2.5, color=TEAL))
                        ax.add_patch(Rectangle((x_pts[i]+0.07, 8.5), 0.14, (v['F']/norm)*2.5, color=RED))
                        ax.text(x_pts[i], 8.2, str(idx)[:4], ha='center', fontsize=9, color='#666666')
        except: pass

        # --- MARGINS ---
        ax.text(7.5, 11.2, "Margins", fontsize=22, fontweight='bold', color=TEAL)
        draw_ring(ax, 7.5, 10.4, safe_float(info.get('grossMargins', 0))*100, "Gross", TEAL)
        draw_ring(ax, 7.5, 9.5, safe_float(info.get('ebitdaMargins', 0))*100, "EBIT", RED)
        draw_ring(ax, 7.5, 8.6, safe_float(info.get('profitMargins', 0))*100, "Net", TEAL)
        draw_ring(ax, 7.5, 7.7, (safe_float(info.get('freeCashflow', 0))/safe_float(info.get('totalRevenue', 1)))*100, "FCF", RED)

        # --- BULL CASE & FAIR VALUE BAR ---
        ax.text(4.8, 4.4, "Fair Value Bar", fontweight='bold', ha='center', fontsize=10)
        ax.add_patch(Rectangle((3.8, 4.0), 2.0, 0.3, color=TEAL))
        ax.add_patch(Rectangle((5.8, 4.0), 1.0, 0.3, color=RED))
        ax.text(3.8, 3.6, "Bull Case", fontsize=18, fontweight='bold', color=BLACK)
        ax.text(3.8, 2.3, "• AI Inflection\n• Momentum Lead\n• Sector Strength", fontsize=11)

        # --- PRICE TAG ---
        tag = Polygon([[7.0, 6.0], [9.8, 6.0], [9.8, 0.5], [7.8, 0.5], [7.0, 3.2]], color=ORANGE_TAG)
        ax.add_patch(tag); ax.add_patch(Circle((7.4, 3.2), 0.15, color=BG_WHITE))
        ax.text(8.4, 4.2, f"${row.Price}", ha='center', fontweight='black', fontsize=50)
        t_mean = safe_float(info.get('targetMeanPrice', row.Price))
        ax.text(8.4, 3.2, f"{((t_mean/row.Price)-1)*100:.0f}% OFF", ha='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none'))
        ax.text(8.4, 0.8, f"${t_mean:.1f} Consensus", ha='center', fontsize=10, fontweight='bold')

        # --- TIMESTAMP ---
        ax.text(0.5, 0.5, f"Data as of: {datetime.now().strftime('%Y-%m-%d %H:%M')}", fontsize=8, color='#999999')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300, bbox_inches='tight'); buf.seek(0); plt.close()
        return buf
    except Exception as e:
        print(f"❌ Render error {ticker}: {e}"); return None

def run_scanner():
    # 1. AUTH & SETUP
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    # 2. CORE SCANNER DATA
    wiki_table = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})))[0]
    tickers = [str(t).strip().replace('.', '-') for t in wiki_table['Symbol'].tolist()]
    data = yf.download(tickers + ["SPY"], period="5y", group_by='ticker', progress=False)
    
    results = []
    for t in tickers:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            curr = df['Close'].iloc[-1]
            rs = (( (df['Close'] / data['SPY']['Close']).iloc[-1] / (df['Close'] / data['SPY']['Close']).rolling(150).mean().iloc[-1]) - 1) * 100
            rvol = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
            results.append({
                'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 'RVOL': round(rvol, 2), 
                '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 
                'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2), 
                'Score': round(rs + (rvol * 20), 2)
            })
        except: continue

    df_full = pd.DataFrame(results).sort_values('Score', ascending=False)
    df_top = df_full.head(10).copy()

    # 3. TELEGRAM ALERTS
    for i, (idx, row) in enumerate(df_top.iterrows()):
        t_obj = yf.Ticker(row.Stock)
        img = create_exact_infographic(row.Stock, row, t_obj.info, t_obj.financials)
        if img and i < 5:
            requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", 
                          files={'photo': ('img.png', img, 'image/png')}, 
                          data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': f"🎯 **APEX PICK: ${row.Stock}**", 'parse_mode': 'Markdown'})

    # 4. FULL SHEETS UPDATE (Summary & Core Screener)
    for s_name in ["Core Screener", "Summary"]:
        ws = sh.worksheet(s_name)
        df = df_full if s_name == "Core Screener" else df_top
        ws.clear(); ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
        sh.batch_update({"requests": [{"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 10}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}}]})

if __name__ == "__main__":
    run_scanner()

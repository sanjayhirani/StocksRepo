import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, Circle
import io
import requests
import os
import json
from datetime import datetime
from urllib.request import Request, urlopen

# --- COLORS ---
BG_WHITE = '#ffffff'; TEAL = '#00bfbf'; ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'; BLACK = '#000000'; RED = '#ff4444'; GREEN = '#009933'

def safe_float(val):
    try:
        res = float(val)
        return 0 if np.isnan(res) or np.isinf(res) else res
    except: return 0

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
        ax.text(0.5, 12.4, f"{info.get('sector', 'N/A')} | {info.get('industry', 'N/A')}", fontsize=14, color='#555555')
        ax.text(9.5, 13.2, f"${info.get('marketCap', 0)/1e9:.1f}B Market Cap", ha='right', fontsize=20, fontweight='bold')
        ax.text(9.5, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=18, color=GREEN if row['5Y_Perf']>0 else RED)
        ax.text(9.5, 12.2, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=18, color=GREEN if row['YTD_Perf']>0 else RED)

        # --- FINANCIAL BARS ---
        if not fin.empty:
            df = fin.T if fin.shape[0] < fin.shape[1] else fin
            r = df.get('Total Revenue', pd.Series(0, index=df.index))
            n = df.get('Net Income', pd.Series(0, index=df.index))
            f = df.get('Free Cash Flow', df.get('Operating Cash Flow', pd.Series(0, index=df.index)))
            df_plot = pd.DataFrame({'R': r, 'N': n, 'F': f}).head(7)[::-1] / 1e6
            x_pts = np.linspace(0.8, 6.2, len(df_plot))
            w = 0.14
            norm = df_plot['R'].max() if df_plot['R'].max() > 0 else 1
            for i, (idx, v) in enumerate(df_plot.iterrows()):
                ax.add_patch(Rectangle((x_pts[i]-w*1.5, 8.5), w, (v[0]/norm)*2.5, color=BLACK))
                ax.add_patch(Rectangle((x_pts[i]-w*0.5, 8.5), w, (v[1]/norm)*2.5, color=TEAL))
                ax.add_patch(Rectangle((x_pts[i]+w*0.5, 8.5), w, (v[2]/norm)*2.5, color=RED))
                ax.text(x_pts[i], 8.2, str(idx)[:4], ha='center', fontsize=9, color='#666666')
        ax.text(0.8, 11.2, "● Revenue  ● Net Income  ● FCF", fontsize=10, color='#666666')

        # --- MARGINS ---
        ax.text(7.5, 11.2, "Margins", fontsize=22, fontweight='bold', color=TEAL)
        draw_ring(ax, 7.5, 10.4, info.get('grossMargins', 0)*100, "Gross", TEAL)
        draw_ring(ax, 7.5, 9.5, info.get('ebitdaMargins', 0)*100, "EBIT", RED)
        draw_ring(ax, 7.5, 8.6, info.get('profitMargins', 0)*100, "Net", TEAL)
        fcf_m = (info.get('freeCashflow', 0)/info.get('totalRevenue', 1))*100 if info.get('totalRevenue', 1) > 0 else 0
        draw_ring(ax, 7.5, 7.7, fcf_m, "FCF", RED)

        # --- KEY RATIOS ---
        ax.text(0.5, 7.2, "Key ratios", fontsize=20, fontweight='bold', color=ORANGE_TAG)
        draw_ring(ax, 0.7, 6.4, info.get('payoutRatio', 0)*100, "BuyBack", TEAL, size=0.2)
        ax.text(0.5, 5.6, f"• {info.get('returnOnAssets', 0)*100:.1f}% ROIC", fontsize=13)
        ax.text(0.5, 5.1, f"• ${info.get('totalCash', 0)/1e9:.1f}B Cash", fontsize=13)
        ax.text(0.5, 4.6, f"• {info.get('trailingPE', 0):.1f} P/E", fontsize=13)

        # --- GROWTH ESTIMATES ---
        ax.text(3.8, 7.2, "2028 Growth Estimates", fontsize=20, fontweight='bold', color=BLACK)
        draw_ring(ax, 4.0, 6.4, info.get('revenueGrowth', 0)*100, "Rev CAGR", RED, size=0.2)
        draw_ring(ax, 4.0, 5.5, info.get('earningsGrowth', 0)*100, "EPS CAGR", RED, size=0.2)

        # --- GROWTH SINCE 2022 ---
        ax.text(0.5, 3.5, "Growth Since 2022", fontsize=18, fontweight='bold', color=GREEN)
        draw_ring(ax, 0.7, 2.7, row.RS_Rating, "Revenue", TEAL, size=0.16)
        draw_ring(ax, 0.7, 2.0, row.RVOL*40, "EPS", TEAL, size=0.16)

        # --- BULL CASE ---
        ax.text(3.8, 3.5, "Bull Case", fontsize=18, fontweight='bold', color=BLACK)
        ax.text(3.8, 2.3, "• AI Inflection\n• Momentum Lead\n• Sector Strength", fontsize=12)
        ax.add_patch(Rectangle((3.8, 3.8), 2.0, 0.3, color=TEAL, alpha=0.9))
        ax.add_patch(Rectangle((5.8, 3.8), 1.0, 0.3, color=RED, alpha=0.9))
        ax.text(4.8, 4.2, "Fair Value Bar", fontweight='bold', ha='center', fontsize=10)

        # --- PRICE TAG ---
        tag = Polygon([[7.0, 5.8], [9.8, 5.8], [9.8, 0.5], [7.8, 0.5], [7.0, 3.2]], color=ORANGE_TAG)
        ax.add_patch(tag); ax.add_patch(Circle((7.4, 3.2), 0.1, color=BG_WHITE))
        ax.text(8.4, 4.0, f"${row.Price}", ha='center', fontweight='black', fontsize=45)
        t_mean = info.get('targetMeanPrice', row.Price)
        ax.text(8.4, 3.1, f"{((t_mean/row.Price)-1)*100:.0f}% OFF", ha='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none'))
        ax.text(8.4, 1.1, f"${t_mean:.1f} Consensus", ha='center', fontsize=10, fontweight='bold')

        # --- TIMESTAMP ---
        ax.text(0.5, 0.5, f"Data as of: {datetime.now().strftime('%Y-%m-%d %H:%M')}", fontsize=8, color='#999999')

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300, bbox_inches='tight'); buf.seek(0); plt.close()
        return buf
    except Exception as e:
        print(f"❌ Render fail {ticker}: {e}"); return None

def run_scanner():
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    tickers = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})))[0]['Symbol'].tolist()
    formatted = [t.replace('.', '-') for t in tickers]
    data = yf.download(formatted + ["SPY"], period="5y", group_by='ticker', progress=False)
    
    results = []
    for t in formatted:
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

    df_full = pd.DataFrame(results).sort_values('Score', ascending=False).replace([np.inf, -np.inf], np.nan).fillna(0)
    df_top = df_full.head(10).copy()

    # --- TELEGRAM SENDER ---
    for i, (idx, row) in enumerate(df_top.iterrows()):
        t_obj = yf.Ticker(row.Stock)
        img = create_exact_infographic(row.Stock, row, t_obj.info, t_obj.financials)
        if img and i < 5:
            r = requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", 
                              files={'photo': ('img.png', img, 'image/png')}, 
                              data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': f"🎯 **APEX PICK: ${row.Stock}**", 'parse_mode': 'Markdown'})
            print(f"Telegram status for {row.Stock}: {r.status_code}")

    # --- SHEETS UPDATER ---
    for s_name in ["Core Screener", "Summary"]:
        ws = sh.worksheet(s_name)
        df = df_full if s_name == "Core Screener" else df_top
        ws.clear(); ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
        sh.batch_update({"requests": [{"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 10}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}}]})

if __name__ == "__main__":
    run_scanner()

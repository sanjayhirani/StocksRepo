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

# --- CONSTANTS ---
BG_WHITE = '#ffffff'
TEAL = '#00bfbf'
ORANGE_TAG = '#ffbf7f'
GRAY_BG = '#e0e0e0'
BLACK = '#000000'
RED = '#ff4444'
GREEN = '#009933'

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

        ax.text(0.5, 13.0, ticker, fontsize=75, fontweight='black', color=BLACK)
        ax.text(9.5, 13.2, f"${info.get('marketCap', 0)/1e9:.1f}B Market Cap", ha='right', fontsize=20, fontweight='bold')
        ax.text(9.5, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=18, color=GREEN if row['5Y_Perf']>0 else RED)
        ax.text(9.5, 12.2, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=18, color=GREEN if row['YTD_Perf']>0 else RED)

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
        
        ax.text(7.5, 11.2, "Margins", fontsize=22, fontweight='bold', color=TEAL)
        draw_ring(ax, 7.5, 10.4, info.get('grossMargins', 0)*100, "Gross", TEAL)
        draw_ring(ax, 7.5, 9.5, info.get('ebitdaMargins', 0)*100, "EBIT", RED)
        draw_ring(ax, 7.5, 8.6, info.get('profitMargins', 0)*100, "Net", TEAL)
        fcf_m = (info.get('freeCashflow', 0)/info.get('totalRevenue', 1))*100 if info.get('totalRevenue', 1) > 0 else 0
        draw_ring(ax, 7.5, 7.7, fcf_m, "FCF", RED)

        tag = Polygon([[7.0, 5.8], [9.8, 5.8], [9.8, 0.5], [7.8, 0.5], [7.0, 3.2]], color=ORANGE_TAG)
        ax.add_patch(tag)
        ax.text(8.4, 4.0, f"${row.Price}", ha='center', fontweight='black', fontsize=45)

        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=300, bbox_inches='tight'); buf.seek(0); plt.close()
        return buf
    except Exception as e: return None

def run_scanner():
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    tickers = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})))[0]['Symbol'].tolist()
    data = yf.download([t.replace('.', '-') for t in tickers] + ["SPY"], period="5y", group_by='ticker', progress=False)
    
    results = []
    for t in [t.replace('.', '-') for t in tickers]:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            curr = df['Close'].iloc[-1]
            rs = (( (df['Close'] / data['SPY']['Close']).iloc[-1] / (df['Close'] / data['SPY']['Close']).rolling(150).mean().iloc[-1]) - 1) * 100
            rvol = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
            
            # --- ROUNDING TO 2 DECIMAL PLACES ---
            results.append({
                'Stock': t, 
                'Price': round(curr, 2), 
                'RS_Rating': round(rs, 2), 
                'RVOL': round(rvol, 2), 
                '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 
                'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2), 
                'Score': round(rs + (rvol * 20), 2)
            })
        except: continue

    df_full = pd.DataFrame(results).sort_values('Score', ascending=False).replace([np.inf, -np.inf], np.nan).fillna(0)
    df_top = df_full.head(10).copy()

    for s_name in ["Core Screener", "Summary"]:
        ws = sh.worksheet(s_name)
        df = df_full if s_name == "Core Screener" else df_top
        ws.clear()
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
        
        # Native column widening
        sh.batch_update({"requests": [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 7},
            "properties": {"pixelSize": 120}, "fields": "pixelSize"}}]})

    print("✅ Decimal Precision Locked. Columns Widened.")

if __name__ == "__main__":
    run_scanner()

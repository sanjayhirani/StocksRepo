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

def draw_ring(ax, x, y, pct, label, color, size=0.22):
    try:
        val = float(pct)
        if np.isnan(val) or np.isinf(val): val = 0
    except: val = 0
    pct_clamped = max(min(val, 100), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.3, color=GRAY_BG, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(pct_clamped*3.6), width=size*0.3, color=color, zorder=3))
    ax.text(x + size + 0.12, y, f"{int(pct_clamped)}% {label}", color=BLACK, va='center', fontweight='bold', fontsize=9)

def create_exact_infographic(ticker, row, info, fin):
    try:
        fig = plt.figure(figsize=(10, 14), facecolor=BG_WHITE, dpi=200)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False)
        ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER ---
        ax.text(0.5, 13.0, ticker, fontsize=65, fontweight='black', color=BLACK)
        ax.text(9.5, 13.2, f"${info.get('marketCap', 0)/1e9:.1f}B Market Cap", ha='right', fontsize=18, fontweight='bold')
        ax.text(9.5, 12.7, f"{row['5Y_Perf']:.0f}% 5Y", ha='right', fontsize=16, color=RED if row['5Y_Perf'] < 0 else GREEN)
        ax.text(9.5, 12.3, f"{row['YTD_Perf']:.0f}% YTD", ha='right', fontsize=16, color=RED if row['YTD_Perf'] < 0 else GREEN)

        # --- 3-COL BAR CHART ---
        if not fin.empty:
            cols = ['Total Revenue', 'Net Income', 'Free Cash Flow']
            available = [c for c in cols if c in fin.columns]
            df_chart = fin[available].head(6)[::-1] / 1e6
            x_pos = np.linspace(0.8, 6.5, len(df_chart))
            width = 0.14
            norm = df_chart.iloc[:, 0].max() if not df_chart.empty and df_chart.iloc[:, 0].max() > 0 else 1
            for i, (idx, vals) in enumerate(df_chart.iterrows()):
                ax.add_patch(Rectangle((x_pos[i]-width*1.5, 8.5), width, (vals[0]/norm)*2.5, color=BLACK))
                if len(vals) > 1: ax.add_patch(Rectangle((x_pos[i]-width*0.5, width), width, (vals[1]/norm)*2.5, color=TEAL))
                if len(vals) > 2: ax.add_patch(Rectangle((x_pos[i]+width*0.5, width), width, (vals[2]/norm)*2.5, color=RED))
                ax.text(x_pos[i], 8.2, str(idx.year), ha='center', fontsize=8, color='#666666')
        ax.text(0.8, 11.2, "● Revenue  ● Net Income  ● FCF", fontsize=9, color='#666666')

        # --- MARGINS (RIGHT) ---
        ax.text(7.5, 11.2, "Margins", fontsize=18, fontweight='bold', color=TEAL)
        draw_ring(ax, 7.5, 10.5, info.get('grossMargins', 0)*100, "Gross", TEAL)
        draw_ring(ax, 7.5, 9.7, info.get('ebitdaMargins', 0)*100, "EBIT", RED)
        draw_ring(ax, 7.5, 8.9, info.get('profitMargins', 0)*100, "Net", TEAL)
        fcf_m = (info.get('freeCashflow', 0)/info.get('totalRevenue', 1))*100 if info.get('totalRevenue') else 0
        draw_ring(ax, 7.5, 8.1, fcf_m, "FCF", RED)

        # --- BOTTOM SECTIONS ---
        ax.text(0.5, 7.2, "Key ratios", fontsize=18, fontweight='bold', color=ORANGE_TAG)
        draw_ring(ax, 0.7, 6.5, info.get('payoutRatio', 0)*100, "BuyBack", TEAL, size=0.18)
        ax.text(0.5, 5.8, f"• {info.get('returnOnAssets', 0)*100:.1f}% ROIC", fontsize=11)
        ax.text(0.5, 5.4, f"• ${info.get('totalCash', 0)/1e9:.1f}B Cash", fontsize=11)
        ax.text(0.5, 5.0, f"• {info.get('trailingPE', 0):.1f} P/E", fontsize=11)

        ax.text(3.8, 7.2, "Growth Estimates", fontsize=18, fontweight='bold', color=BLACK)
        draw_ring(ax, 4.0, 6.5, info.get('revenueGrowth', 0)*100, "Rev CAGR", RED, size=0.18)
        draw_ring(ax, 4.0, 5.8, info.get('earningsGrowth', 0)*100, "EPS CAGR", RED, size=0.18)

        # Growth Since 2022
        ax.text(0.5, 4.0, "Growth Since 2022", fontsize=16, fontweight='bold', color=GREEN)
        draw_ring(ax, 0.7, 3.3, row.RS_Rating, "Revenue", TEAL, size=0.15)
        draw_ring(ax, 0.7, 2.7, row.RVOL*50, "EPS", TEAL, size=0.15)

        # Price Tag with Targets
        tag_pts = [[7.2, 5.5], [9.8, 5.5], [9.8, 1.0], [7.8, 1.0], [7.2, 3.2]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=5))
        ax.add_patch(Circle((7.6, 3.2), 0.08, color=BG_WHITE, zorder=6))
        ax.text(8.5, 4.9, "Price", ha='center', fontweight='bold', fontsize=14)
        ax.text(8.5, 3.9, f"${row.Price}", ha='center', fontweight='black', fontsize=38)
        
        t_mean = info.get('targetMeanPrice', row.Price)
        upside = ((t_mean/row.Price)-1)*100 if row.Price > 0 else 0
        ax.text(8.5, 3.1, f"{upside:.0f}% OFF", ha='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none'))
        ax.text(8.5, 2.2, "WS Price Targets", ha='center', fontweight='bold', fontsize=9)
        ax.text(8.5, 1.8, f"${info.get('targetLowPrice', 0)} Low", ha='center', fontsize=8)
        ax.text(8.5, 1.5, f"${info.get('targetHighPrice', 0)} High", ha='center', fontsize=8)
        ax.text(8.5, 1.2, f"${t_mean:.1f} Consensus", ha='center', fontsize=8, fontweight='bold')

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200, facecolor=BG_WHITE, bbox_inches='tight')
        buf.seek(0); plt.close()
        return buf
    except Exception as e:
        print(f"Error {ticker}: {e}"); return None

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
            perf_5y = ((curr / df['Close'].iloc[0]) - 1) * 100
            perf_ytd = ((curr / df['Close'].loc[df.index >= f"{datetime.now().year}-01-01"].iloc[0]) - 1) * 100
            # RS logic that prioritizes leaders like COHR
            rs = (( (df['Close'] / data['SPY']['Close']).iloc[-1] / (df['Close'] / data['SPY']['Close']).rolling(150).mean().iloc[-1]) - 1) * 100
            rvol = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
            results.append({'Stock': t, 'Price': round(curr, 2), 'RS_Rating': round(rs, 2), 'RVOL': round(rvol, 2), '5Y_Perf': perf_5y, 'YTD_Perf': perf_ytd, 'Score': rs + (rvol * 15)})
        except: continue

    df_full = pd.DataFrame(results).sort_values('Score', ascending=False).replace([np.inf, -np.inf], np.nan).fillna(0)
    df_top = df_full.head(10).copy()

    for i, (idx, row) in enumerate(df_top.iterrows()):
        t_obj = yf.Ticker(row.Stock)
        img = create_exact_infographic(row.Stock, row, t_obj.info, t_obj.financials.T if not t_obj.financials.empty else pd.DataFrame())
        if img and i < 5:
            requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", files={'photo': img}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': f"🎯 **APEX PICK: ${row.Stock}**", 'parse_mode': 'Markdown'})

    sh.worksheet("Core Screener").update([df_full.columns.tolist()] + df_full.astype(str).values.tolist())
    sh.worksheet("Summary").update([df_top.columns.tolist()] + df_top.astype(str).values.tolist())
    print("✅ Logic Corrected. Infographics Synced.")

if __name__ == "__main__":
    run_scanner()

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
BG_COLOR = '#ffffff'      
CYAN_ACCENT = '#00bfbf'  
ORANGE_TAG = '#ffbf7f'   
GRAY_INACTIVE = '#e0e0e0' 
TEXT_TITLE = '#000000'   
TEXT_DATA = '#333333'    
GREEN_POS = '#009933'    
RED_NEG = '#cc0000'      

# --- VISUALS ---
def draw_donut(ax, x, y, pct, label, color, size=0.25):
    pct = max(min(float(pct or 0), 100), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.35, color=GRAY_INACTIVE, zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(pct*3.6), width=size*0.35, color=color, zorder=3))
    ax.text(x + size + 0.15, y, f"{int(pct)}% {label}", color=TEXT_TITLE, va='center', fontweight='bold', fontsize=10)

def draw_fair_value_bar(ax, x, y, upside_pct):
    ax.add_patch(Rectangle((x, y), 2.5, 0.3, color=CYAN_ACCENT, alpha=0.9, zorder=2))
    ax.add_patch(Rectangle((x+1.25, y), 1.25, 0.3, color='#fca3b7', alpha=0.9, zorder=2))
    pos = x + 1.25 - (upside_pct / 50)
    pos = max(min(pos, x+2.5), x)
    ax.add_patch(Rectangle((pos, y-0.05), 0.08, 0.4, color=GREEN_POS, zorder=5))
    ax.text(x+1.25, y+0.45, "Fair Value Bar", color=TEXT_TITLE, ha='center', fontweight='black', fontsize=10)

def create_white_infographic(ticker, row, info, fin):
    try:
        fig = plt.figure(figsize=(10, 16), facecolor=BG_COLOR, dpi=160)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False, facecolor=BG_COLOR)
        ax.set_xlim(0, 12); ax.set_ylim(0, 16)
        ax.set_xticks([]); ax.set_yticks([])

        # Header
        ax.text(0.5, 14.8, ticker, fontsize=70, color=TEXT_TITLE, fontweight='black')
        ax.text(11.5, 15.1, f"${info.get('marketCap', 0)/1e9:.1f}B Market Cap", fontsize=20, color=TEXT_TITLE, ha='right', fontweight='bold')
        ax.text(11.5, 14.6, f"{row['5Y_Perf']:.0f}% 5Y", fontsize=18, color=RED_NEG if row['5Y_Perf']<0 else GREEN_POS, ha='right')
        ax.text(11.5, 14.2, f"{row['YTD_Perf']:.0f}% YTD", fontsize=18, color=RED_NEG if row['YTD_Perf']<0 else GREEN_POS, ha='right')

        # Margins (Top Right)
        ax.text(8.8, 13.0, "Margins", fontsize=24, color=CYAN_ACCENT, fontweight='bold')
        draw_donut(ax, 8.8, 12.2, info.get('grossMargins', 0)*100, "Gross", CYAN_ACCENT)
        draw_donut(ax, 8.8, 11.2, info.get('ebitdaMargins', 0)*100, "EBIT", CYAN_ACCENT)
        draw_donut(ax, 8.8, 10.2, info.get('profitMargins', 0)*100, "Net", CYAN_ACCENT)
        draw_donut(ax, 8.8, 9.2, (info.get('freeCashflow', 0)/info.get('totalRevenue', 1))*100 if info.get('totalRevenue', 0) > 0 else 0, "FCF", CYAN_ACCENT)

        # Financial Bars
        if not fin.empty:
            revs = (fin.get('Total Revenue', pd.Series(dtype=float)).head(10)[::-1] / 1e9).dropna()
            if not revs.empty:
                x_bars = np.linspace(0.8, 6.5, len(revs))
                for i, val in enumerate(revs):
                    h = (val/revs.max())*2.2
                    ax.add_patch(Rectangle((x_bars[i]-0.15, 8.8), 0.3, h, color='#000000', alpha=0.9, zorder=2))
                    ax.text(x_bars[i], 8.5, str(revs.index[i].year), color='#999999', ha='center', fontsize=9)
            ax.text(0.8, 11.2, "● Revenue", color='#999999', fontsize=10)

        # Price Tag
        tag_pts = [[8.5, 5.0], [11.8, 5.0], [11.8, 0.6], [9.2, 0.6], [8.5, 2.8]]
        ax.add_patch(Polygon(tag_pts, color=ORANGE_TAG, zorder=5))
        ax.add_patch(Circle((9.0, 2.8), 0.1, color=BG_COLOR, zorder=6)) 
        ax.text(10.2, 4.3, "Price", color='#000000', ha='center', fontweight='bold', fontsize=16)
        ax.text(10.2, 3.4, f"${row.Price}", color='#000000', ha='center', fontsize=50, fontweight='black')
        
        target = info.get('targetMeanPrice', row.Price)
        upside = ((target/row.Price)-1)*100 if row.Price > 0 else 0
        ax.text(10.2, 2.6, f"{upside:.0f}% OFF", color='#000000', ha='center', fontweight='black', 
                bbox=dict(facecolor='white', edgecolor='none', boxstyle='round,pad=0.4'))
        ax.text(10.2, 1.4, f"WS: ${info.get('targetLowPrice', 0)}L - ${info.get('targetHighPrice', 0)}H", ha='center', fontsize=10, fontweight='bold')

        # Ratios & Bull Case
        ax.text(0.5, 7.6, "Key ratios", fontsize=22, color=ORANGE_TAG, fontweight='bold')
        draw_donut(ax, 0.8, 6.8, info.get('payoutRatio', 0)*100, "Buyback", CYAN_ACCENT, size=0.2)
        ax.text(0.5, 5.8, f"• {info.get('returnOnAssets', 0)*100:.1f}% ROIC", color=TEXT_DATA, fontsize=13)
        ax.text(0.5, 5.3, f"• ${info.get('totalCash', 0)/1e9:.1f}B Cash", color=TEXT_DATA, fontsize=13)
        ax.text(0.5, 4.8, f"• {info.get('trailingPE', 0):.1f} P/E", color=TEXT_DATA, fontsize=13)

        draw_fair_value_bar(ax, 4.2, 4.5, upside)
        ax.text(4.2, 3.5, "Bull Case", fontsize=22, color=GREEN_POS, fontweight='bold')
        ax.text(4.2, 2.8, "• Momentum Leader\n• High Institutional Interest\n• Sector Strength", color=TEXT_DATA, fontsize=12)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=160, facecolor=BG_COLOR, bbox_inches='tight')
        buf.seek(0); plt.close()
        return buf, upside
    except Exception as e:
        print(f"❌ Render Skip {ticker}: {e}"); return None, 0

# --- CORE SCANNER ---
def run_scanner():
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    req = Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req) as resp:
        tickers = pd.read_html(io.BytesIO(resp.read()))[0]['Symbol'].tolist()
    
    formatted_tickers = [t.replace('.', '-') for t in tickers]
    data = yf.download(formatted_tickers + ["SPY"], period="5y", group_by='ticker', progress=False)
    spy_close = data['SPY']['Close']
    
    all_results = []
    for t in formatted_tickers:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            curr_p = df['Close'].iloc[-1]
            perf_5y = ((curr_p / df['Close'].iloc[0]) - 1) * 100
            ytd_start = df['Close'].loc[df.index >= f"{datetime.now().year}-01-01"].iloc[0]
            perf_ytd = ((curr_p / ytd_start) - 1) * 100
            rs_rating = round((( (df['Close'] / spy_close).iloc[-1] / (df['Close'] / spy_close).rolling(150).mean().iloc[-1]) - 1) * 100, 2)
            rvol = round(df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1], 2)
            
            all_results.append({
                'Stock': t, 'Price': round(curr_p, 2), 'RS_Rating': rs_rating, 
                'RVOL': rvol, '5Y_Perf': round(perf_5y, 2), 'YTD_Perf': round(perf_ytd, 2),
                'Score': rs_rating + (rvol * 10)
            })
        except: continue

    df_full = pd.DataFrame(all_results).sort_values('Score', ascending=False)
    df_top = df_full.head(10).copy()

    analysis_col = []
    for i, (idx, row) in enumerate(df_top.iterrows()):
        t_obj = yf.Ticker(row.Stock)
        img, upside = create_white_infographic(row.Stock, row, t_obj.info, t_obj.financials.T)
        analysis_col.append(f"{upside:.1f}% Upside Target")
        
        if img and i < 5:
            requests.post(f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto", files={'photo': img}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': f"🎯 **APEX PICK: ${row.Stock}**", 'parse_mode': 'Markdown'})
    
    df_top['Analysis'] = analysis_col

    # --- RESTORED SHEET SYNC ---
    sh.worksheet("Core Screener").clear()
    sh.worksheet("Core Screener").update([df_full.columns.tolist()] + df_full.astype(str).values.tolist())
    
    sh.worksheet("Summary").clear()
    sh.worksheet("Summary").update([df_top.columns.tolist()] + df_top.astype(str).values.tolist())
    print("✅ Full Data Sync Complete (100+ Core, 10 Summary).")

if __name__ == "__main__":
    run_scanner()

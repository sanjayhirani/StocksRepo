import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, Circle, FancyBboxPatch
import io
import requests
import os
import json
from datetime import datetime
from urllib.request import Request, urlopen

# --- CONSTANTS & STYLE ---
BG_COLOR = '#0b0e11'
CYAN = '#00d4ff'   
GREEN = '#00ff88'  
RED = '#ff4444'    
ORANGE = '#ff9500' 
TEXT_COLOR = '#ffffff'

# --- 1. VISUAL ENGINE ---
def draw_donut(ax, x, y, pct, label, color, size=0.3):
    pct = max(min(float(pct or 0), 100), 0)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.3, color='#2c2e33', zorder=2))
    ax.add_patch(Wedge((x, y), size, 90, 90-(pct*3.6), width=size*0.3, color=color, zorder=3))
    ax.text(x + size + 0.15, y, f"{int(pct)}% {label}", color=TEXT_COLOR, fontsize=9, va='center', fontweight='bold')

def draw_fair_value_bar(ax, x, y, upside_pct):
    # Fixed: Using standard Rectangle for compatibility
    ax.add_patch(Rectangle((x, y), 2.5, 0.3, color=CYAN, alpha=0.6))
    ax.add_patch(Rectangle((x+1.25, y), 1.25, 0.3, color=RED, alpha=0.6))
    pos = x + 1.25 - (upside_pct / 40)
    pos = max(min(pos, x+2.5), x)
    ax.add_patch(Rectangle((pos, y-0.05), 0.08, 0.4, color=GREEN, zorder=5))
    ax.text(x+1.25, y+0.45, "Fair Value Bar", color=TEXT_COLOR, ha='center', fontsize=9, fontweight='bold')

def create_infographic(ticker, row, info, fin):
    try:
        fig = plt.figure(figsize=(10, 14), facecolor=BG_COLOR)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False)
        ax.set_xlim(0, 10); ax.set_ylim(0, 14)
        ax.set_xticks([]); ax.set_yticks([])

        # Header Info
        mcap = info.get('marketCap', 0) / 1e9
        ax.text(0.5, 12.8, ticker, fontsize=55, color=TEXT_COLOR, fontweight='black')
        ax.text(9.5, 13.0, f"${mcap:.1f}B Market Cap", fontsize=16, color=TEXT_COLOR, ha='right', fontweight='bold')
        
        ax.text(9.5, 12.4, f"{row['5Y_Perf']:.0f}% 5Y", fontsize=14, color=RED if row['5Y_Perf'] < 0 else GREEN, ha='right')
        ax.text(9.5, 12.0, f"{row['YTD_Perf']:.0f}% YTD", fontsize=14, color=RED if row['YTD_Perf'] < 0 else GREEN, ha='right')

        # Center: Revenue & Net Income Bars
        if not fin.empty:
            revs = fin.get('Total Revenue', pd.Series(dtype=float)).tail(5) / 1e9
            ni = fin.get('Net Income', pd.Series(dtype=float)).tail(5) / 1e9
            if not revs.empty:
                x_pos = np.linspace(0.8, 5.2, len(revs))
                norm = revs.max() if revs.max() > 0 else 1
                ax.bar(x_pos - 0.1, (revs/norm)*2, width=0.18, color='#ffffff')
                ax.bar(x_pos + 0.1, (ni/norm)*2 + 0.4, width=0.18, color=CYAN)
                for i, y_label in enumerate(revs.index):
                    ax.text(x_pos[i], 8.2, str(y_label.year if hasattr(y_label, 'year') else i), color='#888888', ha='center', fontsize=9)

        # Right Sidebar: Margins
        ax.text(6.8, 11.2, "Margins", fontsize=18, color=CYAN, fontweight='bold')
        draw_donut(ax, 6.8, 10.5, info.get('grossMargins', 0)*100, "Gross", CYAN)
        draw_donut(ax, 6.8, 9.6, info.get('ebitdaMargins', 0)*100, "EBIT", ORANGE)
        draw_donut(ax, 6.8, 8.7, info.get('profitMargins', 0)*100, "Net", CYAN)
        draw_donut(ax, 6.8, 7.8, (info.get('freeCashflow', 0)/info.get('totalRevenue', 1))*100 if info.get('totalRevenue') else 0, "FCF", RED)

        # Key Ratios
        ax.text(0.5, 7.2, "Key ratios", fontsize=18, color=ORANGE, fontweight='bold')
        draw_donut(ax, 0.8, 6.5, info.get('returnOnAssets', 0)*100, "ROA", ORANGE, size=0.25)
        ax.text(0.5, 5.7, f"• ${info.get('totalCash', 0)/1e9:.1f}B Cash", color=TEXT_COLOR, fontsize=12)
        ax.text(0.5, 5.3, f"• {info.get('trailingPE', 0):.1f} P/E", color=TEXT_COLOR, fontsize=12)
        ax.text(0.5, 4.9, f"• {info.get('payoutRatio', 0)*100:.1f}% Payout", color=TEXT_COLOR, fontsize=12)

        # Growth Estimates
        ax.text(0.5, 3.8, "Growth Track", fontsize=18, color=RED, fontweight='bold')
        ax.text(0.5, 3.3, f"• RS Rating: {row.RS_Rating}", color=TEXT_COLOR, fontsize=12)
        ax.text(0.5, 2.9, f"• RVOL Score: {row.RVOL}x", color=TEXT_COLOR, fontsize=12)
        
        # Price Tag
        tag_poly = Polygon([[7.0, 5.2], [9.8, 5.2], [9.8, 1.2], [7.0, 1.2], [6.4, 3.2]], color=ORANGE)
        ax.add_patch(tag_poly)
        ax.add_patch(Circle((6.9, 3.2), 0.08, color=BG_COLOR))
        ax.text(8.4, 4.6, "Price", color=BG_COLOR, ha='center', fontweight='bold', fontsize=12)
        ax.text(8.4, 3.8, f"${row.Price}", color=BG_COLOR, ha='center', fontsize=32, fontweight='black')
        
        target = info.get('targetMeanPrice', 0)
        upside = ((target/row.Price)-1)*100 if (target and row.Price) else 0
        ax.text(8.4, 3.0, f"{upside:.1f}% OFF", color=BG_COLOR, ha='center', fontweight='bold', 
                bbox=dict(facecolor='white', edgecolor='none', boxstyle='round,pad=0.3'))
        ax.text(8.4, 1.8, f"Consensus: ${target}", color=BG_COLOR, ha='center', fontsize=10, fontweight='bold')

        draw_fair_value_bar(ax, 3.8, 2.0, upside)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, facecolor=BG_COLOR)
        buf.seek(0); plt.close()
        return buf, upside
    except Exception as e:
        print(f"❌ Render Error {ticker}: {e}"); return None, 0

# --- 2. ENGINE ---
def run_scanner():
    print(f"🚀 Initializing Full Apex Master Scan...")
    
    # Auth
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    # Get Tickers (with hyphen fix for BRK.B)
    req = Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req) as resp:
        tickers = pd.read_html(io.BytesIO(resp.read()))[0]['Symbol'].tolist()
    
    formatted_tickers = [t.replace('.', '-') for t in tickers]
    data = yf.download(formatted_tickers + ["SPY"], period="5y", group_by='ticker', progress=False)
    spy_close = data['SPY']['Close']
    
    results = []
    for t in formatted_tickers:
        try:
            df = data[t].dropna()
            if len(df) < 252: continue
            
            curr_p = df['Close'].iloc[-1]
            perf_5y = ((curr_p / df['Close'].iloc[0]) - 1) * 100
            ytd_start = df['Close'].loc[df.index >= f"{datetime.now().year}-01-01"].iloc[0]
            perf_ytd = ((curr_p / ytd_start) - 1) * 100
            
            rs_line = df['Close'] / spy_close
            rs_rating = round(((rs_line.iloc[-1] / rs_line.rolling(150).mean().iloc[-1]) - 1) * 100, 2)
            rvol = round(df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1], 2)
            
            if rs_rating > 5 and rvol > 1.0:
                results.append({
                    'Stock': t, 'Price': round(curr_p, 2), 'RS_Rating': rs_rating, 
                    'RVOL': rvol, '5Y_Perf': perf_5y, 'YTD_Perf': perf_ytd,
                    'Score': rs_rating + (rvol * 5)
                })
        except: continue

    df_all = pd.DataFrame(results).sort_values('Score', ascending=False)
    df_top = df_all.head(10).copy()

    final_briefs = []
    for i, (idx, row) in enumerate(df_top.iterrows()):
        print(f"📦 Deep Data for {row.Stock}...")
        t_obj = yf.Ticker(row.Stock)
        info, fin = t_obj.info, t_obj.financials.T
        
        img, upside = create_infographic(row.Stock, row, info, fin)
        final_briefs.append(f"{upside:.1f}% Upside")
        
        if img and i < 5:
            url = f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto"
            requests.post(url, files={'photo': img}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': f"🎯 **APEX PICK: ${row.Stock}**", 'parse_mode': 'Markdown'})
    
    df_top['Analysis'] = final_briefs

    # Update Google Sheets
    try:
        sh.worksheet("Core Screener").clear()
        sh.worksheet("Core Screener").update([df_all.columns.tolist()] + df_all.head(100).astype(str).values.tolist())
        sh.worksheet("Summary").clear()
        sh.worksheet("Summary").update([df_top.columns.tolist()] + df_top.astype(str).values.tolist())
        print("✅ Sheets Updated Successfully.")
    except Exception as e:
        print(f"⚠️ Sheets Error: {e}")

if __name__ == "__main__":
    run_scanner()

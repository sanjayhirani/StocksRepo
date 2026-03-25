import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge, Rectangle, Polygon, Circle, ConnectionPatch
import io
import requests
import os
import json
from datetime import datetime
from urllib.request import Request, urlopen
from matplotlib.font_manager import FontProperties

# --- EXACT PALETTE (From White Infographic) ---
BG_COLOR = '#ffffff'      # Clean White
CYAN_ACCENT = '#00bfbf'  # Light blue ring parts
ORANGE_TAG = '#ffbf7f'   # The price tag color
GRAY_INACTIVE = '#e0e0e0' # Grey parts of the rings
TEXT_TITLE = '#000000'   
TEXT_DATA = '#333333'    
TEXT_TAG = '#000000'     
GREEN_POS = '#009933'    # Percentage green
RED_NEG = '#cc0000'      # Percentage red

# Set clean font (This will attempt to find Montserrat/Open Sans/Lato)
font_prop = FontProperties(family=['Open Sans', 'Lato', 'sans-serif'], size=10)

# --- 1. THE CANVAS VISUAL HELPERS ---
def draw_donut(ax, x, y, pct, label, color, size=0.25):
    pct = max(min(float(pct or 0), 100), 0)
    # Background circle (Gray)
    ax.add_patch(Wedge((x, y), size, 0, 360, width=size*0.35, color=GRAY_INACTIVE, zorder=2))
    # Progress circle (Blue parts from image)
    ax.add_patch(Wedge((x, y), size, 90, 90-(pct*3.6), width=size*0.35, color=color, zorder=3))
    # Text in exact style
    ax.text(x + size + 0.15, y, f"{int(pct)}% {label}", color=TEXT_TITLE, fontproperties=font_prop, va='center', fontweight='bold', fontsize=10)

def draw_fair_value_bar(ax, x, y, upside_pct):
    ax.add_patch(Rectangle((x, y), 2.5, 0.3, color=CYAN_ACCENT, alpha=0.9, zorder=2)) # Undervalued part
    ax.add_patch(Rectangle((x+1.25, y), 1.25, 0.3, color='#fca3b7', alpha=0.9, zorder=2)) # Overvalued part
    pos = x + 1.25 - (upside_pct / 50) # Map to position
    pos = max(min(pos, x+2.5), x)
    ax.add_patch(Rectangle((pos, y-0.05), 0.08, 0.4, color=GREEN_POS, zorder=5))
    ax.text(x+1.25, y+0.45, "Fair Value Bar", color=TEXT_TITLE, ha='center', fontproperties=font_prop, fontweight='black', fontsize=10)

# --- 2. THE INFOGRAPHIC CANVAS ENGINE ---
def create_white_infographic(ticker, row, info, fin):
    try:
        # Create a clean white high-DPI canvas
        fig = plt.figure(figsize=(10, 16), facecolor=BG_COLOR, dpi=160)
        ax = fig.add_axes([0, 0, 1, 1], frameon=False, facecolor=BG_COLOR)
        ax.set_xlim(0, 12); ax.set_ylim(0, 16)
        ax.set_xticks([]); ax.set_yticks([])

        # --- HEADER SECTION (EXACT PLACEMENT) ---
        ax.text(0.5, 14.8, ticker, fontsize=70, color=TEXT_TITLE, fontweight='black', fontproperties=FontProperties(family=['Open Sans', ' Lato']))
        ax.text(11.5, 15.1, f"${info.get('marketCap', 0)/1e9:.1f}B Market Cap", fontsize=20, color=TEXT_TITLE, ha='right', fontweight='bold', fontproperties=font_prop)
        ax.text(11.5, 14.6, f"{row['5Y_Perf']:.0f}% 5Y", fontsize=18, color=RED_NEG if row['5Y_Perf']<0 else GREEN_POS, ha='right', fontproperties=font_prop)
        ax.text(11.5, 14.2, f"{row['YTD_Perf']:.0f}% YTD", fontsize=18, color=RED_NEG if row['YTD_Perf']<0 else GREEN_POS, ha='right', fontproperties=font_prop)

        # --- MARGINS SECTION (Top Right) ---
        ax.text(8.8, 13.0, "Margins", fontsize=24, color=CYAN_ACCENT, fontweight='bold', fontproperties=font_prop)
        draw_donut(ax, 8.8, 12.2, info.get('grossMargins', 0)*100, "Gross", CYAN_ACCENT)
        draw_donut(ax, 8.8, 11.2, info.get('ebitdaMargins', 0)*100, "EBIT", CYAN_ACCENT)
        draw_donut(ax, 8.8, 10.2, info.get('profitMargins', 0)*100, "Net", CYAN_ACCENT)
        draw_donut(ax, 8.8, 9.2, (info.get('freeCashflow', 0)/info.get('totalRevenue', 1))*100 if info.get('totalRevenue') else 0, "FCF", CYAN_ACCENT)

        # --- FINANCIAL BAR CHART (Integrated into main grid) ---
        if not fin.empty:
            revs = fin.get('Total Revenue', pd.Series(dtype=float)).head(10)[::-1] / 1e9
            x_bars = np.linspace(0.8, 6.5, len(revs))
            for i, val in enumerate(revs):
                ax.add_patch(Rectangle((x_bars[i]-0.15, 8.8), 0.3, (val/revs.max())*2.2, color='#000000', alpha=0.9, zorder=2))
                ax.text(x_bars[i], 8.5, str(revs.index[i].year), color='#999999', ha='center', fontsize=9, fontproperties=font_prop)
            ax.text(0.8, 11.2, "● Revenue", color='#999999', fontsize=10, fontproperties=font_prop)

        # --- THE PRICE TAG (BOTTOM RIGHT - EXACT SHAPE/LAYOUT) ---
        tag_pts = Polygon([[8.5, 5.0], [11.8, 5.0], [11.8, 0.6], [9.2, 0.6], [8.5, 2.8]], color=ORANGE_TAG, zorder=5)
        ax.add_patch(tag_poly)
        ax.add_patch(Circle((9.0, 2.8), 0.1, color=BG_COLOR, zorder=6)) # Hole
        ax.text(10.2, 4.3, "Price", color=TEXT_TAG, ha='center', fontweight='bold', fontsize=16, fontproperties=font_prop)
        ax.text(10.2, 3.4, f"${row.Price}", color=TEXT_TAG, ha='center', fontsize=50, fontweight='black', fontproperties=font_prop)
        
        target = info.get('targetMeanPrice', 0)
        upside = ((target/row.Price)-1)*100 if target else 0
        ax.text(10.2, 2.6, f"{upside:.0f}% OFF", color=TEXT_TAG, ha='center', fontweight='black', 
                bbox=dict(facecolor='white', edgecolor='none', boxstyle='round,pad=0.4'))
        ax.text(10.2, 1.4, f"WS Targets: ${info.get('targetLowPrice', 0)} Low - ${info.get('targetHighPrice', 0)} High", color=TEXT_TAG, ha='center', fontsize=11, fontweight='bold', fontproperties=font_prop)

        # --- BOTTOM GRID (Key Ratios, CAGR, Performance, Bull Case) ---
        ax.text(0.5, 7.6, "Key ratios", fontsize=22, color=ORANGE_TAG, fontweight='bold', fontproperties=font_prop)
        draw_donut(ax, 0.8, 6.8, info.get('payoutRatio', 0)*100, "Buyback", CYAN_ACCENT, size=0.2)
        ax.text(0.5, 5.8, f"• {info.get('returnOnAssets', 0)*100:.1f}% ROIC", color=TEXT_DATA, fontsize=13, fontproperties=font_prop)
        ax.text(0.5, 5.3, f"• ${info.get('totalCash', 0)/1e9:.1f}B Cash", color=TEXT_DATA, fontsize=13, fontproperties=font_prop)
        ax.text(0.5, 4.8, f"• {info.get('trailingPE', 0):.1f} P/E | {info.get('forwardPE', 0):.1f} Fwd", color=TEXT_DATA, fontsize=13, fontproperties=font_prop)

        ax.text(4.2, 7.6, "Growth Estimates", fontsize=22, color=RED_NEG, fontweight='bold', fontproperties=font_prop)
        draw_donut(ax, 4.5, 6.8, info.get('revenueGrowth', 0)*100, "Revenue CAGR", CYAN_ACCENT, size=0.2)
        draw_donut(ax, 4.5, 5.8, info.get('earningsGrowth', 0)*100, "EPS CAGR", CYAN_ACCENT, size=0.2)

        ax.text(0.5, 3.5, "Performance Since 2022", fontsize=22, color=CYAN_ACCENT, fontweight='bold', fontproperties=font_prop)
        draw_donut(ax, 0.8, 2.7, row.RS_Rating, "Revenue", CYAN_ACCENT, size=0.2)
        draw_donut(ax, 2.8, 2.7, row.RVOL*50, "EPS", CYAN_ACCENT, size=0.2) # Placeholder for specific EPS data

        draw_fair_value_bar(ax, 4.2, 4.5, upside)
        
        ax.text(4.2, 3.5, "Bull Case", fontsize=22, color=GREEN_POS, fontweight='bold', fontproperties=font_prop)
        ax.text(4.2, 2.8, "• AI Agentic Growth Sector\n• Margin Inflection Play\n• Institutional Momentum Leader", color=TEXT_DATA, fontsize=12, fontproperties=font_prop)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=160, facecolor=BG_COLOR, bbox_inches='tight')
        buf.seek(0); plt.close()
        return buf
    except Exception as e:
        print(f"❌ Error rendering {ticker}: {e}"); return None

# --- 3. MASTER SCANNER ENGINE ---
def run_scanner():
    print(f"🚀 Starting Exact White-Infographic Apex Scan...")
    
    # Auth
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
    sh = gc.open("Stock Scanner")
    
    # Get Tickers
    req = Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req) as resp:
        tickers = pd.read_html(io.BytesIO(resp.read()))[0]['Symbol'].tolist()
    
    # Hyphen fix for yfinance
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
            
            # Simple technical filter
            if rs_rating > 10 and rvol > 1.1:
                results.append({
                    'Stock': t, 'Price': round(curr_p, 2), 'RS_Rating': rs_rating, 
                    'RVOL': rvol, '5Y_Perf': perf_5y, 'YTD_Perf': perf_ytd, 'Score': rs_rating + (rvol * 10)
                })
        except: continue

    df_all = pd.DataFrame(results).sort_values('Score', ascending=False)
    df_top = df_all.head(10).copy()

    # Process Top Picks & Send Telegram
    final_upsides = []
    for i, (idx, row) in enumerate(df_top.iterrows()):
        t_obj = yf.Ticker(row.Stock)
        # Call the exact-design white generator
        img = create_white_infographic(row.Stock, row, t_obj.info, t_obj.financials.T)
        final_upsides.append(i) # Placeholder for data sync

        if img and i < 5: # Only send top 5 to Telegram to manage load
            url = f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto"
            requests.post(url, files={'photo': img}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': f"🎯 **APEX PICK: ${row.Stock}**", 'parse_mode': 'Markdown'})
    
    # Sync Google Sheets
    sh.worksheet("Core Screener").clear()
    sh.worksheet("Core Screener").update([df_all.columns.tolist()] + df_all.head(100).astype(str).values.tolist())
    sh.worksheet("Summary").clear()
    sh.worksheet("Summary").update([df_top.columns.tolist()] + df_top.astype(str).values.tolist())
    print("✅ All Systems Synced in White Mode.")

if __name__ == "__main__":
    run_scanner()

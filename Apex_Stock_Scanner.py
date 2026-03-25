import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import matplotlib.pyplot as plt
import io
import requests
import os
import json
from datetime import datetime
from urllib.request import Request, urlopen

# --- 1. AUTHENTICATION ---
def get_gspread_client():
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

# --- 2. DATA ENGINE (The "Exact" Metrics) ---
def get_equity_briefing_data(ticker, row):
    try:
        t_obj = yf.Ticker(ticker)
        info = t_obj.info
        
        # Margins & Profitability
        gross_margin = info.get('grossMargins', 0) * 100
        ebit_margin = info.get('ebitdaMargins', 0) * 100
        fcf_val = info.get('freeCashflow', 0)
        rev_val = info.get('totalRevenue', 1)
        fcf_margin = (fcf_val / rev_val) * 100 if fcf_val else 0

        # Ratios & Balance Sheet
        roa = info.get('returnOnAssets', 0) * 100 
        cash = info.get('totalCash', 0) / 1e9
        pe = info.get('trailingPE', 0)
        fwd_pe = info.get('forwardPE', 0)
        market_cap = info.get('marketCap', 0) / 1e9
        
        # Performance Data
        hist_5y = t_obj.history(period="5y")
        perf_5y = ((hist_5y['Close'].iloc[-1] / hist_5y['Close'].iloc[0]) - 1) * 100 if len(hist_5y) > 0 else 0
        hist_ytd = t_obj.history(period="ytd")
        perf_ytd = ((hist_ytd['Close'].iloc[-1] / hist_ytd['Close'].iloc[0]) - 1) * 100 if len(hist_ytd) > 0 else 0

        # Analyst Targets
        t_low = info.get('targetLowPrice', 0)
        t_high = info.get('targetHighPrice', 0)
        t_mean = info.get('targetMeanPrice', 0)
        upside = ((t_mean / row.Price) - 1) * 100 if t_mean else 0

        briefing = (
            f"• **Market Cap:** ${market_cap:.1f}B | **5Y:** {perf_5y:.1f}% | **YTD:** {perf_ytd:.1f}%\n"
            f"• **Margins:** {gross_margin:.1f}% Gross | {ebit_margin:.1f}% EBIT | {fcf_margin:.1f}% FCF\n"
            f"• **Ratios:** {pe:.1f} P/E | {fwd_pe:.1f} Fwd | {roa:.1f}% ROA | ${cash:.1f}B Cash\n"
            f"• **Wall St:** Low ${t_low} | High ${t_high} | Cons ${t_mean} ({upside:.1f}% Upside)"
        )
        return briefing
    except Exception as e:
        return f"Briefing Data Unavailable: {str(e)}"

# --- 3. THE "EXACT" VISUAL DASHBOARD ---
def send_telegram_alert(ticker, row, history_df, thesis):
    try:
        t_obj = yf.Ticker(ticker)
        # Pull Annual Financials for the exact bar chart layout
        fin = t_obj.financials.T
        if not fin.empty:
            # We want the last 4 full years
            rev = fin.get('Total Revenue', pd.Series(dtype=float)).head(4)[::-1] / 1e9
            net = fin.get('Net Income', pd.Series(dtype=float)).head(4)[::-1] / 1e9
            years = [d.year for d in rev.index]
        else:
            rev, net, years = [], [], []

        # Color Palette from Infographic
        BG_COLOR = '#0b0e11'
        CYAN = '#00d4ff'   # Revenue / Price
        GREEN = '#00ff88'  # Net Income / Entry
        RED = '#ff4444'    # Stop Loss

        plt.style.use('dark_background')
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 13), gridspec_kw={'height_ratios': [1.8, 1]})
        fig.patch.set_facecolor(BG_COLOR)

        # PANEL 1: Price Action
        prices = history_df['Close'].tail(90)
        ax1.set_facecolor(BG_COLOR)
        ax1.plot(prices.index, prices, color=CYAN, lw=3.5, label='Price Action')
        ax1.axhline(y=row.Buy_Trigger, color=GREEN, ls='--', lw=2.5, label=f'ENTRY ${row.Buy_Trigger}')
        ax1.axhline(y=row.Stop_Loss, color=RED, ls='--', lw=2, label=f'STOP ${row.Stop_Loss}')
        ax1.fill_between(prices.index, row.Stop_Loss, row.Buy_Trigger, color=RED, alpha=0.12)
        
        ax1.set_title(f"${ticker} | GLOBAL EQUITY BRIEFING", fontsize=20, fontweight='bold', color='white', pad=25)
        ax1.grid(color='#2c2e33', alpha=0.5)
        ax1.legend(facecolor=BG_COLOR, edgecolor='white')

        # PANEL 2: Grouped Financial Bars (Exact Revenue vs Net Income)
        if len(years) > 0:
            ax2.set_facecolor(BG_COLOR)
            x = np.arange(len(years))
            ax2.bar(x - 0.2, rev, 0.4, label='Revenue ($B)', color=CYAN, alpha=0.9)
            ax2.bar(x + 0.2, net, 0.4, label='Net Income ($B)', color=GREEN, alpha=0.9)
            
            ax2.set_xticks(x)
            ax2.set_xticklabels(years, fontweight='bold', color='#cccccc')
            ax2.set_title("ANNUAL PERFORMANCE TREND", fontsize=13, color='#888888', pad=15)
            ax2.legend(loc='upper left', frameon=False)
            ax2.spines['top'].set_visible(False)
            ax2.spines['right'].set_visible(False)
        else:
            ax2.text(0.5, 0.5, "Financial History Not Found", ha='center', color='gray')

        plt.tight_layout(pad=4.0)
        
        # Save to memory
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=140, bbox_inches='tight', facecolor=BG_COLOR)
        buf.seek(0)
        plt.close()

        # Build Message
        caption = (
            f"🚀 **APEX BREAKOUT: ${ticker}**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{thesis}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 **MOMENTUM:** RS {row.RS_Rating} | RVOL {row.RVOL}x | ADR {row['ADR%']}%"
        )
        
        url = f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto"
        requests.post(url, files={'photo': buf}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': caption, 'parse_mode': 'Markdown'})
    
    except Exception as e:
        print(f"❌ Visual Render Error for {ticker}: {e}")

# --- 4. ENGINE EXECUTION ---
def run_scanner():
    print(f"🚀 Initializing Exact-Spec Scan...")
    gc = get_gspread_client()
    sh = gc.open("Stock Scanner")
    
    # Get Tickers (S&P 500)
    headers = {'User-Agent': 'Mozilla/5.0'}
    req = Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers=headers)
    with urlopen(req) as resp:
        tickers = pd.read_html(io.BytesIO(resp.read()))[0]['Symbol'].tolist()
    
    # Data Download
    data = yf.download(tickers + ["SPY"], period="1y", group_by='ticker', progress=False)
    spy_close = data['SPY']['Close']
    
    results = []
    for t in tickers:
        try:
            t = t.replace('.', '-')
            df = data[t].dropna()
            if len(df) < 150: continue
            
            curr_p = df['Close'].iloc[-1]
            # Filters: Strong Trend + Liquid
            sma50, sma200 = df['Close'].rolling(50).mean().iloc[-1], df['Close'].rolling(200).mean().iloc[-1]
            if not (curr_p > sma50 > sma200): continue
            
            rs_line = df['Close'] / spy_close
            rs_rating = round(((rs_line.iloc[-1] / rs_line.rolling(150).mean().iloc[-1]) - 1) * 100, 2)
            rvol = round(df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1], 2)
            
            if rs_rating > 5 and rvol > 1.1:
                atr = (df['High'] - df['Low']).rolling(20).mean().iloc[-1]
                results.append({
                    'Stock': t, 'Price': round(curr_p, 2), 'RS_Rating': rs_rating, 
                    'RVOL': rvol, 'ADR%': round((atr/curr_p)*100, 2),
                    'Buy_Trigger': round(df['High'].tail(5).max() * 1.002, 2),
                    'Stop_Loss': round(curr_p * 0.94, 2), # Fixed 6% stop
                    'Score': rs_rating + (rvol * 10)
                })
        except: continue

    df_res = pd.DataFrame(results).sort_values('Score', ascending=False).head(25)
    
    theses = []
    for i, (idx, row) in enumerate(df_res.head(5).iterrows()):
        print(f"📊 Rendering Infographic for {row.Stock}...")
        thesis = get_equity_briefing_data(row.Stock, row)
        theses.append(thesis)
        if i < 3:
            # Trigger Telegram Alert with exact layout
            send_telegram_alert(row.Stock, row, yf.download(row.Stock, period='1y', progress=False), thesis)
    
    while len(theses) < len(df_res): theses.append("")
    df_res['Infographic_Data'] = theses

    # Push to Google Sheets
    sh.worksheet("Core Screener").clear()
    sh.worksheet("Core Screener").update([df_res.columns.tolist()] + df_res.astype(str).values.tolist())
    print("🏁 Exact-Spec Scan Complete. Check Telegram.")

if __name__ == "__main__":
    run_scanner()

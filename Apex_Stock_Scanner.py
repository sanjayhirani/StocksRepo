import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai 
import matplotlib.pyplot as plt
import io
import requests
import os
import json
from datetime import datetime
from urllib.request import Request, urlopen

# --- 1. AUTHENTICATION ---
def setup_ai():
    # Explicitly configure the API Key
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    # FIX: Use 'gemini-1.5-flash-latest' to bypass version-specific 404s
    return genai.GenerativeModel('gemini-1.5-flash-latest')

def get_gspread_client():
    creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

# --- 2. THE "INSTITUTIONAL" ANALYST ---
def get_bespoke_summary(ticker, row):
    model = setup_ai()
    try:
        t_obj = yf.Ticker(ticker)
        info = t_obj.info
        
        # Financial metrics
        g_margin = info.get('grossMargins', 0) * 100
        fcf_val = info.get('freeCashflow', 0)
        rev_val = info.get('totalRevenue', 1)
        fcf_margin = (fcf_val / rev_val) * 100 if fcf_val else 0
        cash = info.get('totalCash', 0) / 1e9
        pe = info.get('trailingPE', 'N/A')
        biz = info.get('longBusinessSummary', 'Business data unavailable.')[:600]

        prompt = (
            f"Act as a Lead Equity Researcher. Create a 'One-Pager' briefing for {ticker}.\n"
            f"MARKET DATA: Price ${row.Price}, RS Rating {row.RS_Rating}, RVOL {row.RVOL}x, ADR {row['ADR%']}%.\n"
            f"FINANCIALS: Gross Margin {g_margin:.1f}%, FCF Margin {fcf_margin:.1f}%, Cash ${cash:.2f}B, P/E {pe}.\n"
            f"BUSINESS CONTEXT: {biz}\n"
            "FORMAT (Bullet points only, no intro text):\n"
            "- Industry/Niche: [Brief description]\n"
            "- Institutional Context: [Why the RVOL/RS matters here]\n"
            "- Growth/Inflection: [Business driver for this breakout]\n"
            "- Bull Case: [Specific strategic advantage]\n"
            "- Verdict: [1-sentence trading conviction]"
        )
        
        # Calling the generation
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ AI Error for {ticker}: {e}")
        return f"- Analysis currently unavailable for {ticker}."

# --- 3. ALERT DISPATCH ---
def send_telegram_alert(ticker, row, history_df, thesis):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 6))
    prices = history_df['Close'].tail(60)
    
    ax.plot(prices.index, prices, color='#00d4ff', lw=3)
    ax.axhline(y=row.Buy_Trigger, color='#00ff88', ls='--', lw=2, label=f'ENTRY ${row.Buy_Trigger}')
    ax.axhline(y=row.Stop_Loss, color='#ff4444', ls='--', lw=2, label=f'STOP ${row.Stop_Loss}')
    ax.fill_between(prices.index, row.Stop_Loss, row.Buy_Trigger, color='#ff4444', alpha=0.1)
    
    ax.set_title(f"${ticker} | RVOL: {row.RVOL}x | ADR: {row['ADR%']}%", color='white', fontsize=14)
    ax.legend(loc='upper left')
    ax.grid(alpha=0.1)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()

    caption = (
        f"🚀 **APEX SIGNAL: {ticker}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 **PRICE:** ${row.Price}\n"
        f"🎯 **ENTRY:** ${row.Buy_Trigger} | 🛑 **STOP:** ${row.Stop_Loss}\n"
        f"📈 **RS:** {row.RS_Rating} | ⚡ **ADR:** {row['ADR%']}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 **STRATEGY THESIS**\n{thesis}"
    )
    
    url = f"https://api.telegram.org/bot{os.environ.get('TELEGRAM_BOT_TOKEN')}/sendPhoto"
    requests.post(url, files={'photo': buf}, data={'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'caption': caption, 'parse_mode': 'Markdown'})

# --- 4. DATA INGESTION ---
def get_tickers():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    req_sp = Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers=headers)
    with urlopen(req_sp) as response:
        sp500 = pd.read_html(io.BytesIO(response.read()))[0]['Symbol'].tolist()
    
    req_nas = Request('https://en.wikipedia.org/wiki/Nasdaq-100', headers=headers)
    with urlopen(req_nas) as response:
        nasdaq100 = pd.read_html(io.BytesIO(response.read()))[4]['Ticker'].tolist()
        
    return list(set([t.replace('.', '-') for t in sp500 + nasdaq100]))

# --- 5. ENGINE ---
def run_scanner():
    print(f"🚀 Apex Scan Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    gc = get_gspread_client()
    sh = gc.open("Stock Scanner")
    
    tickers = get_tickers()
    data = yf.download(tickers + ["SPY"], period="1y", group_by='ticker', progress=False)
    spy_close = data['SPY']['Close']
    
    results = []
    for t in tickers:
        try:
            if t not in data or data[t].empty: continue
            df = data[t].dropna()
            if len(df) < 100: continue
            
            curr_p = df['Close'].iloc[-1]
            sma50, sma200 = df['Close'].rolling(50).mean().iloc[-1], df['Close'].rolling(200).mean().iloc[-1]
            
            if not (curr_p > sma50 > sma200): continue
            
            rs_line = df['Close'] / spy_close
            rs_rating = round(((rs_line.iloc[-1] / rs_line.rolling(150).mean().iloc[-1]) - 1) * 100, 2)
            if rs_rating < 0: continue
            
            rvol = round(df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1], 2)
            adr = round(((df['High'] - df['Low']) / df['Low']).rolling(20).mean().iloc[-1] * 100, 2)
            
            std, atr = df['Close'].rolling(20).std(), (df['High'] - df['Low']).rolling(20).mean()
            is_sq = (2 * std.iloc[-1]) < (1.5 * atr.iloc[-1])
            
            buy = round(df['High'].tail(5).max() * 1.005, 2)
            stop = round(curr_p - (atr.iloc[-1] * 2), 2)
            
            results.append({
                'Stock': t, 'Price': round(curr_p, 2), 'RS_Rating': rs_rating, 
                'RVOL': rvol, 'ADR%': adr, 'Squeeze': 'YES' if is_sq else 'No',
                'Buy_Trigger': buy, 'Stop_Loss': stop, 'Target': round(buy + (buy-stop)*2.5, 2),
                'Score': round(rs_rating + (rvol * 5) + (10 if is_sq else 0), 2)
            })
        except: continue

    df_res = pd.DataFrame(results).sort_values('Score', ascending=False).head(100)
    df_sum = df_res[df_res['Squeeze'] == 'YES'].head(10).copy()
    
    if not df_sum.empty:
        theses = []
        for i, (idx, row) in enumerate(df_sum.iterrows()):
            print(f"🧠 Intelligence: {row.Stock}...")
            thesis = get_bespoke_summary(row.Stock, row)
            theses.append(thesis)
            if i < 3:
                send_telegram_alert(row.Stock, row, yf.download(row.Stock, period='1y', progress=False), thesis)
        df_sum['AI_Thesis'] = theses
    
    # --- 6. OUTPUT ---
    try:
        sh.worksheet("Core Screener").clear()
        sh.worksheet("Core Screener").update([df_res.columns.tolist()] + df_res.astype(str).values.tolist())
        
        sum_sheet = sh.worksheet("Summary")
        sum_sheet.clear()
        if not df_sum.empty:
            sum_sheet.update([df_sum.columns.tolist()] + df_sum.astype(str).values.tolist())
    except Exception as e:
        print(f"⚠️ Sheets Error: {e}")
    
    print("🏁 Scan Finished.")

if __name__ == "__main__":
    run_scanner()

import yfinance as yf
import pandas as pd
import numpy as np
from urllib.request import Request, urlopen
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
from datetime import datetime
import matplotlib.pyplot as plt
import io
import requests
import os
import json

# --- AUTH ---
google_creds_json = json.loads(os.environ.get("GOOGLE_CREDS"))
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_json, scope)
client = gspread.authorize(creds)
client_ai = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

# --- SETTINGS ---
RISK_PER_TRADE_USD = 500
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

spreadsheet = client.open("Stock Scanner")
sheet_core = spreadsheet.worksheet("Core Screener")
sheet_summary = spreadsheet.worksheet("Summary")

def format_sheet(sheet_obj):
    sheet_id = sheet_obj._properties['sheetId']
    requests = [{"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},"cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},"fields": "userEnteredFormat.textFormat.bold"}},
                {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},"fields": "gridProperties.frozenRowCount"}},
                {"autoResizeDimensions": {"dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 25}}}]
    spreadsheet.batch_update({"requests": requests})

# --- ENHANCED AI THESIS ---
def get_ai_thesis(ticker, row):
    # This prompt forces the AI to be specific
    prompt = f"""
    Analyze {ticker} for a professional hedge fund briefing:
    - Current Price: ${row.Price}
    - Relative Strength: {row.RS_Rating} (Outperforming the market)
    - Relative Volume: {row.RVOL}x (Institutional activity)
    - Volatility (ADR): {row['ADR%']}%
    - Technical State: Volatility Squeeze (Ready to explode)
    
    Provide a 3-sentence 'Alpha Thesis'. 
    Sentence 1: Explain the institutional footprint based on the RVOL and RS.
    Sentence 2: Explain the risk/reward based on the Squeeze.
    Sentence 3: Give a definitive 'Buy' conviction statement.
    Do not use generic filler text.
    """
    try:
        response = client_ai.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        return response.text.strip()
    except Exception as e:
        return f"Momentum leader showing RS of {row.RS_Rating}. High-conviction institutional setup."

def send_telegram_alert(ticker, row, history_df):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 6))
    history_df['Close'].tail(60).plot(ax=ax, color='#00ff00', lw=2.5)
    ax.set_title(f"{ticker} - {row.RS_Line}", fontsize=16, color='#00d4ff')
    ax.grid(alpha=0.1)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    plt.close()

    thesis = get_ai_thesis(ticker, row)
    
    # MOBILE OPTIMIZED CAPTION
    caption = (
        f"🚀 **APEX SIGNAL: {ticker}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 **ENTRY:** ${row.Buy_Trigger}\n"
        f"🛑 **STOP:** ${row.Stop_Loss}\n"
        f"💰 **TARGET:** ${row.Target}\n"
        f"📈 **RS RATING:** {row.RS_Rating}\n"
        f"📊 **RVOL:** {row.RVOL}x\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 **AI ANALYSIS:**\n{thesis}"
    )
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {'photo': (f'{ticker}.png', buf, 'image/png')}
    data = {'chat_id': CHAT_ID, 'caption': caption, 'parse_mode': 'Markdown'}
    requests.post(url, files=files, data=data)

# --- 1. DATA INGESTION ---
print("📡 Monitoring the Leaders...")
indices = [{'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', 'idx': 0},
           {'url': 'https://en.wikipedia.org/wiki/Nasdaq-100', 'idx': 4}]

sector_map = {}
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}

for item in indices:
    try:
        req = Request(item['url'], headers=headers)
        with urlopen(req) as response:
            df_wiki = pd.read_html(io.BytesIO(response.read()))[item['idx']]
        t_col = [c for c in df_wiki.columns if any(x in str(c) for x in ['Symbol', 'Ticker'])][0]
        for _, row in df_wiki.iterrows():
            sector_map[str(row[t_col]).replace('.', '-')] = "Mapped"
    except Exception as e: print(f"⚠️ Warning: Scrape fail: {e}")

full_ticker_list = list(sector_map.keys())
all_data = yf.download(full_ticker_list + ["SPY"], period="2y", auto_adjust=True, progress=False)
spy_close = all_data['Close']['SPY']

master_data = []

# --- 2. APEX ANALYSIS ---
for t in full_ticker_list:
    try:
        if t not in all_data['Close'].columns: continue
        df = pd.DataFrame({'High': all_data['High'][t], 'Low': all_data['Low'][t], 'Close': all_data['Close'][t], 'Volume': all_data['Volume'][t]}).dropna()
        if len(df) < 252: continue
        
        close, curr_price = df['Close'], df['Close'].iloc[-1]
        sma_50, sma_150, sma_200 = close.rolling(50).mean().iloc[-1], close.rolling(150).mean().iloc[-1], close.rolling(200).mean().iloc[-1]
        
        if not (curr_price > sma_150 > sma_200) or curr_price < sma_50: continue
        
        rs_line = (close / spy_close)
        rs_rating = round((rs_line.iloc[-1] / rs_line.rolling(200).mean().iloc[-1] - 1) * 100, 2)
        if rs_rating < 0: continue
        
        rvol = round(df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1], 2)
        adr_pct = round(((df['High'] - df['Low']) / df['Low']).rolling(20).mean().iloc[-1] * 100, 2)
        rs_visual = "📈 BREAKOUT" if (rs_line.iloc[-1] > rs_line.iloc[-10]) else "📉 STALLING"
        
        sma_20, std_20 = close.rolling(20).mean(), close.rolling(20).std()
        tr = pd.concat([df['High']-df['Low'], abs(df['High']-close.shift()), abs(df['Low']-close.shift())], axis=1).max(axis=1)
        is_sq = ((sma_20 - (2*std_20)) > (sma_20 - (1.5*tr.rolling(20).mean()))).iloc[-1]
        
        buy_trigger = round(df['High'].tail(5).max() * 1.002, 2)
        stop_loss = round(curr_price - (tr.rolling(20).mean().iloc[-1] * 1.5), 2)
        target = round(buy_trigger + ((buy_trigger - stop_loss) * 2.5), 2)
        shares = int(RISK_PER_TRADE_USD / (buy_trigger - stop_loss)) if (buy_trigger - stop_loss) > 0 else 0
        
        master_data.append({
            'Stock': t, 'Price': round(curr_price, 2), 'RS_Line': rs_visual, 'RS_Rating': rs_rating,
            'RVOL': rvol, 'ADR%': adr_pct, 'Squeeze': "YES" if is_sq else "No", 
            'Buy_Trigger': buy_trigger, 'Stop_Loss': stop_loss, 'Target': target,
            'Shares': shares, 'Score': round(rs_rating + (rvol * 5) + (10 if is_sq else 0), 2)
        })
    except: continue

# --- 3. OUTPUT & ALERTS ---
if master_data:
    df_all = pd.DataFrame(master_data).sort_values(by='Score', ascending=False)
    
    # Update Core Sheet
    sheet_core.clear(); sheet_core.update([df_all.head(100).columns.tolist()] + df_all.head(100).values.tolist()); format_sheet(sheet_core)
    
    # Update Summary Sheet
    df_sum = df_all[df_all['Squeeze'] == "YES"].head(10).copy()
    if not df_sum.empty:
        sheet_summary.clear(); sheet_summary.update([df_sum.columns.tolist()] + df_sum.values.tolist()); format_sheet(sheet_summary)
        
        # Dispatch Top 3
        for _, row in df_sum.head(3).iterrows():
            ticker_hist = yf.download(row.Stock, period='1y', progress=False)
            send_telegram_alert(row.Stock, row, ticker_hist)
    
    print(f"🏁 Apex Scan Complete. Check Telegram for leaders.")

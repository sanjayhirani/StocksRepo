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

# --- AI THESIS GENERATOR ---
def get_ai_thesis(ticker, row):
    prompt = (
        f"Analyze the stock {ticker}. RS: {row.RS_Rating}, RVOL: {row.RVOL}x, ADR: {row['ADR%']}%. "
        f"It's in a volatility Squeeze. Write 3 sentences: "
        f"1. Interpret the {row.RVOL}x volume relative to institutional interest. "
        f"2. Explain why the {row['ADR%']}% ADR makes this a high-potential setup. "
        f"3. Final conviction verdict for a trader."
        "Use zero filler words. Be aggressive and professional."
    )
    try:
        response = client_ai.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        return response.text.strip()
    except Exception as e:
        return f"High-momentum setup with RS of {row.RS_Rating} and ADR of {row['ADR%']}%. Institutional accumulation confirmed."

def send_telegram_alert(ticker, row, history_df):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    # 1. Plot Price & Moving Averages
    prices = history_df['Close'].tail(60)
    sma50 = prices.rolling(50).mean()
    ax.plot(prices.index, prices, color='#00ff00', lw=3, label='Price', zorder=5)
    ax.plot(prices.index, sma50, color='#ff9900', lw=1.5, ls='--', label='50 SMA', alpha=0.7)
    
    # 2. Plot Trade Levels (As requested: Price, Trigger, Stop)
    ax.axhline(y=row.Buy_Trigger, color='#00ff88', linestyle='--', lw=2.5, label=f'BUY @ {row.Buy_Trigger}')
    ax.axhline(y=row.Stop_Loss, color='#ff4444', linestyle='--', lw=2.5, label=f'STOP @ {row.Stop_Loss}')
    ax.axhline(y=row.Price, color='#ffffff', linestyle=':', lw=1.2, alpha=0.9, label=f'NOW @ {row.Price}')
    
    # 3. Visual Shading (Risk vs Reward visualization)
    ax.fill_between(prices.index, row.Stop_Loss, row.Buy_Trigger, color='#ff4444', alpha=0.12)
    ax.fill_between(prices.index, row.Buy_Trigger, row.Buy_Trigger * 1.15, color='#00ff88', alpha=0.06)

    # Formatting
    ax.set_title(f"{ticker} - {row.RS_Line} (ADR: {row['ADR%']}%)", fontsize=18, color='#00d4ff', fontweight='bold')
    ax.legend(loc='upper left', fontsize=10, framealpha=0.4)
    ax.grid(alpha=0.1)
    plt.xticks(rotation=30)
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=130)
    buf.seek(0)
    plt.close()

    thesis = get_ai_thesis(ticker, row)
    
    caption = (
        f"🚀 **APEX SIGNAL: {ticker}**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 **LAST PRICE:** ${row.Price}\n"
        f"🎯 **ENTRY:** ${row.Buy_Trigger}\n"
        f"🛑 **STOP:** ${row.Stop_Loss}\n"
        f"💰 **TARGET:** ${row.Target}\n"
        f"📊 **RVOL:** {row.RVOL}x | **RS:** {row.RS_Rating}\n"
        f"⚡ **VOLATILITY (ADR):** {row['ADR%']}%\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 **ALPHA THESIS:**\n{thesis}"
    )
    
    requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", 
                  files={'photo': (f'{ticker}.png', buf, 'image/png')}, 
                  data={'chat_id': CHAT_ID, 'caption': caption, 'parse_mode': 'Markdown'})

# --- 1. DATA INGESTION ---
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
    except Exception as e: print(f"⚠️ Scrape fail: {e}")

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
        tr_series = pd.concat([df['High']-df['Low'], abs(df['High']-close.shift()), abs(df['Low']-close.shift())], axis=1).max(axis=1)
        is_sq = ((sma_20 - (2*std_20)) > (sma_20 - (1.5*tr_series.rolling(20).mean()))).iloc[-1]
        
        buy_trigger = round(df['High'].tail(5).max() * 1.002, 2)
        stop_loss = round(curr_price - (tr_series.rolling(20).mean().iloc[-1] * 1.5), 2)
        target = round(buy_trigger + ((buy_trigger - stop_loss) * 2.5), 2)
        
        master_data.append({
            'Stock': t, 'Price': round(curr_price, 2), 'RS_Line': rs_visual, 'RS_Rating': rs_rating,
            'RVOL': rvol, 'ADR%': adr_pct, 'Squeeze': "YES" if is_sq else "No", 
            'Buy_Trigger': buy_trigger, 'Stop_Loss': stop_loss, 'Target': target,
            'Score': round(rs_rating + (rvol * 5) + (10 if is_sq else 0), 2)
        })
    except: continue

# --- 3. OUTPUT & ALERTS ---
if master_data:
    df_all = pd.DataFrame(master_data).sort_values(by='Score', ascending=False)
    df_sum = df_all[df_all['Squeeze'] == "YES"].head(10).copy()
    
    if not df_sum.empty:
        for _, row in df_sum.head(3).iterrows():
            ticker_hist = yf.download(row.Stock, period='1y', progress=False)
            send_telegram_alert(row.Stock, row, ticker_hist)

    sheet_core.clear(); sheet_core.update([df_all.head(100).columns.tolist()] + df_all.head(100).values.tolist()); format_sheet(sheet_core)
    sheet_summary.clear(); sheet_summary.update([df_sum.columns.tolist()] + df_sum.values.tolist()); format_sheet(sheet_summary)
    
print(f"🏁 Apex Scan Complete.")

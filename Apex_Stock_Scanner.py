import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import mplfinance as mpf
import matplotlib.pyplot as plt

CACHE_DIR = "data_cache"
CACHE_FILE = os.path.join(CACHE_DIR, "russell_1000_2y.pkl")

def get_russell_1000():
    url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        for table in wiki_tables:
            if 'Symbol' in table.columns:
                return [str(t).strip().replace('.', '-') for t in table['Symbol'].tolist()]
    except: return []

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def run_scanner():
    try:
        # 1. SETUP & AUTH
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        tkrs = get_russell_1000()
        if not tkrs: return

        # 2. DATA DOWNLOAD (Standard Bulk with Cache)
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if data.empty and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        else:
            data.to_pickle(CACHE_FILE)

        results = []

        # 3. APEX LOGIC LOOP
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                # Techs
                df['RSI'] = calculate_rsi(df['Close'])
                sma200 = df['Close'].rolling(200).mean().iloc[-1]
                atr = (df['High']-df['Low']).rolling(14).mean().iloc[-1]
                close = df['Close'].iloc[-1]
                
                # Scan Window (Last 12 Trading Days)
                rsi_window = df['RSI'].tail(12)
                
                setup_type = None
                if close > sma200 and rsi_window.min() < 42:
                    setup_type = "LONG"
                    p_score = round((42 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmin() - 1)
                    trigger = round(df['High'].tail(2).max() * 1.005, 2)
                    stop = round(trigger - (atr * 2.5), 2)
                elif close < sma200 and rsi_window.max() > 58:
                    setup_type = "SHORT"
                    p_score = round((rsi_window.max() - 58) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmax() - 1)
                    trigger = round(df['Low'].tail(2).min() * 0.995, 2)
                    stop = round(trigger + (atr * 2.5), 2)

                if setup_type and (abs(trigger - stop) / trigger) <= 0.12:
                    results.append({
                        'Stock': t, 'Squeeze': f"{setup_type} (Age:{age}d)", 'Power_Score': p_score,
                        'Vol_Surge': f"{df['Volume'].iloc[-1]/df['Volume'].rolling(20).mean().iloc[-1]:.2f}x",
                        'Buy_At': trigger if setup_type == "LONG" else "",
                        'Sell_At': trigger if setup_type == "SHORT" else "",
                        'Stop_Loss': stop, 'Price': round(close, 2),
                        'Mkt_Cap': f"{close * 1e6 / 1e9:.1f}B", # Estimated based on scale
                        'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                        'df_ptr': df
                    })
            except: continue

        # 4. SHEET UPDATES
        df_full = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn)
            ws.clear()
            limit = 5 if sn == "Summary" else 50
            out = df_full.head(limit).drop(columns=['df_ptr'])
            ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

        # 5. CHARTING (Gap Fixed by Explicit X-Axis Slice)
        for _, row in df_full.head(5).iterrows():
            t, hist = row.Stock, row['df_ptr'].tail(100).copy()
            # Force the chart to only see the last 100 days (kills the white gap)
            hist.index = hist.index.strftime('%Y-%m-%d') 
            
            sma200_plt = row['df_ptr']['Close'].rolling(200).mean().tail(100)
            buf = io.BytesIO()
            mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(sma200_plt.values, color='blue')], 
                     style='charles', savefig=buf, tight_layout=True, datetime_format='%Y-%m-%d')
            buf.seek(0)

            caption = (f"<b>{row.Squeeze}: {t}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"💰 Last: ${row.Price}\n"
                       f"⚔️ Trigger: ${row.Buy_At if row.Buy_At else row.Sell_At}\n"
                       f"🛡️ Stop: ${row.Stop_Loss}\n"
                       f"📊 Power Score: {row.Power_Score}")

            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                          files={'photo': (f'{t}.png', buf)}, 
                          data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
            plt.close('all')

        print("✅ Scan Complete.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

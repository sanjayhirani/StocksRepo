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
        # 1. AUTH
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        tkrs = get_russell_1000()
        if not tkrs: return

        # 2. DATA (Russell 1000 is ~1000 tickers + SPY)
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if data.empty and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        else:
            data.to_pickle(CACHE_FILE)

        results = []

        # 3. APEX LOGIC
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                # Keep index as DatetimeIndex for mplfinance
                df.index = pd.to_datetime(df.index)
                
                df['RSI'] = calculate_rsi(df['Close'])
                sma200 = df['Close'].rolling(200).mean()
                atr = (df['High']-df['Low']).rolling(14).mean().iloc[-1]
                close = float(df['Close'].iloc[-1])
                
                rsi_window = df['RSI'].tail(12)
                
                setup_type = None
                if close > sma200.iloc[-1] and rsi_window.min() < 42:
                    setup_type = "LONG"
                    p_score = round((42 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmin() - 1)
                    trigger = round(df['High'].tail(2).max() * 1.005, 2)
                    stop = round(trigger - (atr * 2.5), 2)
                elif close < sma200.iloc[-1] and rsi_window.max() > 58:
                    setup_type = "SHORT"
                    p_score = round((rsi_window.max() - 58) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmax() - 1)
                    trigger = round(df['Low'].tail(2).min() * 0.995, 2)
                    stop = round(trigger + (atr * 2.5), 2)

                if setup_type and (abs(trigger - stop) / trigger) <= 0.12:
                    # Quick Market Cap approximation to save time
                    vol_surge = df['Volume'].iloc[-1]/df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 'Squeeze': f"{setup_type} (Age:{age}d)", 'Power_Score': p_score,
                        'Vol_Surge': f"{vol_surge:.2f}x",
                        'Buy_At': trigger if setup_type == "LONG" else "",
                        'Sell_At': trigger if setup_type == "SHORT" else "",
                        'Stop_Loss': stop, 'Price': round(close, 2),
                        'Mkt_Cap': "N/A", # Removed slow fetch
                        'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                        'df_ptr': df,
                        'sma200_ser': sma200
                    })
            except: continue

        # 4. SHEET UPDATES
        df_full = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn)
            ws.clear()
            limit = 5 if sn == "Summary" else 50
            out = df_full.head(limit).drop(columns=['df_ptr', 'sma200_ser'])
            ws.update([out.columns.tolist()] + out.astype(str).values.tolist())

        # 5. TELEGRAM (No-Gap Charting)
        for _, row in df_full.head(5).iterrows():
            t = row.Stock
            # Slice last 100 days for visual clarity
            hist = row['df_ptr'].tail(100)
            sma200_slice = row['sma200_ser'].tail(100)
            
            buf = io.BytesIO()
            # show_nontrading=False removes the weekend gaps without breaking the index
            mpf.plot(hist, type='candle', 
                     addplot=[mpf.make_addplot(sma200_slice, color='blue', width=1.2)], 
                     style='charles', volume=True, savefig=buf, 
                     tight_layout=True, show_nontrading=False)
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

        print("✅ Scan Complete. Charts and Sheets updated.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import mplfinance as mpf
import matplotlib.pyplot as plt

# --- CONFIG ---
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
        # 1. SETUP
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        tkrs = get_russell_1000()
        if not tkrs: return

        # 2. DATA DOWNLOAD & CACHE
        print(f"🚀 Scanning {len(tkrs)} tickers...")
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        if data.empty and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        else:
            data.to_pickle(CACHE_FILE)

        spy_close = data['SPY']['Close']
        results = []

        # 3. ADVANCED LOGIC LOOP
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                # Technical Foundations
                df['RSI'] = calculate_rsi(df['Close'])
                sma200 = df['Close'].rolling(200).mean().iloc[-1]
                atr = (df['High']-df['Low']).rolling(14).mean().iloc[-1]
                close = df['Close'].iloc[-1]
                
                # Squeeze Windows
                rsi_window = df['RSI'].tail(12)
                recent_high = df['High'].tail(2).max()
                recent_low = df['Low'].tail(2).min()
                
                setup_type, trigger, stop, age, p_score = None, None, None, 0, 0

                # --- LONG SETUP (Price > SMA200 AND RSI dipped < 42 in last 12 days) ---
                if close > sma200 and rsi_window.min() < 42:
                    setup_type = "LONG"
                    age = int(len(rsi_window) - rsi_window.argmin() - 1)
                    trigger = round(recent_high * 1.005, 2)
                    stop = round(trigger - (atr * 2.5), 2)
                    p_score = round((42 - rsi_window.min()) * 5, 2)

                # --- SHORT SETUP (Price < SMA200 AND RSI spiked > 58 in last 12 days) ---
                elif close < sma200 and rsi_window.max() > 58:
                    setup_type = "SHORT"
                    age = int(len(rsi_window) - rsi_window.argmax() - 1)
                    trigger = round(recent_low * 0.995, 2)
                    stop = round(trigger + (atr * 2.5), 2)
                    p_score = round((rsi_window.max() - 58) * 5, 2)

                if setup_type:
                    # Risk filter: max 12% distance to stop
                    risk = abs(trigger - stop) / trigger
                    if risk <= 0.12:
                        results.append({
                            'Stock': t,
                            'Squeeze': f"{setup_type} (Age:{age}d)",
                            'Power_Score': p_score,
                            'Vol_Surge': f"{df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]:.2f}x",
                            'Buy_At': trigger if setup_type == "LONG" else "",
                            'Sell_At': trigger if setup_type == "SHORT" else "",
                            'Stop_Loss': stop,
                            'Mkt_Cap': f"{data[t]['Close'].iloc[-1] * 1000000 / 1e9:.1f}B", # Placeholder cap logic for speed
                            'Price': round(close, 2),
                            'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                            'df_ptr': df
                        })
            except: continue

        # 4. REPORTING
        df_full = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn)
            ws.clear()
            limit = 5 if sn == "Summary" else 50
            final_out = df_full.head(limit).drop(columns=['df_ptr'])
            ws.update([final_out.columns.tolist()] + final_out.astype(str).values.tolist())

        # 5. TELEGRAM
        for _, row in df_full.head(5).iterrows():
            t, hist = row.Stock, row['df_ptr'].tail(100)
            buf = io.BytesIO()
            sma200_plt = row['df_ptr']['Close'].rolling(200).mean().tail(100)
            mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(sma200_plt, color='blue')], style='charles', savefig=buf)
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

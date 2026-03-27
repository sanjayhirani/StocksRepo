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

def run_scanner():
    """APEX BULL RUN SCANNER - Integrated Backtester Logic"""
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        tkrs = get_russell_1000()
        if not tkrs: return

        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if (data is None or data.empty) and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        elif data is not None and not data.empty:
            data.to_pickle(CACHE_FILE)
        else: return

        results = []
        VOL_STOP_PCT = 0.25 # Backtester Constant

        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                # Indicators
                df['SMA200'] = df['Close'].rolling(200).mean()
                close = float(df['Close'].iloc[-1])
                sma200_val = df['SMA200'].iloc[-1]
                
                # APEX LOGIC: Price must be above 200-SMA for Bull Run
                if close > sma200_val:
                    # Calculate Trail Stop (25% off 52-week High)
                    peak_52w = df['Close'].tail(252).max()
                    apex_stop = round(peak_52w * (1 - VOL_STOP_PCT), 2)
                    
                    # Power Score = (Trend Strength % * 0.7) + (Distance from High % * 0.3)
                    trend_strength = (close - sma200_val) / sma200_val
                    dist_from_high = 1 - (close / peak_52w)
                    p_score = round((trend_strength * 100) - (dist_from_high * 50), 2)

                    vol_surge = df['Volume'].iloc[-1]/df['Volume'].rolling(20).mean().iloc[-1]
                    
                    results.append({
                        'Stock': t, 
                        'Status': "🚀 BULL RUN", 
                        'Power_Score': p_score,
                        'Vol_Surge': f"{vol_surge:.2f}x", 
                        'Price': round(close, 2),
                        'SMA200': round(sma200_val, 2),
                        'Apex_Stop': apex_stop,
                        'Dist_To_Stop': f"{round(((close-apex_stop)/close)*100, 1)}%",
                        'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                        'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_full = df_full.sort_values(by='Power_Score', ascending=False)
            
            # Update Google Sheets
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                limit = 5 if sn == "Summary" else 100
                final_cols = df_full.head(limit).drop(columns=['df_ptr'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())

            # Telegram Alerts
            for _, row in df_full.head(5).iterrows():
                t, hist = row.Stock, row['df_ptr'].tail(150)
                buf = io.BytesIO()
                
                # Plot with SMA200 (Blue) and the Apex Stop Line (Red Dash)
                ap = [
                    mpf.make_addplot(hist['SMA200'], color='blue', width=1.5),
                ]
                
                mpf.plot(hist, type='candle', addplot=ap, style='charles', 
                         volume=True, savefig=buf, tight_layout=True)
                buf.seek(0)

                caption = (
                    f"<b>🚀 APEX SIGNAL: {t}</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>Last:</b> ${row.Price}\n"
                    f"📈 <b>SMA200:</b> ${row.SMA200}\n"
                    f"🛡️ <b>Apex Stop:</b> ${row.Apex_Stop}\n"
                    f"🔥 <b>Power Score:</b> {row.Power_Score}\n"
                    f"📊 <b>Vol Surge:</b> {row.Vol_Surge}"
                )
                
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                              files={'photo': (f'{t}.png', buf)}, 
                              data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print("✅ Apex Scanner Run Finished.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

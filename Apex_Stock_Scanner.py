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
CACHE_FILE = os.path.join(CACHE_DIR, "sp500_apex_2y.pkl")

def get_sp500_tickers():
    """ORIGINAL METHOD: Scrapes tickers and sectors from the S&P 500 Wikipedia table."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        df = wiki_tables[0]
        # Keep the Ticker and Sector association
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'GICS Sector']].values.tolist()
    except: return []

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def run_scanner():
    """S&P 500 APEX SCANNER - FULL 500 + SECTORS + 2-5D SUMMARY"""
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        if not ticker_data: return
        
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if (data is None or data.empty) and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        elif data is not None and not data.empty:
            data.to_pickle(CACHE_FILE)
        else: return

        results = []
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                df['RSI'] = calculate_rsi(df['Close'])
                df['SMA200'] = df['Close'].rolling(200).mean()
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                
                close, sma200, atr = float(df['Close'].iloc[-1]), df['SMA200'].iloc[-1], df['ATR'].iloc[-1]
                rsi_window = df['RSI'].tail(12)
                
                setup_type, trigger, stop, target, p_score, age = None, 0, 0, 0, 0, 0

                # LONG: Quality Dip (Above 200 SMA)
                if close > sma200 and rsi_window.min() < 35:
                    setup_type = "LONG"
                    p_score = round((40 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.values.argmin() - 1)
                    trigger = round(df['High'].tail(3).max() * 1.005, 2)
                    stop = round(df['Low'].tail(5).min() - (atr * 1.5), 2)
                    target = round(close + (abs(close - stop) * 2.0), 2)
                
                # SHORT: Structural Breakdown (Below 200 SMA)
                elif close < sma200 and rsi_window.max() > 65:
                    setup_type = "SHORT"
                    p_score = round((rsi_window.max() - 60) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.values.argmax() - 1)
                    trigger = round(df['Low'].tail(3).min() * 0.995, 2)
                    stop = round(df['High'].tail(5).max() + (atr * 1.5), 2)
                    target = round(close - (abs(stop - close) * 2.0), 2)

                if setup_type:
                    dist = abs(close - trigger) / close
                    vol_surge = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Squeeze': f"{setup_type} (Age:{age}d)", 'Power_Score': p_score,
                        'Vol_Surge': f"{vol_surge:.2f}x", 
                        'Buy_At': trigger if setup_type == "LONG" else "",
                        'Sell_At': trigger if setup_type == "SHORT" else "",
                        'Stop_Loss': stop, 'Target': target, 'Price': round(close, 2),
                        'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                        'Dist_To_Trigger': dist, 'Age_Val': age,
                        'df_ptr': df, 'sma200_val': df['SMA200']
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            # 1. Summary: STRICT Age 2-5, Top 5 by Power Score
            df_summary = df_full[(df_full['Age_Val'] >= 2) & (df_full['Age_Val'] <= 5)].copy()
            df_summary = df_summary.sort_values(by=['Power_Score', 'Dist_To_Trigger'], ascending=[False, True]).head(5)

            # 2. Core Screener: Full Variety (All 500+), sorted by Proximity
            df_core = df_full.sort_values(by=['Dist_To_Trigger', 'Power_Score'], ascending=[True, False])
            
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                final_cols = source_df.drop(columns=['df_ptr', 'sma200_val', 'Dist_To_Trigger', 'Age_Val'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())

            for _, row in df_summary.iterrows():
                t, hist, sma200_plt = row.Stock, row['df_ptr'].tail(100), row['sma200_val'].tail(100)
                buf = io.BytesIO()
                mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(sma200_plt, color='blue', width=1.5)], 
                         style='charles', volume=True, savefig=buf, tight_layout=True)
                buf.seek(0)
                icon = "🛡️" if "LONG" in row.Squeeze else "⚠️"
                trigger_val = row.Buy_At if row.Buy_At else row.Sell_At
                caption = (f"<b>{icon} {t} ({row.Sector})</b>\n"
                           f"<b>{row.Squeeze}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${trigger_val}\n💰 Price: ${row.Price}\n"
                           f"🛡️ Stop: ${row.Stop_Loss}\n🏁 Target: ${row.Target}\n📊 Power Score: {row.Power_Score}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')
                
        print(f"✅ Apex S&P 500 Scanner Complete. {len(df_full)} results including sectors.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

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
    """ORIGINAL METHOD: Scrapes tickers and sectors from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        df = wiki_tables[0]
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
    """S&P 500 APEX - CACHED + HIGH CONVICTION INTRADAY ENGINE"""
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        if not ticker_data: return
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        # 1. MARKET BASELINE (Hourly SPY for Intraday Alpha)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_4h_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1

        # 2. CACHED DAILY DATA PULL
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

                # INTRADAY ALPHA CALC (Zero-Day Detection)
                # Note: We pull a small hourly slice for the Alpha check
                df_h = yf.download(t, period="2d", interval="1h", progress=False)
                stock_4h_change = (df_h['Close'].iloc[-1] / df_h['Close'].iloc[-5]) - 1
                alpha = stock_4h_change - spy_4h_change
                
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                df['RSI'] = calculate_rsi(df['Close'])
                df['SMA200'] = df['Close'].rolling(200).mean()
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                
                close, sma200, atr = float(df['Close'].iloc[-1]), df['SMA200'].iloc[-1], df['ATR'].iloc[-1]
                rsi_window = df['RSI'].tail(12)
                
                setup_type, conviction = None, 0

                # LONG: Quality Dip + Relative Strength (Alpha > 0)
                if close > sma200 and rsi_window.min() < 45 and alpha > 0.003:
                    setup_type = "LONG"
                    conviction = ((45 - rsi_window.min()) * 2) + (alpha * 1000)
                
                # SHORT: Structural Breakdown + Relative Weakness (Alpha < 0)
                elif close < sma200 and rsi_window.max() > 55 and alpha < -0.003:
                    setup_type = "SHORT"
                    conviction = ((rsi_window.max() - 55) * 2) + (abs(alpha) * 1000)

                if setup_type:
                    age = int(len(rsi_window) - (rsi_window.values.argmin() if setup_type == "LONG" else rsi_window.values.argmax()) - 1)
                    trigger = round(df['High'].tail(3).max() * 1.005, 2) if setup_type == "LONG" else round(df['Low'].tail(3).min() * 0.995, 2)
                    
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Setup': f"{setup_type}", 'Alpha_1h': f"{alpha*100:+.2f}%",
                        'Conviction': round(conviction, 2), 'Price': round(close, 2),
                        'Trigger': trigger, 
                        'Stop': round(df['Low'].tail(5).min() - (atr * 1.5), 2) if setup_type == "LONG" else round(df['High'].tail(5).max() + (atr * 1.5), 2),
                        'Target': round(close + (abs(close - (df['Low'].tail(5).min() - (atr * 1.5))) * 2), 2) if setup_type == "LONG" else round(close - (abs((df_d['High'].tail(5).max() + (atr * 1.5)) - close) * 2), 2),
                        'Age_Val': age, 'df_ptr': df, 'sma200_val': df['SMA200']
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            # Summary: 2-5 Day Squeeze + Top Conviction
            df_summary = df_full[(df_full['Age_Val'] >= 2) & (df_full['Age_Val'] <= 5)].sort_values('Conviction', ascending=False).head(5)
            # Core: Full Variety
            df_core = df_full.sort_values(['Sector', 'Conviction'], ascending=[True, False])

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                # Ensuring no data loss in columns
                final_cols = source_df[['Stock', 'Sector', 'Setup', 'Alpha_1h', 'Conviction', 'Price', 'Trigger', 'Stop', 'Target']]
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())

            for _, row in df_summary.iterrows():
                t, hist, sma200_plt = row.Stock, row['df_ptr'].tail(100), row['sma200_val'].tail(100)
                buf = io.BytesIO()
                mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(sma200_plt, color='blue')], style='charles', savefig=buf)
                buf.seek(0)
                
                icon = "🚀" if row.Setup == "LONG" else "📉"
                caption = (f"<b>{icon} {t} | {row.Sector}</b>\n"
                           f"<b>{row.Setup} | Alpha: {row.Alpha_1h}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${row.Trigger}\n📊 Conviction: {row.Conviction}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print(f"✅ Apex Scanner Complete. {len(df_full)} High-Conviction results found.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

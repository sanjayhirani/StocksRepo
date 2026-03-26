import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
import time
from datetime import datetime
from urllib.request import Request, urlopen
import mplfinance as mpf
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- CACHE CONFIG ---
CACHE_DIR = "data_cache"
CACHE_FILE = os.path.join(CACHE_DIR, "russell_1000_2y.pkl")

def get_cached_data(tkrs):
    if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
    if os.path.exists(CACHE_FILE):
        try:
            existing_data = pd.read_pickle(CACHE_FILE)
            existing_data = existing_data.dropna(axis=1, how='all')
            last_date = existing_data.index.max()
            if last_date.date() >= datetime.now().date():
                print("✅ Cache is up to date and sanitized.")
                return existing_data
            print(f"📜 Updating cache from {last_date.date()}...")
            new_data = yf.download(tkrs, start=last_date, group_by='ticker', progress=True)
            if not new_data.empty:
                combined = pd.concat([existing_data, new_data])
                combined = combined[~combined.index.duplicated(keep='last')].sort_index()
                start_cutoff = combined.index.max() - pd.DateOffset(years=2)
                final_data = combined[combined.index >= start_cutoff]
                final_data.to_pickle(CACHE_FILE)
                return final_data
        except: pass
    print("🚀 Full 2-year download...")
    final_data = yf.download(tkrs, period="2y", group_by='ticker', progress=True)
    final_data = final_data.dropna(axis=1, how='all')
    final_data.to_pickle(CACHE_FILE)
    return final_data

def fetch_ticker_info(t):
    try:
        return t, yf.Ticker(t).info
    except: return t, {}

def calculate_rsi(series, period=14):
    if len(series) < period: return pd.Series([np.nan] * len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def get_russell_1000():
    url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0'})))
        for table in wiki_tables:
            if 'Symbol' in table.columns: return [str(t).strip().replace('.', '-') for t in table['Symbol'].tolist()]
    except: return []

def run_scanner():
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        tkrs = get_russell_1000()
        data = get_cached_data(tkrs)
        if data.empty: return

        with ThreadPoolExecutor(max_workers=10) as executor:
            info_map = dict(executor.map(fetch_ticker_info, tkrs))

        results = []
        available_tickers = data.columns.levels[0] if isinstance(data.columns, pd.MultiIndex) else data.columns

        for t in tkrs:
            # --- THE NUCLEAR SHIELD ---
            try:
                if t not in available_tickers: continue
                df = data[t].dropna(subset=['Close', 'High', 'Low'])
                if len(df) < 212: continue
                
                close = df['Close'].iloc[-1]
                sma200 = df['Close'].rolling(200).mean().iloc[-1]
                atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
                df['RSI'] = calculate_rsi(df['Close'])
                
                rsi_window = df['RSI'].tail(12).dropna()
                if rsi_window.empty or len(rsi_window) < 1: continue 

                recent_slice = df.iloc[-2:]
                if len(recent_slice) < 2 or recent_slice['High'].isnull().any(): continue

                # Logic
                if close > sma200 and rsi_window.min() < 42:
                    days_since = len(rsi_window) - rsi_window.argmin() - 1
                    buy_trigger = round(recent_slice['High'].max() * 1.005, 2)
                    stop_loss = round(buy_trigger - (atr * 2.5), 2)
                    results.append({'Stock': t, 'Type': 'LONG', 'RSI': int(df['RSI'].iloc[-1]), 'Power_Score': round((42 - rsi_window.min()) * 5, 2), 'Buy_At': buy_trigger, 'Sell_At': '', 'Days_Since': days_since, 'Stop_Loss': stop_loss, 'Target_1': round(buy_trigger * 1.25, 2), 'Price': round(close, 2)})
                elif close < sma200 and rsi_window.max() > 58:
                    days_since = len(rsi_window) - rsi_window.argmax() - 1
                    sell_trigger = round(recent_slice['Low'].min() * 0.995, 2)
                    stop_loss = round(sell_trigger + (atr * 2.5), 2)
                    results.append({'Stock': t, 'Type': 'SHORT', 'RSI': int(df['RSI'].iloc[-1]), 'Power_Score': round((rsi_window.max() - 58) * 5, 2), 'Buy_At': '', 'Sell_At': sell_trigger, 'Days_Since': days_since, 'Stop_Loss': stop_loss, 'Target_1': round(sell_trigger * 0.85, 2), 'Price': round(close, 2)})
            except: continue # Skip bad apples

        final_df = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        if final_df.empty: return

        full_data = []
        for _, row in final_df.head(50).iterrows():
            t = row.Stock
            info, hist = info_map.get(t, {}), data[t].dropna()
            if hist.empty: continue
            full_data.append({
                'Stock': t, 'Squeeze': f"{row.Type} (RSI:{row.RSI})", 'Power_Score': row.Power_Score,
                'Vol_Surge': f"{hist['Volume'].iloc[-1]/hist['Volume'].rolling(20).mean().iloc[-1]:.2f}x",
                'Buy_At': row.Buy_At, 'Sell_At': row.Sell_At, 'RSI_Days': row.Days_Since, 'Stop_Loss': row.Stop_Loss, 'Target_1': row.Target_1,
                'Gross_M': f"{(info.get('grossMargins', 0) or 0)*100:.0f}%", 'EBIT_M': f"{(info.get('ebitdaMargins', 0) or 0)*100:.0f}%",
                'Mkt_Cap': f"{(info.get('marketCap', 0) or 0)/1e9:.1f}B", 'Price': row.Price, 'YTD': round(((row.Price/hist['Close'].iloc[0])-1)*100, 1)
            })
        
        core_df = pd.DataFrame(full_data)
        for name, df_to_sheet in [("Summary", core_df.head(5).drop(columns=['Gross_M', 'EBIT_M'])), ("Core Screener", core_df)]:
            ws = sh.worksheet(name)
            ws.clear()
            ws.update([df_to_sheet.columns.tolist()] + df_to_sheet.astype(str).values.tolist())

        # TELEGRAM
        for _, row in core_df.head(5).iterrows():
            t, hist = row.Stock, data[row.Stock].dropna().tail(100)
            buf = io.BytesIO()
            mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(hist['Close'].rolling(200).mean(), color='blue')], style='charles', savefig=buf)
            buf.seek(0)
            is_long = "LONG" in row.Squeeze
            caption = (f"<b>{'🎯 BUY' if is_long else '💀 SELL'}: {t}</b>\n━━━━━━━━━━━━━━━━━━━━\n💰 <b>Last:</b> ${row.Price} | ⏳ <b>Age:</b> {row.RSI_Days}d\n"
                       f"⚔️ <b>Trigger:</b> ${row.Buy_At if is_long else row.Sell_At}\n🛡️ <b>Stop:</b> ${row.Stop_Loss} | 🏁 <b>Target:</b> ${row.Target_1}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n📊 <i>Apex Score: {row.Power_Score}</i>")
            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
        
        print("✅ Scan Complete.")
    except Exception as e: print(f"FATAL: {e}")

if __name__ == "__main__":
    run_scanner()

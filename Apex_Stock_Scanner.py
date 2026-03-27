import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import mplfinance as mpf
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- CACHE CONFIG ---
CACHE_DIR = "data_cache"
CACHE_FILE = os.path.join(CACHE_DIR, "russell_1000_2y.pkl")

def get_russell_1000():
    url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'Mozilla/5.0'})))
        for table in wiki_tables:
            if 'Symbol' in table.columns:
                return [str(t).strip().replace('.', '-') for t in table['Symbol'].tolist()]
    except: return []

def calculate_rsi(series, period=14):
    if len(series) < period: return pd.Series([np.nan] * len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def run_scanner():
    try:
        # 1. SETUP
        token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat = os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        tkrs = get_russell_1000()
        results = []
        final_cache = {}

        print(f"🚀 Scanning {len(tkrs)} tickers...")

        # 2. SCANNING LOOP
        for t in tkrs:
            try:
                # Individual download for reliability
                df = yf.download(t, period="2y", progress=False, interval="1d")
                if df.empty or len(df) < 212: continue
                
                final_cache[t] = df

                close = float(df['Close'].iloc[-1])
                sma200 = float(df['Close'].rolling(200).mean().iloc[-1])
                atr = float((df['High'] - df['Low']).rolling(14).mean().iloc[-1])
                df['RSI'] = calculate_rsi(df['Close'])
                rsi_window = df['RSI'].tail(12).dropna()
                
                if rsi_window.empty: continue

                curr_rsi = float(df['RSI'].iloc[-1])
                recent_high = float(df['High'].tail(2).max())
                recent_low = float(df['Low'].tail(2).min())

                # LONG CRITERIA
                if close > sma200 and rsi_window.min() < 42:
                    days_since = int(len(rsi_window) - rsi_window.argmin() - 1)
                    buy_trigger = round(recent_high * 1.005, 2)
                    stop_loss = round(buy_trigger - (atr * 2.5), 2)
                    if (buy_trigger - stop_loss) / buy_trigger <= 0.12:
                        results.append({'Stock': t, 'Type': 'LONG', 'RSI': int(curr_rsi), 'Power_Score': round((42 - rsi_window.min()) * 5, 2), 'Buy_At': buy_trigger, 'Sell_At': '', 'Days_Since': days_since, 'Stop_Loss': stop_loss, 'Target_1': round(buy_trigger * 1.25, 2), 'Price': round(close, 2), 'df_ptr': df})

                # SHORT CRITERIA
                elif close < sma200 and rsi_window.max() > 58:
                    days_since = int(len(rsi_window) - rsi_window.argmax() - 1)
                    sell_trigger = round(recent_low * 0.995, 2)
                    stop_loss = round(sell_trigger + (atr * 2.5), 2)
                    if (stop_loss - sell_trigger) / sell_trigger <= 0.12:
                        results.append({'Stock': t, 'Type': 'SHORT', 'RSI': int(curr_rsi), 'Power_Score': round((rsi_window.max() - 58) * 5, 2), 'Buy_At': '', 'Sell_At': sell_trigger, 'Days_Since': days_since, 'Stop_Loss': stop_loss, 'Target_1': round(sell_trigger * 0.85, 2), 'Price': round(close, 2), 'df_ptr': df})
            except: continue

        # Save cache for persistence across runs
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        pd.to_pickle(final_cache, CACHE_FILE)

        if not results:
            print("⚠️ No setups found today.")
            return

        # 3. METADATA & REPORTING
        final_df = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        def get_info(t):
            try: return t, yf.Ticker(t).info
            except: return t, {}
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            info_map = dict(executor.map(get_info, [r['Stock'] for r in results[:50]]))

        full_report = []
        for _, row in final_df.head(50).iterrows():
            t, df_t, info = row.Stock, row['df_ptr'], info_map.get(row.Stock, {})
            vol_avg = df_t['Volume'].rolling(20).mean().iloc[-1]
            
            full_report.append({
                'Stock': t, 
                'Squeeze': f"{row.Type} (RSI:{row.RSI})", 
                'Power_Score': row.Power_Score,
                'Vol_Surge': f"{df_t['Volume'].iloc[-1]/vol_avg:.2f}x" if vol_avg > 0 else "1.0x",
                'Buy_At': row.Buy_At, 
                'Sell_At': row.Sell_At, 
                'RSI_Days': row.Days_Since,
                'Stop_Loss': row.Stop_Loss, 
                'Target_1': row.Target_1,
                'Gross_M': f"{(info.get('grossMargins', 0) or 0)*100:.0f}%",
                'Mkt_Cap': f"{(info.get('marketCap', 0) or 0)/1e9:.1f}B",
                'Price': row.Price, 
                'YTD': round(((row.Price/df_t['Close'].iloc[0])-1)*100, 1)
            })

        # 4. GOOGLE SHEETS UPDATE
        core_df = pd.DataFrame(full_report)
        for name, d_frame in [("Summary", core_df.head(5)), ("Core Screener", core_df)]:
            ws = sh.worksheet(name)
            ws.clear()
            ws.update([d_frame.columns.tolist()] + d_frame.astype(str).values.tolist())

        # 5. TELEGRAM ALERTS
        for _, row in core_df.head(5).iterrows():
            t, hist = row.Stock, final_cache[row.Stock].tail(100)
            buf = io.BytesIO()
            mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(hist['Close'].rolling(200).mean(), color='blue')], style='charles', savefig=buf)
            buf.seek(0)
            is_long = "LONG" in row.Squeeze
            caption = (f"<b>{'🎯 BUY' if is_long else '💀 SELL'}: {t}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                       f"💰 <b>Last:</b> ${row.Price} | ⏳ <b>Age:</b> {row.RSI_Days}d\n"
                       f"⚔️ <b>Trigger:</b> ${row.Buy_At if is_long else row.Sell_At}\n"
                       f"🛡️ <b>Stop:</b> ${row.Stop_Loss} | 🏁 <b>Target:</b> ${row.Target_1}\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n📊 <i>Score: {row.Power_Score}</i>")
            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})

        print("✅ Scan Complete. Success.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

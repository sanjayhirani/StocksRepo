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

# --- CORE UTILS ---
def fetch_ticker_info(t):
    try: return t, yf.Ticker(t).info
    except: return t, {}

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def get_russell_1000():
    url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
    for table in wiki_tables:
        if 'Symbol' in table.columns:
            return [str(t).strip().replace('.', '-') for t in table['Symbol'].tolist()]
    return []

def run_scanner():
    try:
        # 1. AUTH & CONFIG
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        # 2. DATA DOWNLOAD
        tkrs = get_russell_1000()
        data = yf.download(tkrs, period="2y", group_by='ticker', progress=False)

        with ThreadPoolExecutor(max_workers=10) as executor:
            info_results = list(executor.map(fetch_ticker_info, tkrs))
            info_map = {t: info for t, info in info_results}

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna()
                if len(df) < 200: continue
                
                close = df['Close'].iloc[-1]
                sma200 = df['Close'].rolling(200).mean().iloc[-1]
                atr = (df['High'] - df['Low']).rolling(14).mean().iloc[-1]
                df['RSI'] = calculate_rsi(df['Close'])
                
                # --- RSI DURATION & TIMING LOGIC ---
                rsi_window = df['RSI'].tail(12)
                
                # TIMING MULTIPLIER (The Apex King Logic)
                def get_timing_multiplier(days):
                    if 3 <= days <= 5: return 1.0  # Golden Zone
                    if days <= 2: return 0.5       # Too early (Falling Knife)
                    if 6 <= days <= 8: return 0.7  # Fading
                    return 0.2                     # Too late

                # SETUP A: LONG (Oversold in Uptrend)
                if close > sma200 and rsi_window.min() < 42:
                    days_since = len(rsi_window) - rsi_window.argmin() - 1
                    multiplier = get_timing_multiplier(days_since)
                    
                    two_day_high = df['High'].iloc[-2:].max()
                    buy_trigger = round(two_day_high * 1.005, 2)
                    stop_loss = round(buy_trigger - (atr * 2.5), 2)
                    
                    if (buy_trigger - stop_loss) / buy_trigger <= 0.12:
                        raw_score = (42 - rsi_window.min()) * 5
                        results.append({
                            'Stock': t, 'Type': 'LONG', 'RSI': int(df['RSI'].iloc[-1]),
                            'Power_Score': round(raw_score * multiplier, 2),
                            'Buy_At': buy_trigger, 'Sell_At': '', 'Days_Since': days_since,
                            'Stop_Loss': stop_loss, 'Target_1': round(buy_trigger * 1.25, 2),
                            'Price': round(close, 2), 'df': df
                        })

                # SETUP B: SHORT (Overbought in Downtrend)
                elif close < sma200 and rsi_window.max() > 58:
                    days_since = len(rsi_window) - rsi_window.argmax() - 1
                    multiplier = get_timing_multiplier(days_since)
                    
                    two_day_low = df['Low'].iloc[-2:].min()
                    sell_trigger = round(two_day_low * 0.995, 2)
                    stop_loss = round(sell_trigger + (atr * 2.5), 2)
                    
                    if (stop_loss - sell_trigger) / sell_trigger <= 0.12:
                        raw_score = (rsi_window.max() - 58) * 5
                        results.append({
                            'Stock': t, 'Type': 'SHORT', 'RSI': int(df['RSI'].iloc[-1]),
                            'Power_Score': round(raw_score * multiplier, 2),
                            'Buy_At': '', 'Sell_At': sell_trigger, 'Days_Since': days_since,
                            'Stop_Loss': stop_loss, 'Target_1': round(sell_trigger * 0.85, 2),
                            'Price': round(close, 2), 'df': df
                        })
            except: continue

        # Sort by the new TIMED Power Score
        final_df = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        sheet_rows = []
        for _, row in final_df.iterrows():
            info = info_map.get(row.Stock, {})
            df_t = row.df
            sheet_rows.append({
                'Stock': row.Stock, 'Squeeze': f"{row.Type} (RSI:{row.RSI})", 'Power_Score': row.Power_Score,
                'Vol_Surge': f"{df_t['Volume'].iloc[-1]/df_t['Volume'].rolling(20).mean().iloc[-1]:.2f}x",
                'Buy_At': row.Buy_At, 'Sell_At': row.Sell_At, 'RSI_Days': row.Days_Since,
                'Stop_Loss': row.Stop_Loss, 'Target_1': row.Target_1,
                'Gross_M': f"{info.get('grossMargins', 0)*100:.0f}%", 'EBIT_M': f"{info.get('ebitdaMargins', 0)*100:.0f}%",
                'Mkt_Cap': f"{info.get('marketCap', 0)/1e9:.1f}B", 'Price': row.Price,
                'YTD': round(((row.Price/df_t['Close'].iloc[0])-1)*100, 1)
            })

        output_df = pd.DataFrame(sheet_rows)

        # 3. UPDATE SHEETS
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn); ws.clear()
            up_df = output_df.head(10) if sn == "Summary" else output_df
            ws.update([up_df.columns.tolist()] + up_df.astype(str).values.tolist())

        # 4. TELEGRAM ALERTS
        for _, row in output_df.head(10).iterrows():
            ticker = row.Stock
            hist = data[ticker].tail(100)
            apds = [mpf.make_addplot(hist['Close'].rolling(200).mean(), color='blue', width=1.5)]
            buf = io.BytesIO()
            mpf.plot(hist, type='candle', addplot=apds, style='charles', savefig=buf)
            buf.seek(0)

            is_long = "LONG" in row.Squeeze
            entry_price = row.Buy_At if is_long else row.Sell_At
            emoji = "🎯 BUY" if is_long else "💀 SELL"

            caption = (
                f"<b>{emoji}: {ticker}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Last:</b> ${row.Price} | ⏳ <b>Age:</b> {row.RSI_Days}d\n"
                f"⚔️ <b>Trigger:</b> ${entry_price}\n"
                f"🛡️ <b>Stop:</b> ${row.Stop_Loss} | 🏁 <b>Target:</b> ${row.Target_1}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <i>Apex Score: {row.Power_Score} | {row.Squeeze}</i>"
            )
            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{ticker}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
            plt.close('all')

    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

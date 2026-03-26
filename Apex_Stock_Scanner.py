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
                current_rsi = df['RSI'].iloc[-1]

                # --- APEX DUAL-SCAN LOGIC ---
                
                # OPTION A: LONG SETUP (Oversold in Uptrend)
                if close > sma200 and current_rsi < 42:
                    two_day_high = df['High'].iloc[-2:].max()
                    buy_trigger = round(two_day_high * 1.005, 2)
                    stop_loss = round(buy_trigger - (atr * 2.5), 2)
                    if (buy_trigger - stop_loss) / buy_trigger <= 0.12:
                        results.append({
                            'Stock': t, 'Squeeze': f"LONG (RSI:{int(current_rsi)})",
                            'Power_Score': round(42 - current_rsi, 2),
                            'Buy_At': buy_trigger, 'Stop_Loss': stop_loss, 'Target_1': round(buy_trigger * 1.25, 2),
                            'Trade_Type': 'LONG', 'Price': round(close, 2), 'df': df
                        })

                # OPTION B: SHORT SETUP (Overbought in Downtrend)
                elif close < sma200 and current_rsi > 58:
                    two_day_low = df['Low'].iloc[-2:].min()
                    sell_trigger = round(two_day_low * 0.995, 2)
                    stop_loss = round(sell_trigger + (atr * 2.5), 2)
                    if (stop_loss - sell_trigger) / sell_trigger <= 0.12:
                        results.append({
                            'Stock': t, 'Squeeze': f"SHORT (RSI:{int(current_rsi)})",
                            'Power_Score': round(current_rsi - 58, 2),
                            'Buy_At': sell_trigger, 'Stop_Loss': stop_loss, 'Target_1': round(sell_trigger * 0.85, 2),
                            'Trade_Type': 'SHORT', 'Price': round(close, 2), 'df': df
                        })
            except: continue

        # Sort and take Top 10
        final_df = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        # Build final output for Sheets
        sheet_data = []
        for _, row in final_df.iterrows():
            info = info_map.get(row.Stock, {})
            df_t = row.df
            sheet_data.append({
                'Stock': row.Stock, 'Squeeze': row.Squeeze, 'Power_Score': row.Power_Score,
                'Vol_Surge': f"{df_t['Volume'].iloc[-1]/df_t['Volume'].rolling(20).mean().iloc[-1]:.2f}x",
                'Buy_At': row.Buy_At, 'Stop_Loss': row.Stop_Loss, 'Target_1': row.Target_1,
                'Gross_M': f"{info.get('grossMargins', 0)*100:.0f}%", 'EBIT_M': f"{info.get('ebitdaMargins', 0)*100:.0f}%",
                'Mkt_Cap': f"{info.get('marketCap', 0)/1e9:.1f}B", 'Price': row.Price,
                'YTD': round(((row.Price/df_t['Close'].iloc[0])-1)*100, 1)
            })

        df_out = pd.DataFrame(sheet_data)

        # 3. UPDATE SHEETS
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn); ws.clear()
            up_df = df_out.head(10) if sn == "Summary" else df_out
            ws.update([up_df.columns.tolist()] + up_df.astype(str).values.tolist())

        # 4. TELEGRAM ALERTS (TOP 10 MIXED)
        for _, row in df_out.head(10).iterrows():
            ticker = row.Stock
            hist = data[ticker].tail(100)
            apds = [mpf.make_addplot(hist['Close'].rolling(200).mean(), color='blue', width=1.5)]
            
            buf = io.BytesIO()
            mpf.plot(hist, type='candle', addplot=apds, style='charles', savefig=buf)
            buf.seek(0)

            emoji = "🎯 BUY" if "LONG" in row.Squeeze else "💀 SELL"
            caption = (
                f"<b>{emoji}: {ticker}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Last Price:</b> ${row.Price}\n"
                f"⚔️ <b>{('Entry' if 'LONG' in emoji else 'Short At')}:</b> ${row.Buy_At}\n"
                f"🛡️ <b>Stop Loss:</b> ${row.Stop_Loss}\n"
                f"🏁 <b>Exit Target:</b> ${row.Target_1}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📊 <i>Apex Score: {row.Power_Score} | {row.Squeeze}</i>"
            )
            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                          files={'photo': (f'{ticker}.png', buf)}, 
                          data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
            plt.close('all')

    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

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
CACHE_FILE = os.path.join(CACHE_DIR, "sp500_apex_v2.pkl")

def get_sp500_tickers():
    """Scrapes tickers and sectors from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        df = wiki_tables[0]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'GICS Sector']].values.tolist()
    except Exception as e:
        print(f"Error scraping tickers: {e}")
        return []

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def run_scanner():
    """APEX V2 SCANNER: SMA-FREE STRUCTURAL PIVOTS + INTRADAY ALPHA"""
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
        
        # Download tickers + SPY for Alpha calculation
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if (data is None or data.empty) and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        elif data is not None and not data.empty:
            data.to_pickle(CACHE_FILE)
        else: return

        # Pre-calculate SPY performance for Alpha
        spy_df = data['SPY'].dropna()
        spy_perf = (spy_df['Close'].iloc[-1] / spy_df['Open'].iloc[-1]) - 1

        results = []
        for t in tkrs:
            try:
                if t not in data.columns.levels[0] or t == "SPY": continue
                df = data[t].dropna().copy()
                if len(df) < 50: continue
                
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                
                # Indicators
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                df['SMA200'] = df['Close'].rolling(200).mean() # Kept for visual charting only
                
                close = float(df['Close'].iloc[-1])
                open_p = float(df['Open'].iloc[-1])
                atr = df['ATR'].iloc[-1]
                rsi_window = df['RSI'].tail(12)
                
                # --- APEX V2 CORE LOGIC ---
                # 1. Alpha Calculation (Stock % vs SPY %)
                stock_perf = (close / open_p) - 1
                alpha = stock_perf - spy_perf
                
                # 2. 3-Day Extremums
                three_day_high = df['High'].tail(3).max()
                three_day_low = df['Low'].tail(3).min()
                
                setup_type, trigger, stop, target, conviction = None, 0, 0, 0, 0

                # LONG: "The Spring" (Mean Reversion + Alpha)
                if rsi_window.min() < 35 and close > three_day_high and alpha > 0.005:
                    setup_type = "PIVOT LONG"
                    conviction = round(((35 - rsi_window.min()) * 5) + (alpha * 1500), 2)
                    trigger = round(three_day_high * 1.002, 2)
                    stop = round(df['Low'].tail(5).min() - (atr * 1.5), 2)
                    target = round(close + (atr * 3), 2)

                # SHORT: "The Trap" (Relative Weakness + Alpha)
                elif rsi_window.max() > 50 and close < three_day_low and alpha < -0.005:
                    setup_type = "PIVOT SHORT"
                    conviction = round(((rsi_window.max() - 45) * 5) + (abs(alpha) * 1500), 2)
                    trigger = round(three_day_low * 0.998, 2)
                    stop = round(df['High'].tail(5).max() + (atr * 1.5), 2)
                    target = round(close - (atr * 3), 2)

                if setup_type:
                    vol_surge = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Setup': setup_type, 'Conviction': conviction,
                        'Alpha_1h': f"{alpha*100:.2f}%", 'Vol_Surge': f"{vol_surge:.2f}x",
                        'Trigger': trigger, 'Stop': stop, 'Target': target, 'Price': round(close, 2),
                        'df_ptr': df, 'conv_val': conviction
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            # Sort by Conviction for both sheets
            df_full = df_full.sort_values(by='conv_val', ascending=False)
            
            # 1. Summary: Top 5 High-Conviction
            df_summary = df_full.head(5)

            # Update Google Sheets
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_full
                final_cols = source_df.drop(columns=['df_ptr', 'conv_val'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())

            # Telegram Notifications for Top 5
            for _, row in df_summary.iterrows():
                t, hist = row.Stock, row['df_ptr'].tail(75)
                sma200_plt = hist['SMA200']
                
                buf = io.BytesIO()
                ap = [mpf.make_addplot(sma200_plt, color='blue', width=1, alpha=0.5)]
                mpf.plot(hist, type='candle', addplot=ap, style='charles', 
                         volume=True, savefig=buf, tight_layout=True)
                buf.seek(0)
                
                icon = "🚀" if "LONG" in row.Setup else "📉"
                caption = (f"<b>{icon} {t} ({row.Sector})</b>\n"
                           f"<b>{row.Setup}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${row.Trigger}\n💰 Price: ${row.Price}\n"
                           f"🛡️ Stop: ${row.Stop}\n🏁 Target: ${row.Target}\n"
                           f"📊 Conviction: {row.Conviction}\n⚡ Alpha: {row.Alpha_1h}")
                
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                              files={'photo': (f'{t}.png', buf)}, 
                              data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')
                
        print(f"✅ Apex V2 Complete. {len(df_full)} high-conviction pivots found.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

import yfinance as yf
import pandas as pd
import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import mplfinance as mpf
import matplotlib.pyplot as plt

CACHE_DIR = "data_cache"
CACHE_FILE = os.path.join(CACHE_DIR, "sp500_apex_2y.pkl")

def get_sp500_tickers():
    """Scrapes tickers and sectors from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        df = wiki_tables[0]
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'GICS Sector']].values.tolist()
    except Exception as e:
        print(f"Ticker Fetch Error: {e}")
        return []

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def apply_pro_styling(sh, ws_name, row_count, col_count):
    """Applies Black Fill/White Text to headers and centers data."""
    ws = sh.worksheet(ws_name)
    # Header range based on dynamic column count
    last_col_letter = chr(64 + col_count)
    header_range = f'A1:{last_col_letter}1'
    
    # Black Header, White Bold Text
    fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    format_cell_range(ws, header_range, fmt)
    
    # Center align body and set column widths
    body_range = f'A2:{last_col_letter}{row_count+1}'
    format_cell_range(ws, body_range, cellFormat(horizontalAlignment='CENTER'))
    set_column_width(ws, 'A:Z', 115)

def run_scanner():
    try:
        # 1. SETUP & AUTH
        token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat = os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        gc = gspread.authorize(creds)
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        if not ticker_data: return
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        # 2. ALPHA BASELINE (SPY 1h PERFORMANCE)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_chg = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1 if not spy_h.empty else 0

        # 3. DATA COLLECTION (2y AS ORIGINAL)
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        # Using 1y for efficiency but logic remains the same
        data = yf.download(tkrs, period="1y", group_by='ticker', progress=False)
        
        if data.empty:
            if os.path.exists(CACHE_FILE):
                data = pd.read_pickle(CACHE_FILE)
            else:
                print("No data returned and no cache found.")
                return
        else:
            data.to_pickle(CACHE_FILE)

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna().copy()
                if len(df) < 50: continue
                
                # PIVOT LOGIC: 3-Day High/Low of PREVIOUS 3 days
                t_high = df['High'].iloc[-4:-1].max()
                t_low = df['Low'].iloc[-4:-1].min()
                
                # ALPHA: Outperforming market over last 5 trading hours
                stock_chg = (df['Close'].iloc[-1] / df['Close'].iloc[-5]) - 1
                alpha = stock_chg - spy_chg
                
                # VOLUME SURGE: Comparing current volume to 20-day average
                vol_avg = df['Volume'].rolling(20).mean().iloc[-1]
                vol_surge = df['Volume'].iloc[-1] / vol_avg
                
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                close, rsi, atr = df['Close'].iloc[-1], df['RSI'].iloc[-1], df['ATR'].iloc[-1]

                setup, score = None, 0

                # LONG: Strength-on-Strength (Finds the Booming Stocks)
                if close > t_high and alpha > 0.005:
                    setup = "LONG 🚀"
                    # Conviction Score (Alpha weighted + Momentum)
                    score = round((alpha * 4000) + (rsi * 0.2) + (vol_surge * 5), 2)
                    trig, stop = t_high, close - (atr * 2)
                    target = close + (abs(close - stop) * 2.5)
                
                # SHORT: Weakness-on-Weakness (Finds the Tanking Stocks)
                elif close < t_low and alpha < -0.005:
                    setup = "SHORT 📉"
                    score = round((abs(alpha) * 4000) + ((100 - rsi) * 0.2) + (vol_surge * 5), 2)
                    trig, stop = t_low, close + (atr * 2)
                    target = close - (abs(stop - close) * 2.5)

                if setup:
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"), 'Type': setup,
                        'Alpha_1h': f"{alpha*100:+.2f}%", 'Vol_Surge': f"{vol_surge:.1f}x",
                        'Score': score, 'Price': round(close, 2), 'Trigger': round(trig, 2),
                        'Stop': round(stop, 2), 'Target': round(target, 2), 'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            # 4. SORTING & FILTERS
            df_full = df_full.sort_values('Score', ascending=False)
            df_summary = df_full.head(5)

            # 5. GOOGLE SHEETS UPDATE
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn)
                ws.clear()
                source_df = df_summary if sn == "Summary" else df_full
                # Drop chart data before upload
                sheet_data = source_df.drop(columns=['df_ptr'])
                ws.update([sheet_data.columns.tolist()] + sheet_data.astype(str).values.tolist())
                # Apply Black/White Header Style
                apply_pro_styling(sh, sn, len(sheet_data), len(sheet_data.columns))

            # 6. TELEGRAM ALERTS
            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                # Plot last 50 days of candles
                mpf.plot(row['df_ptr'].tail(50), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Type} | {row.Stock} ({row.Sector})</b>\n"
                           f"Alpha: {row.Alpha_1h} | Vol: {row.Vol_Surge}\n"
                           f"Score: {row.Score}\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trig: ${row.Trigger}\n🛡️ Stop: ${row.Stop}\n💰 Target: ${row.Target}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                              files={'photo': (f'{row.Stock}.png', buf)}, 
                              data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print(f"✅ Apex Scan Complete. Found {len(df_full)} Candidates.")
    except Exception as e:
        print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

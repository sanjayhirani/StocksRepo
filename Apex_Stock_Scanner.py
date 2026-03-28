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

def apply_formatting(sh, ws_name, row_count):
    """Adds the Black/White header styling."""
    ws = sh.worksheet(ws_name)
    header_fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    format_cell_range(ws, 'A1:J1', header_fmt)
    format_cell_range(ws, f'A2:J{row_count+1}', cellFormat(horizontalAlignment='CENTER'))

def run_scanner():
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        if not ticker_data: return
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        # Alpha Baseline
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1 if not spy_h.empty else 0

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
                if len(df) < 50: continue
                
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                
                # Logic Fix: Compare against previous 3 days (offset current day)
                t_high = df['High'].iloc[-4:-1].max()
                t_low = df['Low'].iloc[-4:-1].min()
                
                close, atr = float(df['Close'].iloc[-1]), df['ATR'].iloc[-1]
                rsi_window = df['RSI'].tail(12)
                
                # Alpha Calc
                stock_change = (close / df['Close'].iloc[-5]) - 1
                alpha = stock_change - spy_change
                
                setup_type, trigger, stop, target, p_score, age = None, 0, 0, 0, 0, 0

                # NEW LONG: Pivot Break + Alpha (Replaces SMA200 filter)
                if close > t_high and alpha > 0.005:
                    setup_type = "LONG"
                    p_score = round((alpha * 4000) + (df['RSI'].iloc[-1] * 0.2), 2)
                    age = int(len(rsi_window) - rsi_window.values.argmin() - 1)
                    trigger = t_high
                    stop = round(close - (atr * 2), 2)
                    target = round(close + (abs(close - stop) * 2.5), 2)
                
                # NEW SHORT: Pivot Breakdown + Alpha
                elif close < t_low and alpha < -0.005:
                    setup_type = "SHORT"
                    p_score = round((abs(alpha) * 4000) + ((100 - df['RSI'].iloc[-1]) * 0.2), 2)
                    age = int(len(rsi_window) - rsi_window.values.argmax() - 1)
                    trigger = t_low
                    stop = round(close + (atr * 2), 2)
                    target = round(close - (abs(stop - close) * 2.5), 2)

                if setup_type:
                    vol_surge = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Setup': f"{setup_type} (Age:{age}d)", 'Alpha': f"{alpha*100:+.2f}%",
                        'Power_Score': p_score, 'Vol_Surge': f"{vol_surge:.2f}x", 
                        'Trigger': trigger, 'Stop_Loss': stop, 'Target': target, 'Price': round(close, 2),
                        'Age_Val': age, 'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_summary = df_full.sort_values(by='Power_Score', ascending=False).head(5)
            df_core = df_full.sort_values(by='Power_Score', ascending=False)
            
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                final_cols = source_df.drop(columns=['df_ptr', 'Age_Val'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())
                apply_formatting(sh, sn, len(final_cols))

            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(50), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                icon = "🚀" if "LONG" in row.Setup else "📉"
                caption = (f"<b>{icon} {row.Stock} ({row.Sector})</b>\n"
                           f"Alpha: {row.Alpha} | Score: {row.Power_Score}\n"
                           f"⚔️ Trigger: ${row.Trigger} | 🛡️ Stop: ${row.Stop_Loss}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{row.Stock}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')
                
        print(f"✅ Apex Complete. Found {len(df_full)} results.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

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

def apply_pro_styling(sh, ws_name, row_count, col_count):
    """FORCED Black Header/White Text styling for Google Sheets."""
    ws = sh.worksheet(ws_name)
    last_col = chr(64 + col_count)
    header_fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    format_cell_range(ws, f'A1:{last_col}1', header_fmt)
    format_cell_range(ws, f'A2:{last_col}{row_count+1}', cellFormat(horizontalAlignment='CENTER'))
    set_column_width(ws, 'A:Z', 120)

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

        # Alpha Baseline (Market vs Stock comparison)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1 if not spy_h.empty else 0

        # Data collection
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs, period="1y", group_by='ticker', progress=False)
        
        if data.empty and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        elif not data.empty:
            data.to_pickle(CACHE_FILE)

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna().copy()
                if len(df) < 50: continue
                
                # Logic: Today's close vs PREVIOUS 3-day high (Allows for breakout detection)
                prev_3d_high = df['High'].iloc[-4:-1].max()
                prev_3d_low = df['Low'].iloc[-4:-1].min()
                
                close = float(df['Close'].iloc[-1])
                stock_change = (close / df['Close'].iloc[-5]) - 1
                alpha = stock_change - spy_change
                
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                rsi_now, atr = df['RSI'].iloc[-1], df['ATR'].iloc[-1]
                
                setup, score = None, 0

                # LONG: Catching the Runners (VLO/MPC Style)
                # Removed RSI < 35 gate. Now triggers on Breakout + Positive Alpha.
                if close > prev_3d_high and alpha > 0.002:
                    setup = "LONG 🚀"
                    score = round((alpha * 5000) + (rsi_now * 0.2), 2)
                    trig, stop = prev_3d_high, close - (atr * 2)
                    target = close + (abs(close - stop) * 2.5)
                
                # SHORT: Catching the Tankers (NKE Style)
                elif close < prev_3d_low and alpha < -0.002:
                    setup = "SHORT 📉"
                    score = round((abs(alpha) * 5000) + ((100 - rsi_now) * 0.2), 2)
                    trig, stop = prev_3d_low, close + (atr * 2)
                    target = close - (abs(stop - close) * 2.5)

                if setup:
                    vol_surge = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"), 'Setup': setup,
                        'Alpha': f"{alpha*100:+.2f}%", 'Vol': f"{vol_surge:.1f}x", 'Score': score,
                        'Price': round(close, 2), 'Trigger': round(trig, 2), 'Stop': round(stop, 2),
                        'Target': round(target, 2), 'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_full = df_full.sort_values('Score', ascending=False)
            df_summary = df_full.head(5)

            # --- SHEETS UPDATE ---
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source = df_summary if sn == "Summary" else df_full
                final_sheet_df = source.drop(columns=['df_ptr'])
                ws.update([final_sheet_df.columns.tolist()] + final_sheet_df.astype(str).values.tolist())
                apply_pro_styling(sh, sn, len(final_sheet_df), len(final_sheet_df.columns))

            # --- TELEGRAM UPDATE ---
            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(50), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Setup} | {row.Stock} ({row.Sector})</b>\n"
                           f"Alpha: {row.Alpha} | Vol: {row.Vol}\nScore: {row.Score}\n"
                           f"━━━━━━━━━━━━━━━━━━━━\n⚔️ Trig: ${row.Trigger}\n🛡️ Stop: ${row.Stop}\n💰 Target: ${row.Target}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{row.Stock}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print(f"✅ Apex Complete. Found {len(df_full)} High-Conviction pivots.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

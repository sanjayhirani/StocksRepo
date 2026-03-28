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

def apply_pro_formatting(sh, ws_name, row_count, col_count):
    """Applies Black Fill/White Text headers and alternating row colors."""
    ws = sh.worksheet(ws_name)
    header_fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    # Target range based on columns provided
    last_col = chr(64 + col_count)
    format_cell_range(ws, f'A1:{last_col}1', header_fmt)
    
    body_fmt = cellFormat(horizontalAlignment='CENTER')
    format_cell_range(ws, f'A2:{last_col}{row_count+1}', body_fmt)

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

        # 1. BASELINE ALPHA (SPY Hourly)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1 if not spy_h.empty else 0

        # 2. CACHED DATA COLLECTION
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
                if len(df) < 50: continue # Min data check
                
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                
                # Pivot Logic: 3-Day Extremes
                t_high, t_low = df['High'].tail(3).max(), df['Low'].tail(3).min()
                
                # Alpha Calculation (Relative to SPY)
                stock_change = (df['Close'].iloc[-1] / df['Close'].iloc[-5]) - 1 
                alpha = stock_change - spy_change
                
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                
                close, atr, rsi_now = float(df['Close'].iloc[-1]), df['ATR'].iloc[-1], df['RSI'].iloc[-1]
                rsi_window = df['RSI'].tail(12)

                setup_type, p_score, age = None, 0, 0

                # LONG: RSI Oversold + 3-Day High Break + Pos Alpha (Day 1 Strength)
                if rsi_window.min() < 42 and close >= t_high and alpha > 0.003:
                    setup_type = "LONG 🚀"
                    p_score = round(((42 - rsi_window.min()) * 4) + (alpha * 1500), 2)
                    age = int(len(rsi_window) - rsi_window.values.argmin() - 1)
                    trigger, stop = t_high, close - (atr * 2)
                    target = round(close + (abs(close - stop) * 2.5), 2)
                
                # SHORT: RSI Overbought + 3-Day Low Break + Neg Alpha (Day 1 Weakness)
                elif rsi_window.max() > 52 and close <= t_low and alpha < -0.003:
                    setup_type = "SHORT 📉"
                    p_score = round(((rsi_window.max() - 52) * 4) + (abs(alpha) * 1500), 2)
                    age = int(len(rsi_window) - rsi_window.values.argmax() - 1)
                    trigger, stop = t_low, close + (atr * 2)
                    target = round(close - (abs(stop - close) * 2.5), 2)

                if setup_type:
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Setup': setup_type, 'Alpha': f"{alpha*100:+.2f}%", 'Power_Score': p_score,
                        'Price': round(close, 2), 'Trigger': round(trigger, 2),
                        'Stop_Loss': round(stop, 2), 'Target': target,
                        'Age_Val': age, 'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            # Sector Analysis (For Summary Header)
            sentiment = df_full.groupby(['Sector', 'Setup']).size().unstack(fill_value=0)
            
            # Summary: Top 5 by Power Score (Strict 2-5 Day Squeeze)
            df_summary = df_full[(df_full['Age_Val'] >= 2) & (df_full['Age_Val'] <= 5)].copy()
            df_summary = df_summary.sort_values(by='Power_Score', ascending=False).head(5)

            # Core: Everything else sorted by Conviction
            df_core = df_full.sort_values(by='Power_Score', ascending=False)

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                
                # Drop chart pointers for the sheet
                final_cols = source_df.drop(columns=['df_ptr', 'Age_Val'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())
                apply_pro_formatting(sh, sn, len(final_cols), len(final_cols.columns))

            # Telegram Top 5 Alerts
            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(60), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Setup} | {row.Stock} ({row.Sector})</b>\n"
                           f"Alpha: {row.Alpha} | Score: {row.Power_Score}\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${row.Trigger}\n🛡️ Stop: ${row.Stop_Loss}\n🏁 Target: ${row.Target}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{row.Stock}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print(f"✅ Apex Complete. Found {len(df_full)} High-Conviction pivots.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

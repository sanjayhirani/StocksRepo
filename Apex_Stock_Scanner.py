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

def apply_sheet_style(sh, ws_name, row_count):
    """Applies Black Fill / White Text to headers."""
    ws = sh.worksheet(ws_name)
    # Header Styling
    fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    format_cell_range(ws, 'A1:I1', fmt)
    # Body Alignment
    format_cell_range(ws, f'A2:I{row_count+1}', cellFormat(horizontalAlignment='CENTER'))
    set_column_width(ws, 'A:I', 110)

def run_scanner():
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        # Alpha Baseline (Hourly SPY)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1 if not spy_h.empty else 0

        # Data collection (2y as original)
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
                
                # Pivot Logic (3-Day Extremes)
                t_high, t_low = df['High'].tail(3).max(), df['Low'].tail(3).min()
                
                # Stock Alpha (1-Day relative)
                stock_chg = (df['Close'].iloc[-1] / df['Close'].iloc[-5]) - 1
                alpha = stock_chg - spy_change
                
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                close, rsi_now, atr = df['Close'].iloc[-1], df['RSI'].iloc[-1], df['ATR'].iloc[-1]

                setup, score = None, 0

                # LONG: Strength-on-Strength (Finds VLO/MPC)
                if close >= t_high and alpha > 0.005:
                    setup = "LONG 🚀"
                    score = round((rsi_now * 0.5) + (alpha * 2000), 2)
                    trigger, stop = t_high, close - (atr * 2)
                    target = close + (abs(close - stop) * 2.5)
                
                # SHORT: Weakness-on-Weakness (Finds NKE/BSX)
                elif close <= t_low and alpha < -0.005:
                    setup = "SHORT 📉"
                    score = round(((100 - rsi_now) * 0.5) + (abs(alpha) * 2000), 2)
                    trigger, stop = t_low, close + (atr * 2)
                    target = close - (abs(stop - close) * 2.5)

                if setup:
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"), 'Type': setup,
                        'Alpha_1h': f"{alpha*100:+.2f}%", 'Score': score,
                        'Price': round(close, 2), 'Trigger': round(trigger, 2),
                        'Stop': round(stop, 2), 'Target': round(target, 2), 'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_summary = df_full.sort_values('Score', ascending=False).head(5)
            df_core = df_full.sort_values(['Sector', 'Score'], ascending=[True, False])

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                cols = ['Stock', 'Sector', 'Type', 'Alpha_1h', 'Score', 'Price', 'Trigger', 'Stop', 'Target']
                final = source_df[cols]
                ws.update([final.columns.tolist()] + final.astype(str).values.tolist())
                apply_sheet_style(sh, sn, len(final))

            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(60), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Type} | {row.Stock}</b>\nAlpha: {row.Alpha_1h} | Score: {row.Score}\n"
                           f"⚔️ Trig: ${row.Trigger} | 🛡️ Stop: ${row.Stop}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{row.Stock}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print(f"✅ Apex Update Complete. Found {len(df_full)} results.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

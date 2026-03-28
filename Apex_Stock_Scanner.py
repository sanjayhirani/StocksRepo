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
CACHE_FILE = os.path.join(CACHE_DIR, "sp500_apex_v2.pkl")

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

def format_sheets(sh, worksheet_name):
    """Applies Black/White styling and professional formatting."""
    ws = sh.worksheet(worksheet_name)
    fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    format_cell_range(ws, 'A1:J1', fmt)
    set_column_width(ws, 'A:J', 120)
    # Alternating row colors (Light Grey)
    rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range('A2:J100', ws)],
        booleanRule=BooleanRule(condition=BooleanCondition('CUSTOM_FORMULA', ['=ISODD(ROW())']), format=cellFormat(backgroundColor=color(0.95, 0.95, 0.95)))
    )
    get_conditional_format_rules(ws).append(rule)
    get_conditional_format_rules(ws).save()

def run_scanner():
    """S&P 500 APEX - SMA-FREE PIVOT ENGINE"""
    try:
        token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat = os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        # 1. Market Baseline (Hourly Alpha)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_4h_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1

        # 2. Daily Data (Cached)
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs, period="1y", group_by='ticker', progress=False)
        if data is not None and not data.empty: data.to_pickle(CACHE_FILE)
        elif os.path.exists(CACHE_FILE): data = pd.read_pickle(CACHE_FILE)

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna().copy()
                if len(df) < 50: continue
                
                # Pivot Logic: 3-Day High/Low
                three_day_h = df['High'].tail(3).max()
                three_day_l = df['Low'].tail(3).min()
                
                # Fresh Alpha pull
                df_h = yf.download(t, period="2d", interval="1h", progress=False)
                alpha = (df_h['Close'].iloc[-1] / df_h['Close'].iloc[-5] - 1) - spy_4h_change
                
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                close, atr, rsi_now = float(df['Close'].iloc[-1]), df['ATR'].iloc[-1], df['RSI'].iloc[-1]

                setup_type, conviction = None, 0
                
                # SMA-FREE LONG: Price breaks 3-day high + Oversold + Positive Alpha
                if rsi_now < 40 and close >= three_day_h and alpha > 0.005:
                    setup_type = "LONG 🚀"
                    conviction = ((40 - rsi_now) * 5) + (alpha * 2000)
                    trigger, stop = three_day_h, close - (atr * 2)
                    target = close + (abs(close - stop) * 2)
                
                # SMA-FREE SHORT: Price breaks 3-day low + Overbought + Negative Alpha
                elif rsi_now > 55 and close <= three_day_l and alpha < -0.005:
                    setup_type = "SHORT 📉"
                    conviction = ((rsi_now - 50) * 5) + (abs(alpha) * 2000)
                    trigger, stop = three_day_l, close + (atr * 2)
                    target = close - (abs(stop - close) * 2)

                if setup_type:
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Setup': setup_type, 'Alpha_1h': f"{alpha*100:+.2f}%",
                        'Conviction': round(conviction, 2), 'Price': round(close, 2),
                        'Trigger': round(trigger, 2), 'Stop': round(stop, 2), 'Target': round(target, 2),
                        'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_summary = df_full.sort_values('Conviction', ascending=False).head(5)
            df_core = df_full.sort_values(['Sector', 'Conviction'], ascending=[True, False])

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                final_cols = source_df[['Stock', 'Sector', 'Setup', 'Alpha_1h', 'Conviction', 'Price', 'Trigger', 'Stop', 'Target']]
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())
                format_sheets(sh, sn)

            # Telegram Alerts
            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(60), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Setup} | {row.Stock} ({row.Sector})</b>\n"
                           f"Alpha: {row.Alpha_1h} | Conviction: {row.Conviction}\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${row.Trigger}\n🛡️ Stop: ${row.Stop}\n💰 Target: ${row.Target}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{row.Stock}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

    except Exception as e: print(f"Error: {e}")

if __name__ == "__main__":
    run_scanner()

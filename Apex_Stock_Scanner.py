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
CACHE_FILE = os.path.join(CACHE_DIR, "sp500_apex_v3.pkl")

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
    """Applies Black/White Professional Styling."""
    ws = sh.worksheet(ws_name)
    # Header: Black Background, White Bold Text
    header_fmt = cellFormat(
        backgroundColor=color(0, 0, 0),
        textFormat=textFormat(bold=True, foregroundColor=color(1, 1, 1)),
        horizontalAlignment='CENTER'
    )
    format_cell_range(ws, 'A1:J1', header_fmt)
    
    # Grid and Alignment
    body_fmt = cellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
    format_cell_range(ws, f'A2:J{row_count+1}', body_fmt)
    
    # Alternating Row Colors
    rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f'A2:J{row_count+1}', ws)],
        booleanRule=BooleanRule(
            condition=BooleanCondition('CUSTOM_FORMULA', ['=ISODD(ROW())']),
            format=cellFormat(backgroundColor=color(0.96, 0.96, 0.96))
        )
    )
    get_conditional_format_rules(ws).append(rule)
    get_conditional_format_rules(ws).save()

def run_scanner():
    try:
        token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat = os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        tkrs = [x[0] for x in ticker_data]
        sector_map = {x[0]: x[1] for x in ticker_data}

        # 1. Market Alpha Baseline (1h Interval)
        spy_h = yf.download("SPY", period="2d", interval="1h", progress=False)
        spy_change = (spy_h['Close'].iloc[-1] / spy_h['Close'].iloc[-5]) - 1

        # 2. Data Pull (Daily)
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs, period="1y", group_by='ticker', progress=False)
        data.to_pickle(CACHE_FILE)

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna().copy()
                if len(df) < 60: continue
                
                # 3-Day Pivot
                t_high, t_low = df['High'].tail(3).max(), df['Low'].tail(3).min()
                
                # Intraday Alpha Check
                df_h = yf.download(t, period="2d", interval="1h", progress=False)
                alpha = ((df_h['Close'].iloc[-1] / df_h['Close'].iloc[-5]) - 1) - spy_change
                
                df['RSI'] = calculate_rsi(df['Close'])
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                close, rsi_now, atr = df['Close'].iloc[-1], df['RSI'].iloc[-1], df['ATR'].iloc[-1]

                setup = None
                if rsi_now < 42 and close >= t_high and alpha > 0.004:
                    setup, icon = "LONG", "🚀"
                    score = ((42 - rsi_now) * 4) + (alpha * 2500)
                    trig, stop = t_high, close - (atr * 2)
                elif rsi_now > 52 and close <= t_low and alpha < -0.004:
                    setup, icon = "SHORT", "📉"
                    score = ((rsi_now - 52) * 4) + (abs(alpha) * 2500)
                    trig, stop = t_low, close + (atr * 2)

                if setup:
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"), 'Type': icon + " " + setup,
                        'Alpha_1h': f"{alpha*100:+.2f}%", 'Conviction': round(score, 2),
                        'Price': round(close, 2), 'Trigger': round(trig, 2), 
                        'Stop': round(stop, 2), 'Target': round(close + (close-stop)*2 if setup=="LONG" else close - (stop-close)*2, 2),
                        'df_ptr': df
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            # Sector Sentiment Analysis
            sentiment = df_full.groupby('Sector')['Type'].value_counts().unstack().fillna(0)
            sentiment['Bias'] = sentiment.apply(lambda x: "BULLISH" if x.get('🚀 LONG', 0) > x.get('📉 SHORT', 0) else "BEARISH", axis=1)
            
            df_summary = df_full.sort_values('Conviction', ascending=False).head(5)
            df_core = df_full.sort_values(['Sector', 'Conviction'], ascending=[True, False])

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                cols = ['Stock', 'Sector', 'Type', 'Alpha_1h', 'Conviction', 'Price', 'Trigger', 'Stop', 'Target']
                final = source_df[cols]
                ws.update([final.columns.tolist()] + final.astype(str).values.tolist())
                apply_pro_formatting(sh, sn, len(final), len(cols))

            # Telegram Top 5
            for _, row in df_summary.iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(50), type='candle', style='charles', savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Type} | {row.Stock} ({row.Sector})</b>\n"
                           f"Alpha: {row.Alpha_1h} | Score: {row.Conviction}\n"
                           f"⚔️ Trigger: ${row.Trigger}\n🛡️ Stop: ${row.Stop}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

    except Exception as e: print(f"Error: {e}")

if __name__ == "__main__":
    run_scanner()

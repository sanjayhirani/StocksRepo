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
CACHE_FILE = os.path.join(CACHE_DIR, "sp500_apex_2y.pkl")

def get_sp500_tickers():
    """ORIGINAL METHOD: Scrapes tickers and sectors from the S&P 500 Wikipedia table."""
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

def format_headers(ws, col_count):
    """Applies black background and white bold text to the first row."""
    header_range = f'A1:{gspread.utils.rowcol_to_a1(1, col_count)}'
    ws.format(header_range, {
        "backgroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0},
        "horizontalAlignment": "CENTER",
        "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "bold": True}
    })

def run_scanner():
    """S&P 500 APEX SCANNER - FULL 500 + SECTORS + FORMATTED TRADE JOURNAL"""
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
                if len(df) < 210: continue
                
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                df['RSI'] = calculate_rsi(df['Close'])
                df['SMA200'] = df['Close'].rolling(200).mean()
                df['ATR'] = (df['High'] - df['Low']).rolling(14).mean()
                
                close, sma200, atr = float(df['Close'].iloc[-1]), df['SMA200'].iloc[-1], df['ATR'].iloc[-1]
                rsi_window = df['RSI'].tail(12)
                
                setup_type, trigger, stop, target, p_score, age = None, 0, 0, 0, 0, 0

                if close > sma200 and rsi_window.min() < 35:
                    setup_type = "LONG"
                    p_score = round((40 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.values.argmin() - 1)
                    trigger = round(df['High'].tail(3).max() * 1.005, 2)
                    stop = round(df['Low'].tail(5).min() - (atr * 1.5), 2)
                    target = round(close + (abs(close - stop) * 2.0), 2)
                
                elif close < sma200 and rsi_window.max() > 65:
                    setup_type = "SHORT"
                    p_score = round((rsi_window.max() - 60) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.values.argmax() - 1)
                    trigger = round(df['Low'].tail(3).min() * 0.995, 2)
                    stop = round(df['High'].tail(5).max() + (atr * 1.5), 2)
                    target = round(close - (abs(stop - close) * 2.0), 2)

                if setup_type:
                    dist = abs(close - trigger) / close
                    vol_surge = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Squeeze': f"{setup_type} (Age:{age}d)", 'Power_Score': p_score,
                        'Vol_Surge': f"{vol_surge:.2f}x", 
                        'Buy_At': trigger if setup_type == "LONG" else "",
                        'Sell_At': trigger if setup_type == "SHORT" else "",
                        'Stop_Loss': stop, 'Target': target, 'Price': round(close, 2),
                        'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                        'Dist_To_Trigger': dist, 'Age_Val': age,
                        'df_ptr': df, 'sma200_val': df['SMA200'], 'Type': setup_type
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_summary = df_full[(df_full['Age_Val'] >= 2) & (df_full['Age_Val'] <= 5)].copy()
            df_summary = df_summary.sort_values(by=['Power_Score', 'Dist_To_Trigger'], ascending=[False, True]).head(5)
            df_core = df_full.sort_values(by=['Dist_To_Trigger', 'Power_Score'], ascending=[True, False])
            
            # --- UPDATE SHEETS: SUMMARY & CORE ---
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                final_cols = source_df.drop(columns=['df_ptr', 'sma200_val', 'Dist_To_Trigger', 'Age_Val', 'Type'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())
                format_headers(ws, len(final_cols.columns))

            # --- TRADE JOURNAL LOGIC ---
            try:
                try: ws_j = sh.worksheet("Trade Journal")
                except: ws_j = sh.add_worksheet(title="Trade Journal", rows="1000", cols="20")
                
                existing_data = ws_j.get_all_records()
                df_j = pd.DataFrame(existing_data) if existing_data else pd.DataFrame(columns=['Stock', 'Date', 'Type', 'Trigger', 'Stop', 'Target', 'Status', 'Price_Now', 'PL_Pct'])

                for idx, row in df_j.iterrows():
                    if row['Status'] in ['ACTIVE', 'PENDING']:
                        hist = yf.download(row['Stock'], period="5d", progress=False)
                        if not hist.empty:
                            curr_close, w_low, w_high = hist['Close'].iloc[-1], hist['Low'].min(), hist['High'].max()
                            if row['Status'] == 'PENDING':
                                if (row['Type'] == 'LONG' and w_high >= row['Trigger']) or (row['Type'] == 'SHORT' and w_low <= row['Trigger']):
                                    df_j.at[idx, 'Status'] = 'ACTIVE'
                            if df_j.at[idx, 'Status'] == 'ACTIVE':
                                if (row['Type'] == 'LONG' and w_low <= row['Stop']) or (row['Type'] == 'SHORT' and w_high >= row['Stop']):
                                    df_j.at[idx, 'Status'], df_j.at[idx, 'Price_Now'] = 'STOPPED OUT', row['Stop']
                                elif (row['Type'] == 'LONG' and w_high >= row['Target']) or (row['Type'] == 'SHORT' and w_low <= row['Target']):
                                    df_j.at[idx, 'Status'], df_j.at[idx, 'Price_Now'] = 'TARGET HIT', row['Target']
                                else:
                                    df_j.at[idx, 'Price_Now'] = round(float(curr_close), 2)
                                
                                # Calculate % Return
                                entry = row['Trigger']
                                exit_p = df_j.at[idx, 'Price_Now']
                                df_j.at[idx, 'PL_Pct'] = round(((exit_p - entry) / entry * 100) if row['Type'] == 'LONG' else ((entry - exit_p) / entry * 100), 2)

                if not df_summary.empty:
                    top = df_summary.iloc[0]
                    new_row = {'Stock': top['Stock'], 'Date': datetime.now().strftime("%Y-%m-%d"), 'Type': top['Type'], 
                               'Trigger': top['Buy_At'] if top['Type'] == "LONG" else top['Sell_At'],
                               'Stop': top['Stop_Loss'], 'Target': top['Target'], 'Status': 'PENDING', 'Price_Now': top['Price'], 'PL_Pct': 0.0}
                    df_j = pd.concat([df_j, pd.DataFrame([new_row])], ignore_index=True)

                ws_j.clear()
                ws_j.update([df_j.columns.tolist()] + df_j.astype(str).values.tolist())
                format_headers(ws_j, len(df_j.columns))

                # Dashboard (Visual Style)
                w, l, p = len(df_j[df_j['Status']=='TARGET HIT']), len(df_j[df_j['Status']=='STOPPED OUT']), len(df_j[df_j['Status']=='PENDING'])
                dash_data = [["PERFORMANCE DASHBOARD", ""], ["Wins ✅", w], ["Losses ❌", l], ["Pending ⏳", p]]
                ws_j.update("L1:M4", dash_data)
                ws_j.format("L1:M1", {"backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.2}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}})
            except Exception as je: print(f"Journal Error: {je}")

            # --- TELEGRAM ALERTS ---
            for _, row in df_summary.iterrows():
                t, hist, sma200_plt = row.Stock, row['df_ptr'].tail(100), row['sma200_val'].tail(100)
                buf = io.BytesIO()
                mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(sma200_plt, color='blue', width=1.5)], 
                         style='charles', volume=True, savefig=buf, tight_layout=True)
                buf.seek(0)
                icon = "🛡️" if "LONG" in row.Squeeze else "⚠️"
                trigger_val = row.Buy_At if row.Buy_At else row.Sell_At
                caption = (f"<b>{icon} {t} ({row.Sector})</b>\n"
                           f"<b>{row.Squeeze}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${trigger_val}\n💰 Price: ${row.Price}\n"
                           f"🛡️ Stop: ${row.Stop_Loss}\n🏁 Target: ${row.Target}\n📊 Power Score: {row.Power_Score}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')
                
        print(f"✅ Apex S&P 500 Scanner Complete. {len(df_full)} results logged.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

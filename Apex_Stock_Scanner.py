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

def apply_pro_formatting(ws, df, is_journal=False):
    cols = len(df.columns)
    rows = len(df) + 1
    last_col_letter = gspread.utils.rowcol_to_a1(1, cols).replace("1", "")
    
    # 1. Main Header Styling
    ws.format(f"A1:{last_col_letter}1", {
        "backgroundColor": {"red": 0.05, "green": 0.05, "blue": 0.05},
        "horizontalAlignment": "CENTER",
        "textFormat": {"foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}, "bold": True, "fontSize": 10}
    })
    
    # 2. Data Alignment & Font
    if rows > 1:
        ws.format(f"A2:{last_col_letter}{rows}", {
            "textFormat": {"foregroundColor": {"red": 0.1, "green": 0.1, "blue": 0.1}, "fontSize": 9},
            "verticalAlignment": "MIDDLE", "horizontalAlignment": "CENTER"
        })

    # 3. Timestamp (Top Right)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    ws.update("J1", [[f"Last Sync: {now_str}"]])
    ws.format("J1", {"textFormat": {"italic": True, "fontSize": 8, "foregroundColor": {"red": 0.4, "green": 0.4, "blue": 0.4}}})

    # 4. Conditional Rules for Journal
    if is_journal and rows > 1:
        ws.conditional_format_rule(f"G2:G{rows}", {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "TARGET HIT"}], "style": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}, "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.4, "blue": 0.0}, "bold": True}}})
        ws.conditional_format_rule(f"G2:G{rows}", {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "STOPPED OUT"}], "style": {"backgroundColor": {"red": 0.98, "green": 0.85, "blue": 0.85}, "textFormat": {"foregroundColor": {"red": 0.6, "green": 0.0, "blue": 0.0}, "bold": True}}})
        ws.conditional_format_rule(f"I2:I{rows}", {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}], "style": {"textFormat": {"foregroundColor": {"red": 0.0, "green": 0.5, "blue": 0.0}, "bold": True}}})
        ws.conditional_format_rule(f"I2:I{rows}", {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}], "style": {"textFormat": {"foregroundColor": {"red": 0.7, "green": 0.0, "blue": 0.0}, "bold": True}}})

def run_scanner():
    try:
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        ticker_data = get_sp500_tickers()
        if not ticker_data: return
        
        tkrs = [x[0] for x in ticker_data]; sector_map = {x[0]: x[1] for x in ticker_data}
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if (data is None or data.empty) and os.path.exists(CACHE_FILE): data = pd.read_pickle(CACHE_FILE)
        elif data is not None and not data.empty: data.to_pickle(CACHE_FILE)
        else: return

        results = []
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                df.index = pd.to_datetime(df.index); df = df.sort_index()
                df['RSI'], df['SMA200'], df['ATR'] = calculate_rsi(df['Close']), df['Close'].rolling(200).mean(), (df['High'] - df['Low']).rolling(14).mean()
                close, sma200, atr = float(df['Close'].iloc[-1]), df['SMA200'].iloc[-1], df['ATR'].iloc[-1]
                rsi_window = df['RSI'].tail(12)
                
                setup_type, trigger, stop, target, p_score, age = None, 0, 0, 0, 0, 0
                if close > sma200 and rsi_window.min() < 35:
                    setup_type, p_score = "LONG", round((40 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.values.argmin() - 1)
                    trigger, stop = round(df['High'].tail(3).max() * 1.005, 2), round(df['Low'].tail(5).min() - (atr * 1.5), 2)
                    target = round(close + (abs(close - stop) * 2.0), 2)
                elif close < sma200 and rsi_window.max() > 65:
                    setup_type, p_score = "SHORT", round((rsi_window.max() - 60) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.values.argmax() - 1)
                    trigger, stop = round(df['Low'].tail(3).min() * 0.995, 2), round(df['High'].tail(5).max() + (atr * 1.5), 2)
                    target = round(close - (abs(stop - close) * 2.0), 2)

                if setup_type:
                    results.append({
                        'Stock': t, 'Sector': sector_map.get(t, "N/A"),
                        'Setup': f"{'🔼' if setup_type=='LONG' else '🔽'} {setup_type}",
                        'Score': p_score, 'Vol': f"{df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]:.2f}x", 
                        'Entry': trigger, 'Stop': stop, 'Target': target, 'Price': round(close, 2),
                        'Dist_To_Trigger': abs(close - trigger) / close, 'Age_Val': age,
                        'df_ptr': df, 'sma200_val': df['SMA200'], 'Type': setup_type
                    })
            except: continue

        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_summary = df_full[(df_full['Age_Val'] >= 2) & (df_full['Age_Val'] <= 5)].sort_values(by=['Score', 'Dist_To_Trigger'], ascending=[False, True]).head(5)
            df_core = df_full.sort_values(by=['Dist_To_Trigger', 'Score'], ascending=[True, False])
            
            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn); ws.clear()
                source_df = df_summary if sn == "Summary" else df_core
                final = source_df.drop(columns=['df_ptr', 'sma200_val', 'Dist_To_Trigger', 'Age_Val', 'Type'])
                ws.update([final.columns.tolist()] + final.astype(str).values.tolist())
                apply_pro_formatting(ws, final)

            # --- TRADE JOURNAL ---
            try:
                try: ws_j = sh.worksheet("Trade Journal")
                except: ws_j = sh.add_worksheet(title="Trade Journal", rows="1000", cols="20")
                
                journal_cols = ['Stock', 'Date', 'Dir', 'Entry', 'Stop', 'Target', 'Status', 'Price_Now', 'PL_Pct']
                raw_j = ws_j.get_values("A1:I500") 
                df_j = pd.DataFrame(raw_j[1:], columns=raw_j[0]) if len(raw_j) > 1 and raw_j[0] == journal_cols else pd.DataFrame(columns=journal_cols)

                for idx, row in df_j.iterrows():
                    if row['Status'] in ['ACTIVE', 'PENDING']:
                        hist = yf.download(row['Stock'], period="5d", progress=False)
                        if not hist.empty:
                            close_col = hist['Close'].iloc[:, 0] if isinstance(hist['Close'], pd.DataFrame) else hist['Close']
                            low_col = hist['Low'].iloc[:, 0] if isinstance(hist['Low'], pd.DataFrame) else hist['Low']
                            high_col = hist['High'].iloc[:, 0] if isinstance(hist['High'], pd.DataFrame) else hist['High']
                            
                            curr, low, high = float(close_col.iloc[-1]), float(low_col.min()), float(high_col.max())
                            ent, stp, tgt = float(row['Entry']), float(row['Stop']), float(row['Target'])
                            
                            if row['Status'] == 'PENDING':
                                if (row['Dir'] == 'LONG' and high >= ent) or (row['Dir'] == 'SHORT' and low <= ent): df_j.at[idx, 'Status'] = 'ACTIVE'
                            if df_j.at[idx, 'Status'] == 'ACTIVE':
                                if (row['Dir'] == 'LONG' and low <= stp) or (row['Dir'] == 'SHORT' and high >= stp): 
                                    df_j.at[idx, 'Status'], df_j.at[idx, 'Price_Now'] = 'STOPPED OUT', stp
                                elif (row['Dir'] == 'LONG' and high >= tgt) or (row['Dir'] == 'SHORT' and low <= tgt): 
                                    df_j.at[idx, 'Status'], df_j.at[idx, 'Price_Now'] = 'TARGET HIT', tgt
                                else: df_j.at[idx, 'Price_Now'] = round(curr, 2)
                                exit_val = float(df_j.at[idx, 'Price_Now'])
                                df_j.at[idx, 'PL_Pct'] = round(((exit_val - ent)/ent*100) if row['Dir']=='LONG' else ((ent - exit_val)/ent*100), 2)

                if not df_summary.empty:
                    top = df_summary.iloc[0]
                    new_row = {'Stock': top['Stock'], 'Date': datetime.now().strftime("%m/%d"), 'Dir': top['Type'], 'Entry': top['Entry'], 'Stop': top['Stop'], 'Target': top['Target'], 'Status': 'PENDING', 'Price_Now': top['Price'], 'PL_Pct': 0.0}
                    df_j = pd.concat([df_j, pd.DataFrame([new_row])], ignore_index=True)

                ws_j.clear()
                ws_j.update([df_j.columns.tolist()] + df_j.astype(str).values.tolist())
                apply_pro_formatting(ws_j, df_j, is_journal=True)

                # Dashboard (K1:L5)
                w, l = len(df_j[df_j['Status']=='TARGET HIT']), len(df_j[df_j['Status']=='STOPPED OUT'])
                total_closed = w + l
                win_rate = round((w / total_closed * 100), 1) if total_closed > 0 else 0
                total_pl = round(df_j['PL_Pct'].astype(float).sum(), 2)
                
                dash = [["KPI TERMINAL", ""], ["Wins ✅", w], ["Losses ❌", l], ["Win Rate %", f"{win_rate}%"], ["Total P/L", f"{total_pl}%"]]
                ws_j.update("K1:L5", dash)
                ws_j.format("K1:L1", {"backgroundColor": {"red": 0.0, "green": 0.2, "blue": 0.4}, "textFormat": {"foregroundColor": {"red": 1, "green": 1, "blue": 1}, "bold": True}})
                ws_j.format("K5", {"textFormat": {"bold": True, "foregroundColor": {"red": 0, "green": 0.5, "blue": 0}} if total_pl >= 0 else {"foregroundColor": {"red": 0.7, "green": 0, "blue": 0}}})
            except Exception as je: print(f"Journal Error: {je}")

            # --- TELEGRAM ---
            for _, row in df_summary.iterrows():
                t, hist, sma200_plt = row.Stock, row['df_ptr'].tail(100), row['sma200_val'].tail(100)
                buf = io.BytesIO()
                mpf.plot(hist, type='candle', addplot=[mpf.make_addplot(sma200_plt, color='blue', width=1.5)], style='charles', volume=True, savefig=buf, tight_layout=True)
                buf.seek(0)
                caption = (f"<b>{'🛡️' if 'LONG' in row.Setup else '⚠️'} {t} ({row.Sector})</b>\n<b>{row.Setup}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"⚔️ Trigger: ${row.Entry}\n💰 Price: ${row.Price}\n🛡️ Stop: ${row.Stop}\n🏁 Target: ${row.Target}\n📊 Score: {row.Score}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{t}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')
                
        print(f"✅ Apex S&P 500 Scanner Complete.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

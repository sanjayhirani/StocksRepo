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
CACHE_FILE = os.path.join(CACHE_DIR, "russell_1000_2y.pkl")

def get_russell_1000():
    url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    try:
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        for table in wiki_tables:
            if 'Symbol' in table.columns:
                return [str(t).strip().replace('.', '-') for t in table['Symbol'].tolist()]
    except: return []

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def run_backtest_on_sheet(sh):
    try:
        ws_bt = sh.worksheet("Backtester")
    except:
        ws_bt = sh.add_worksheet(title="Backtester", rows="200", cols="20")
    
    ticker = ws_bt.acell('A2').value
    if not ticker: ticker = "SPY"

    df = yf.download(ticker, period="2y", progress=False)
    if df.empty: return

    df['RSI'] = calculate_rsi(df['Close'])
    df['Pct_Chg'] = df['Close'].pct_change()
    
    df['Signal'] = 0
    df.loc[df['RSI'] < 32, 'Signal'] = 1
    df.loc[df['RSI'] > 68, 'Signal'] = -1
    
    df['Strategy_Ret'] = df['Signal'].shift(1) * df['Pct_Chg']
    df['Cum_Strategy'] = (1 + df['Strategy_Ret'].fillna(0)).cumprod() - 1
    df['Cum_Hold'] = (1 + df['Pct_Chg'].fillna(0)).cumprod() - 1
    
    trades = df[df['Signal'].shift(1) != 0]
    win_rate = (len(trades[trades['Strategy_Ret'] > 0]) / len(trades) * 100) if len(trades) > 0 else 0
    
    stats = [
        ["METRIC", "VALUE"],
        ["Win Rate", f"{win_rate:.2f}%"],
        ["Strategy Ret", f"{df['Cum_Strategy'].iloc[-1]*100:.2f}%"],
        ["Buy & Hold", f"{df['Cum_Hold'].iloc[-1]*100:.2f}%"],
        ["Alpha", f"{(df['Cum_Strategy'].iloc[-1] - df['Cum_Hold'].iloc[-1])*100:.2f}%"]
    ]
    # Update stats dashboard
    ws_bt.update('D1:E5', stats)

    # Prepare backtest data for upload - removing multi-index issues
    bt_display = df[['Close', 'RSI', 'Signal', 'Cum_Strategy']].tail(100).reset_index()
    bt_display.columns = ['Date', 'Close', 'RSI', 'Signal', 'Cum_Strategy'] # Explicit headers
    bt_display['Date'] = bt_display['Date'].dt.strftime('%Y-%m-%d')
    
    # Convert to list of strings (following original script's logic)
    upload_data = [bt_display.columns.tolist()] + bt_display.astype(str).values.tolist()
    ws_bt.update('A9', upload_data)

def run_scanner():
    try:
        # 1. AUTH & SHEET SETUP
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        run_backtest_on_sheet(sh)

        tkrs = get_russell_1000()
        if not tkrs: return

        # 2. DATA DOWNLOAD & CACHE (EXACT ORIGINAL CODE)
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        if (data is None or data.empty) and os.path.exists(CACHE_FILE):
            data = pd.read_pickle(CACHE_FILE)
        elif data is not None and not data.empty:
            data.to_pickle(CACHE_FILE)
        else:
            return

        results = []

        # 3. APEX LOGIC (Target SMA200 / Wilder's RSI)
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                df.index = pd.to_datetime(df.index)
                df['RSI'] = calculate_rsi(df['Close'])
                sma200_val = df['Close'].rolling(200).mean().iloc[-1]
                atr = (df['High']-df['Low']).rolling(14).mean().iloc[-1]
                close = float(df['Close'].iloc[-1])
                
                rsi_window = df['RSI'].tail(12)
                setup_type = None
                
                if rsi_window.min() < 32:
                    setup_type = "LONG"
                    p_score = round((32 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmin() - 1)
                    trigger = round(df['High'].tail(2).max() * 1.005, 2)
                    stop = round(trigger - (atr * 2.5), 2)
                    target = round(sma200_val, 2) 
                elif rsi_window.max() > 68:
                    setup_type = "SHORT"
                    p_score = round((rsi_window.max() - 68) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmax() - 1)
                    trigger = round(df['Low'].tail(2).min() * 0.995, 2)
                    stop = round(trigger + (atr * 2.5), 2)
                    target = round(sma200_val, 2)

                if setup_type and (abs(trigger - stop) / trigger) <= 0.12:
                    vol_surge = df['Volume'].iloc[-1]/df['Volume'].rolling(20).mean().iloc[-1]
                    results.append({
                        'Stock': t, 
                        'Squeeze': f"{setup_type} (Age:{age}d)", 
                        'Power_Score': p_score,
                        'Vol_Surge': f"{vol_surge:.2f}x",
                        'Buy_At': trigger if setup_type == "LONG" else "",
                        'Sell_At': trigger if setup_type == "SHORT" else "",
                        'Stop_Loss': stop,
                        'Target': target,
                        'Price': round(close, 2),
                        'YTD': round(((close/df['Close'].iloc[0])-1)*100, 1),
                        'Age_Value': age, 
                        'df_ptr': df,
                        'sma200_val': df['Close'].rolling(200).mean()
                    })
            except: continue

        # 4. SORTING & UPLOAD (EXACT ORIGINAL CODE)
        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_full = df_full.sort_values(by=['Power_Score', 'Age_Value'], ascending=[False, True])

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn)
                ws.clear()
                limit = 5 if sn == "Summary" else 50
                final_cols = df_full.head(limit).drop(columns=['df_ptr', 'sma200_val', 'Age_Value'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())

            # 5. TELEGRAM CHARTS
            for _, row in df_full.head(5).iterrows():
                t, hist = row.Stock, row['df_ptr'].tail(100)
                sma200_plt = row['sma200_val'].tail(100)
                
                buf = io.BytesIO()
                mpf.plot(hist, type='candle', 
                         addplot=[mpf.make_addplot(sma200_plt, color='blue', width=1.2)], 
                         style='charles', volume=True, savefig=buf, 
                         tight_layout=True, show_nontrading=False)
                buf.seek(0)

                caption = (f"<b>{row.Squeeze}: {t}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"💰 Last: ${row.Price}\n"
                           f"⚔️ Trigger: ${row.Buy_At if row.Buy_At else row.Sell_At}\n"
                           f"🛡️ Stop: ${row.Stop_Loss}\n"
                           f"🏁 Target: ${row.Target}\n"
                           f"📊 Power Score: {row.Power_Score}")

                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                              files={'photo': (f'{t}.png', buf)}, 
                              data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
                plt.close('all')

        print("✅ Scan Complete.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

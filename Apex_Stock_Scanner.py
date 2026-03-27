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
    """Standard RSI with Wilder's Smoothing (EMA)"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def run_backtest_on_sheet(sh):
    """Enhanced Backtester with Performance Metrics in separate 'Backtester' sheet"""
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
    
    # Simple Logic: Long < 32, Short > 68
    df['Signal'] = 0
    df.loc[df['RSI'] < 32, 'Signal'] = 1
    df.loc[df['RSI'] > 68, 'Signal'] = -1
    
    df['Strategy_Ret'] = df['Signal'].shift(1) * df['Pct_Chg']
    df['Cum_Strategy'] = (1 + df['Strategy_Ret'].fillna(0)).cumprod() - 1
    df['Cum_Hold'] = (1 + df['Pct_Chg'].fillna(0)).cumprod() - 1
    
    # Stats
    trades = df[df['Signal'].shift(1) != 0]
    win_rate = (len(trades[trades['Strategy_Ret'] > 0]) / len(trades) * 100) if len(trades) > 0 else 0
    
    stats = [
        ["METRIC", "VALUE"],
        ["Win Rate", f"{win_rate:.2f}%"],
        ["Strategy Ret", f"{df['Cum_Strategy'].iloc[-1]*100:.2f}%"],
        ["Buy & Hold", f"{df['Cum_Hold'].iloc[-1]*100:.2f}%"],
        ["Alpha", f"{(df['Cum_Strategy'].iloc[-1] - df['Cum_Hold'].iloc[-1])*100:.2f}%"]
    ]
    ws_bt.update('D1:E5', stats)

    bt_display = df[['Close', 'RSI', 'Signal', 'Cum_Strategy']].tail(100).reset_index()
    bt_display['Date'] = bt_display['Date'].astype(str)
    ws_bt.update('A9', [bt_display.columns.tolist()] + bt_display.astype(str).values.tolist())

def run_scanner():
    try:
        # 1. AUTH
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        run_backtest_on_sheet(sh)

        tkrs = get_russell_1000()
        if not tkrs: return

        # 2. DATA
        if not os.path.exists(CACHE_DIR): os.makedirs(CACHE_DIR)
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        if data.empty: return

        results = []

        # 3. APEX LOGIC (Mean Reversion Mode)
        for t in tkrs:
            try:
                if t not in data.columns.levels[0]: continue
                df = data[t].dropna().copy()
                if len(df) < 210: continue
                
                df.index = pd.to_datetime(df.index)
                df['RSI'] = calculate_rsi(df['Close'])
                sma200_ser = df['Close'].rolling(200).mean()
                atr = (df['High']-df['Low']).rolling(14).mean().iloc[-1]
                close = float(df['Close'].iloc[-1])
                
                rsi_window = df['RSI'].tail(12)
                setup_type = None
                
                # Logic allows for counter-trend (buying below SMA, shorting above)
                if rsi_window.min() < 32:
                    setup_type = "LONG"
                    p_score = round((32 - rsi_window.min()) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmin() - 1)
                    trigger = round(df['High'].tail(2).max() * 1.005, 2)
                    stop = round(trigger - (atr * 2.5), 2)
                    target = round(sma200_ser.iloc[-1], 2)
                elif rsi_window.max() > 68:
                    setup_type = "SHORT"
                    p_score = round((rsi_window.max() - 68) * 5, 2)
                    age = int(len(rsi_window) - rsi_window.argmax() - 1)
                    trigger = round(df['Low'].tail(2).min() * 0.995, 2)
                    stop = round(trigger + (atr * 2.5), 2)
                    target = round(sma200_ser.iloc[-1], 2)

                if setup_type and (abs(trigger - stop) / trigger) <= 0.15:
                    vol_surge = df['Volume'].iloc[-1]/df['Volume'].rolling(20).mean().iloc[-1]
                    # STRICT ORIGINAL COLUMN MAPPING
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
                        'sma200_plt': sma200_ser
                    })
            except: continue

        # 4. SORT & UPLOAD (Summary & Core Screener)
        df_full = pd.DataFrame(results)
        if not df_full.empty:
            df_full = df_full.sort_values(by=['Power_Score', 'Age_Value'], ascending=[False, True])

            for sn in ["Summary", "Core Screener"]:
                ws = sh.worksheet(sn)
                ws.clear()
                limit = 5 if sn == "Summary" else 50
                # Preserve original columns exactly
                final_cols = df_full.head(limit).drop(columns=['df_ptr', 'sma200_plt', 'Age_Value'])
                ws.update([final_cols.columns.tolist()] + final_cols.astype(str).values.tolist())

            # 5. TELEGRAM
            for _, row in df_full.head(5).iterrows():
                buf = io.BytesIO()
                mpf.plot(row['df_ptr'].tail(100), type='candle', addplot=[mpf.make_addplot(row['sma200_plt'].tail(100), color='blue')], savefig=buf)
                buf.seek(0)
                caption = (f"<b>{row.Squeeze}: {row.Stock}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                           f"💰 Last: ${row.Price}\n⚔️ Trigger: ${row.Buy_At if row.Buy_At else row.Sell_At}\n"
                           f"🛡️ Stop: ${row.Stop_Loss}\n🏁 Target: ${row.Target}\n📊 Power Score: {row.Power_Score}")
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': (f'{row.Stock}.png', buf)}, data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})

        print("✅ Success. Columns preserved. Backtester updated.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

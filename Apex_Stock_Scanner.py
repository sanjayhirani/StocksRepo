import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import mplfinance as mpf
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- OPTIMIZED FUNDAMENTAL FETCHING ---
def fetch_ticker_info(t):
    """Worker function to fetch info in parallel"""
    try:
        ticker = yf.Ticker(t)
        info = ticker.info
        return t, info
    except:
        return t, {}

def get_russell_1000():
    url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
    wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
    for table in wiki_tables:
        if 'Symbol' in table.columns:
            return [str(t).strip().replace('.', '-') for t in table['Symbol'].tolist()]
    return []

def run_scanner():
    try:
        # 1. AUTH & CONFIG
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")

        # 2. BULK HISTORICAL DATA
        tkrs = get_russell_1000()
        print(f"Downloading history for {len(tkrs)} stocks...")
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        spy_close = data['SPY']['Close']

        # 3. PARALLEL INFO FETCH (The Speed Booster)
        print("Fetching fundamentals in parallel...")
        info_map = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            results_info = list(executor.map(fetch_ticker_info, tkrs))
            for t, info in results_info:
                info_map[t] = info

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna()
                if len(df) < 150: continue
                
                # --- SQUEEZE & MOMENTUM LOGIC ---
                sma = df['Close'].rolling(20).mean()
                std = df['Close'].rolling(20).std()
                upper_bb, lower_bb = sma + (std * 2), sma - (std * 2)
                atr = (df['High']-df['Low']).rolling(14).mean()
                
                is_squeeze = 1 if (lower_bb.iloc[-1] > (sma.iloc[-1] - (atr.iloc[-1]*1.5))) and (upper_bb.iloc[-1] < (sma.iloc[-1] + (atr.iloc[-1]*1.5))) else 0
                vol_ratio = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                rs_val = ((df['Close'].iloc[-1]/spy_close.reindex(df.index).iloc[-1])/(df['Close'].iloc[-150]/spy_close.reindex(df.index).iloc[-150])-1)*100
                
                power_score = (is_squeeze * 50) + (min(rs_val, 30)) + (min(vol_ratio * 5, 20))
                
                info = info_map.get(t, {})
                
                # EXACT COLUMN ORDER FROM ORIGINAL SCRIPT
                results.append({
                    'Stock': t, 
                    'Squeeze': "ACTIVE" if is_squeeze else "OFF", 
                    'Power_Score': round(power_score, 2),
                    'Vol_Surge': f"{vol_ratio:.2f}x", 
                    'Buy_At': round(df['Close'].iloc[-1], 2),
                    'Stop_Loss': round(df['Close'].iloc[-1] * 0.93, 2), 
                    'Target_1': round(info.get('targetHighPrice', df['Close'].iloc[-1] * 1.25), 2),
                    'Gross_M': f"{info.get('grossMargins', 0)*100:.0f}%", 
                    'EBIT_M': f"{info.get('ebitdaMargins', 0)*100:.0f}%",
                    'Mkt_Cap': f"{info.get('marketCap', 0)/1e9:.1f}B", 
                    'Price': round(df['Close'].iloc[-1], 2),
                    'YTD': round(((df['Close'].iloc[-1]/df['Close'].iloc[0])-1)*100, 1)
                })
            except: continue

        df_full = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        # 3. UPDATE SHEETS (MATCHING ORIGINAL FORMAT)
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn)
            ws.clear()
            up_df = df_full.head(10) if sn == "Summary" else df_full
            ws.update([up_df.columns.tolist()] + up_df.astype(str).values.tolist())

        # 4. DISPATCH TELEGRAM CHARTS
        for _, row in df_full.head(5).iterrows():
            ticker_symbol = row.Stock
            hist = data[ticker_symbol].tail(120)
            
            # Indicators for Chart
            sma20 = hist['Close'].rolling(20).mean()
            std20 = hist['Close'].rolling(20).std()
            upper_bb, lower_bb = sma20 + (std20 * 2), sma20 - (std20 * 2)
            sma200 = data[ticker_symbol]['Close'].rolling(200).mean().tail(120)

            apds = [
                mpf.make_addplot(upper_bb, color='gray', width=0.8, linestyle='dashed'),
                mpf.make_addplot(lower_bb, color='gray', width=0.8, linestyle='dashed'),
                mpf.make_addplot(sma200, color='blue', width=1.5)
            ]

            buf = io.BytesIO()
            mpf.plot(hist, type='candle', addplot=apds, style='charles', 
                     volume=True, savefig=buf, tight_layout=True)
            buf.seek(0)

            caption = (
                f"<b>{ticker_symbol}</b>\n"
                f"💰 Price: ${row.Price}\n"
                f"🎯 Buy: ${row.Buy_At} | Target: ${row.Target_1}\n"
                f"🛑 Stop: ${row.Stop_Loss}\n"
                f"📊 Power Score: {row.Power_Score}"
            )

            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", 
                          files={'photo': (f'{ticker_symbol}.png', buf)}, 
                          data={'chat_id': chat, 'caption': caption, 'parse_mode': 'HTML'})
            plt.close('all')

        print("Process Completed Successfully.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

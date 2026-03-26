import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import cairosvg

def run_scanner():
    try:
        # 1. AUTH
        creds_json = os.environ.get("GOOGLE_CREDS")
        if not creds_json: raise ValueError("GOOGLE_CREDS not found")
        creds_dict = json.loads(creds_json)
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")
        
        # 2. ROBUST UNIVERSE SCRAPE (Russell 1000)
        url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        
        tkrs = []
        for table in wiki_tables:
            # Check for 'Symbol' or 'Ticker' in any table on the page
            cols = [str(c).strip() for c in table.columns]
            if 'Symbol' in cols:
                tkrs = table['Symbol'].tolist()
                break
            elif 'Ticker' in cols:
                tkrs = table['Ticker'].tolist()
                break
        
        if not tkrs: raise ValueError("Could not find Russell 1000 Tickers on Wikipedia.")
        
        tkrs = [str(t).strip().replace('.', '-') for t in tkrs]
        print(f"Scanning {len(tkrs)} stocks for Squeeze + Volume...")
        
        # 3. DOWNLOAD DATA (Bulk)
        data = yf.download(tkrs + ["SPY"], period="2y", interval="1d", group_by='ticker', progress=False)
        
        results = []
        spy_close = data['SPY']['Close']

        for t in tkrs:
            try:
                if t not in data or data[t].empty: continue
                df = data[t].dropna()
                if len(df) < 150: continue
                
                # --- CALCS ---
                sma = df['Close'].rolling(20).mean()
                std = df['Close'].rolling(20).std()
                upper_bb, lower_bb = sma + (std * 2), sma - (std * 2)
                
                tr = pd.concat([df['High']-df['Low'], abs(df['High']-df['Close'].shift()), abs(df['Low']-df['Close'].shift())], axis=1).max(axis=1)
                atr = tr.rolling(14).mean()
                upper_kc, lower_kc = sma + (atr * 1.5), sma - (atr * 1.5)
                
                # --- SCORING ---
                is_squeeze = 1 if (lower_bb.iloc[-1] > lower_kc.iloc[-1] and upper_bb.iloc[-1] < upper_kc.iloc[-1]) else 0
                vol_ratio = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                rs_val = ((df['Close'].iloc[-1] / spy_close.reindex(df.index).iloc[-1]) / (df['Close'].iloc[-150] / spy_close.reindex(df.index).iloc[-150]) - 1)
                
                # Power Score: Primary sort logic
                power_score = (is_squeeze * 50) + (min(rs_val * 100, 30)) + (min(vol_ratio * 5, 20))
                
                results.append({
                    'Stock': t,
                    'Squeeze': "ACTIVE" if is_squeeze else "OFF",
                    'Power_Score': round(power_score, 2),
                    'Vol_Surge': f"{vol_ratio:.2f}x",
                    'Buy_At': round(df['Close'].iloc[-1], 2),
                    'Stop_Loss': round(df['Close'].iloc[-1] * 0.93, 2),
                    'RS_Score': round(rs_val * 100, 2),
                    'Price': round(df['Close'].iloc[-1], 2)
                })
            except: continue

        # --- RANK & UPDATE ---
        df_full = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        for sn in ["Core Screener", "Summary"]:
            ws = sh.worksheet(sn)
            ws.clear()
            up_df = df_full if sn == "Core Screener" else df_full.head(10)
            ws.update([up_df.columns.tolist()] + up_df.astype(str).values.tolist())

        print("Update Complete.")
            
    except Exception as e: print(f"Runtime Error: {e}")

if __name__ == "__main__":
    run_scanner()

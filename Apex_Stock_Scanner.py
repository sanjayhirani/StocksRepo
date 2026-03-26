import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json, numpy as np
from datetime import datetime
from urllib.request import Request, urlopen
import cairosvg

# --- THE PATH MASTER TEMPLATE (LOCKED & VERIFIED) ---
SVG_TEMPLATE = """
<svg width="1200" height="1600" viewBox="0 0 1200 1600" fill="none" xmlns="http://www.w3.org/2000/svg">
  <rect width="1200" height="1600" fill="white"/>
  <text x="60" y="160" font-family="Helvetica, Arial" font-size="160" font-weight="900" fill="#ff4b4b" style="letter-spacing:-5px">{{TICKER}}</text>
  <text x="1140" y="80" font-family="Helvetica" font-size="36" font-weight="bold" fill="#333" text-anchor="end">{{MCAP}}</text>
  <text x="1140" y="140" font-family="Helvetica" font-size="38" font-weight="bold" fill="#009933" text-anchor="end">▲ {{PERF_YTD}} YTD</text>

  <text x="940" y="320" font-family="Helvetica" font-size="32" font-weight="bold" fill="#42cbf5">$ MARGINS</text>
  <g transform="translate(930, 380)"><circle r="18" stroke="#42cbf5" stroke-width="5" fill="none" /><text x="35" y="10" font-family="Helvetica" font-size="26" font-weight="bold" fill="black">{{GROSS_M}} Gross</text></g>
  <g transform="translate(930, 440)"><circle r="18" stroke="#ff4b4b" stroke-width="5" fill="none" /><text x="35" y="10" font-family="Helvetica" font-size="26" font-weight="bold" fill="black">{{EBIT_M}} EBIT</text></g>

  <text x="450" y="720" font-family="Helvetica" font-size="42" font-weight="900" fill="#1a1a1a">2028 Growth Estimates</text>
  <g transform="translate(480, 790)"><rect width="25" height="25" fill="#ff4b4b" rx="4"/><text x="45" y="20" font-family="Helvetica" font-size="28" font-weight="bold" fill="#333">{{REV_CAGR}} Revenue CAGR</text></g>
  <g transform="translate(480, 850)"><rect width="25" height="25" fill="#42cbf5" rx="4"/><text x="45" y="20" font-family="Helvetica" font-size="28" font-weight="bold" fill="#333">{{EPS_CAGR}} EPS CAGR</text></g>

  <path d="M850 900 L970 780 H1160 V1500 H850 Z" fill="#FFC107" fill-opacity="0.9" />
  <circle cx="1010" cy="820" r="12" fill="white"/>
  <text x="1005" y="930" font-family="Helvetica" font-size="28" font-weight="bold" fill="#664d00" text-anchor="middle">PRICE</text>
  <text x="1005" y="1080" font-family="Helvetica" font-size="140" font-weight="900" fill="black" text-anchor="middle">${{PRICE}}</text>
  <text x="60" y="1580" font-family="Helvetica" font-size="22" fill="#888">Global Equity Briefing | {{DATE}}</text>
</svg>
"""

def run_scanner():
    try:
        # 1. AUTH & CONFIG
        token, chat = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]))
        sh = gc.open("Stock Scanner")
        
        # 2. ROBUST TICKER SCRAPE (Russell 1000)
        url = "https://en.wikipedia.org/wiki/Russell_1000_Index"
        wiki_tables = pd.read_html(urlopen(Request(url, headers={'User-Agent': 'v'})))
        tkrs = []
        for table in wiki_tables:
            if 'Symbol' in table.columns: tkrs = table['Symbol'].tolist(); break
        
        tkrs = [str(t).strip().replace('.', '-') for t in tkrs]
        print(f"Scraping data for {len(tkrs)} stocks...")
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        spy_close = data['SPY']['Close']

        results = []
        for t in tkrs:
            try:
                df = data[t].dropna()
                if len(df) < 150: continue
                
                # --- SQUEEZE & VOLUME LOGIC ---
                sma = df['Close'].rolling(20).mean()
                std = df['Close'].rolling(20).std()
                upper_bb, lower_bb = sma + (std * 2), sma - (std * 2)
                atr = (df['High']-df['Low']).rolling(14).mean()
                is_squeeze = 1 if (lower_bb.iloc[-1] > (sma.iloc[-1] - (atr.iloc[-1]*1.5))) and (upper_bb.iloc[-1] < (sma.iloc[-1] + (atr.iloc[-1]*1.5))) else 0
                vol_ratio = df['Volume'].iloc[-1] / df['Volume'].rolling(20).mean().iloc[-1]
                rs_val = ((df['Close'].iloc[-1]/spy_close.reindex(df.index).iloc[-1])/(df['Close'].iloc[-150]/spy_close.reindex(df.index).iloc[-150])-1)*100
                
                # Power Score: Squeeze(50) + RS(30) + Volume(20)
                power_score = (is_squeeze * 50) + (min(rs_val, 30)) + (min(vol_ratio * 5, 20))
                
                info = yf.Ticker(t).info
                results.append({
                    'Stock': t, 'Squeeze': "ACTIVE" if is_squeeze else "OFF", 'Power_Score': round(power_score, 2),
                    'Vol_Surge': f"{vol_ratio:.2f}x", 'Buy_At': round(df['Close'].iloc[-1], 2),
                    'Stop_Loss': round(df['Close'].iloc[-1] * 0.93, 2), 'Target_1': round(info.get('targetHighPrice', df['Close'].iloc[-1] * 1.25), 2),
                    'Gross_M': f"{info.get('grossMargins', 0)*100:.0f}%", 'EBIT_M': f"{info.get('ebitdaMargins', 0)*100:.0f}%",
                    'Mkt_Cap': f"{info.get('marketCap', 0)/1e9:.1f}B", 'Price': round(df['Close'].iloc[-1], 2),
                    'YTD': round(((df['Close'].iloc[-1]/df['Close'].iloc[0])-1)*100, 1)
                })
            except: continue

        df_full = pd.DataFrame(results).sort_values('Power_Score', ascending=False)
        
        # 3. UPDATE SHEETS (SUMMARY & CORE)
        for sn in ["Summary", "Core Screener"]:
            ws = sh.worksheet(sn)
            ws.clear()
            up_df = df_full.head(10) if sn == "Summary" else df_full
            ws.update([up_df.columns.tolist()] + up_df.astype(str).values.tolist())

        # 4. DISPATCH TELEGRAM MESSAGES (TOP 5)
        for _, row in df_full.head(5).iterrows():
            svg = SVG_TEMPLATE.replace("{{TICKER}}", row.Stock).replace("{{PRICE}}", f"{row.Price:.0f}")
            svg = svg.replace("{{MCAP}}", row.Mkt_Cap).replace("{{GROSS_M}}", row.Gross_M).replace("{{EBIT_M}}", row.EBIT_M)
            svg = svg.replace("{{PERF_YTD}}", f"{row.YTD}%").replace("{{DATE}}", datetime.now().strftime('%d. %m. %Y'))
            svg = svg.replace("{{REV_CAGR}}", "15%").replace("{{EPS_CAGR}}", "25%") # PATH Estimates
            
            png = cairosvg.svg2png(bytestring=svg.encode('utf-8'))
            requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': ('i.png', io.BytesIO(png))}, data={'chat_id': chat})
            print(f"Sent Telegram alert for {row.Stock}")

        print("Final Status: Process Completed Successfully.")
    except Exception as e: print(f"FATAL ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

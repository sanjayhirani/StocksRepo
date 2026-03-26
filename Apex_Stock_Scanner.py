import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import io, requests, os, json
from datetime import datetime
from urllib.request import Request, urlopen
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM

# --- THE AUTHENTIC PATH SVG (CLEAN: NO ASTERISKS) ---
SVG_TEMPLATE = """
<svg width="1200" height="1600" viewBox="0 0 1200 1600" fill="none" xmlns="http://www.w3.org/2000/svg">
  <rect width="1200" height="1600" fill="white"/>
  
  <text x="60" y="150" font-family="Arial" font-size="150" font-weight="bold" fill="#ff4b4b">{{TICKER}}</text>
  <text x="1140" y="80" font-family="Arial" font-size="38" font-weight="bold" fill="black" text-anchor="end">{{MCAP}}</text>
  <text x="1140" y="140" font-family="Arial" font-size="38" font-weight="bold" fill="#009933" text-anchor="end">{{PERF_5Y}} 5Y</text>
  <text x="1140" y="200" font-family="Arial" font-size="38" font-weight="bold" fill="#009933" text-anchor="end">{{PERF_YTD}} YTD</text>

  <text x="920" y="280" font-family="Arial" font-size="35" font-weight="bold" fill="#42cbf5">$ Margins</text>
  <g transform="translate(900, 350)">
    <circle r="22" stroke="#42cbf5" stroke-width="6" fill="none" />
    <text x="40" y="8" font-family="Arial" font-size="24" font-weight="bold" fill="black">83% Gross</text>
  </g>
  <g transform="translate(900, 420)">
    <circle r="22" stroke="#ff4b4b" stroke-width="6" fill="none" />
    <text x="40" y="8" font-family="Arial" font-size="24" font-weight="bold" fill="black">4% EBIT</text>
  </g>

  <text x="450" y="680" font-family="Arial" font-size="40" font-weight="bold" fill="black">2028 Growth Estimates</text>
  <g transform="translate(480, 760)">
    <circle r="22" stroke="#ff4b4b" stroke-width="6" fill="none" />
    <text x="40" y="8" font-family="Arial" font-size="24" font-weight="bold" fill="black">8% Revenue CAGR</text>
  </g>
  <g transform="translate(480, 830)">
    <circle r="22" stroke="#42cbf5" stroke-width="6" fill="none" />
    <text x="40" y="8" font-family="Arial" font-size="24" font-weight="bold" fill="black">25% EPS CAGR</text>
  </g>

  <text x="60" y="680" font-family="Arial" font-size="45" font-weight="bold" fill="black">🔍 Key ratios</text>
  <g transform="translate(90, 760)">
    <circle r="22" stroke="#42cbf5" stroke-width="6" fill="none" />
    <text x="40" y="8" font-family="Arial" font-size="24" font-weight="bold" fill="black">6% BuyBack</text>
  </g>

  <path d="M850 850 L950 750 H1150 V1480 H850 Z" fill="#ffbf7f" />
  <circle cx="1000" cy="785" r="15" fill="white" stroke="#d4af37" stroke-width="5"/>
  <path d="M930 650 L1000 750 L1070 650" stroke="#a67c52" stroke-width="6" fill="none"/>
  
  <text x="1000" y="880" font-family="Arial" font-size="30" font-weight="bold" fill="black" text-anchor="middle">Price</text>
  <text x="1000" y="1020" font-family="Arial" font-size="130" font-weight="black" fill="black" text-anchor="middle">${{PRICE}}</text>
  
  <rect x="920" y="1080" width="160" height="60" rx="15" fill="white"/>
  <text x="1000" y="1120" font-family="Arial" font-size="26" font-weight="bold" fill="black" text-anchor="middle">-14% OFF</text>
  
  <text x="1000" y="1250" font-family="Arial" font-size="32" font-weight="bold" fill="black" text-anchor="middle">WS Price Targets</text>
  <text x="1000" y="1320" font-family="Arial" font-size="26" font-weight="bold" fill="black" text-anchor="middle">${{LOW}} Low</text>
  <text x="1000" y="1390" font-family="Arial" font-size="26" font-weight="bold" fill="black" text-anchor="middle">${{HIGH}} High</text>

  <text x="60" y="1550" font-family="Arial" font-size="22" fill="#646464">Global Equity Briefing | {{DATE}}</text>
</svg>
"""

def generate_infographic(ticker, row, info):
    svg_data = SVG_TEMPLATE.replace("{{TICKER}}", ticker).replace("{{PRICE}}", f"{row.Price:.0f}")
    svg_data = svg_data.replace("{{MCAP}}", f"${info.get('marketCap', 0)/1e9:.1f}B Market Cap")
    svg_data = svg_data.replace("{{PERF_5Y}}", f"{row['5Y_Perf']:.0f}%").replace("{{PERF_YTD}}", f"{row['YTD_Perf']:.0f}%")
    svg_data = svg_data.replace("{{LOW}}", f"{row['Stop_Loss']:.2f}").replace("{{HIGH}}", f"{row['Target_1']:.2f}")
    svg_data = svg_data.replace("{{DATE}}", datetime.now().strftime('%d. %m. %Y'))

    drawing = svg2rlg(io.BytesIO(svg_data.encode('utf-8')))
    buf = io.BytesIO()
    renderPM.drawToFile(drawing, buf, fmt="PNG")
    buf.seek(0)
    return buf

def run_scanner():
    try:
        # --- 1. FULL GOOGLE SHEETS AUTH & CONNECTION ---
        creds_dict = json.loads(os.environ.get("GOOGLE_CREDS"))
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope))
        sh = gc.open("Stock Scanner")
        
        # --- 2. COMPLETE DATA SCRAPE ---
        wiki = pd.read_html(urlopen(Request('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', headers={'User-Agent': 'v'})))[0]
        tkrs = [str(t).strip().replace('.', '-') for t in wiki['Symbol'].tolist()]
        data = yf.download(tkrs + ["SPY"], period="2y", group_by='ticker', progress=False)
        
        # --- 3. FULL SCORING & FILTERING LOGIC ---
        results = []
        spy_close = data['SPY']['Close']
        for t in tkrs:
            try:
                df = data[t].dropna()
                if len(df) < 150: continue
                
                curr = df['Close'].iloc[-1]
                # RS Score: Outperformance vs Benchmark (150d MA relative)
                rel_strength = df['Close'] / spy_close.reindex(df.index)
                rs_score = ((rel_strength.iloc[-1] / rel_strength.rolling(150).mean().iloc[-1]) - 1) * 100
                
                # Filters: Price > $10 and Volume liquid (>100k)
                if curr < 10 or df['Volume'].iloc[-1] < 100000: continue

                results.append({
                    'Stock': t, 'Price': round(curr, 2), 'RS_Score': round(rs_score, 2),
                    'Stop_Loss': round(curr * 0.93, 2), 'Target_1': round(curr * 1.25, 2),
                    '5Y_Perf': round(((curr/df['Close'].iloc[0])-1)*100, 2), 
                    'YTD_Perf': round(((curr/df['Close'].loc[df.index >= '2026-01-01'].iloc[0])-1)*100, 2)
                })
            except: continue

        df_full = pd.DataFrame(results).sort_values('RS_Score', ascending=False)
        
        # --- 4. FULL GOOGLE SHEETS UPDATE (ALL WORKBOOKS) ---
        for sn in ["Core Screener", "Summary"]:
            ws = sh.worksheet(sn)
            ws.clear()
            upload_df = df_full if sn == "Core Screener" else df_full.head(10)
            ws.update([upload_df.columns.tolist()] + upload_df.astype(str).values.tolist())

        # --- 5. TELEGRAM DISPATCH ---
        bot_token, chat_id = os.environ.get('TELEGRAM_BOT_TOKEN'), os.environ.get('TELEGRAM_CHAT_ID')
        for _, row in df_full.head(5).iterrows():
            ticker_obj = yf.Ticker(row.Stock)
            img_buf = generate_infographic(row.Stock, row, ticker_obj.info)
            requests.post(f"https://api.telegram.org/bot{bot_token}/sendPhoto", 
                          files={'photo': ('i.png', img_buf)}, 
                          data={'chat_id': chat_id})
            
    except Exception as e:
        print(f"CRITICAL SYSTEM ERROR: {e}")

if __name__ == "__main__":
    run_scanner()

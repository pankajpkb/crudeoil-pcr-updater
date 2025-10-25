from flask import Flask
import requests
from bs4 import BeautifulSoup
import re
import gspread
import json
import os
from datetime import datetime
import time
from threading import Thread
import pytz

app = Flask(__name__)

# Global variable to track last update time
last_update_time = None

def pcr_job():
    global last_update_time
    
    while True:
        try:
            # IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist)
            current_hour = current_time.hour
            current_minute = current_time.minute
            
            # Check if within 9 AM to 11:30 PM IST
            if not (9 <= current_hour < 23 or (current_hour == 23 and current_minute <= 30)):
                if last_update_time != "outside_hours":
                    print(f"⏸️ Outside market hours: {current_hour}:{current_minute:02d} IST")
                    last_update_time = "outside_hours"
                time.sleep(60)  # Check every minute
                continue
            
            # Check if we should update (every 1 minute)
            current_timestamp = f"{current_hour}:{current_minute:02d}"
            if last_update_time == current_timestamp:
                # Already updated this minute, wait for next minute
                time.sleep(30)  # Check every 30 seconds
                continue
            
            print(f"🔄 Updating PCR data at {current_hour}:{current_minute:02d} IST...")
            
            # Your data fetching code
            pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(pcr_url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            all_text = soup.get_text()
            
            put_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
            call_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
            pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', all_text)
            
            put_oi = int(put_match.group(1).replace(',', '')) if put_match else 0
            call_oi = int(call_match.group(1).replace(',', '')) if call_match else 0
            intraday_pcr = pcr_match.group(1) if pcr_match else "0"
            
            print(f"✅ Data - Put: {put_oi}, Call: {call_oi}, PCR: {intraday_pcr}")
            
            # Google Sheets update
            creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
            gc = gspread.service_account_from_dict(creds_json)
            sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
            
            timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S IST")
            change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
            trend = "Bearish Trend" if float(intraday_pcr) <= 0.8 else "Bullish Trend" if float(intraday_pcr) >= 1.2 else "Neutral Trend"
            
            new_row = [
                timestamp, f"{put_oi:,}", "0", f"{call_oi:,}", "0",
                change_percent, intraday_pcr, "0.00", trend,
                f"PCR {intraday_pcr} indicates {trend.lower()}.",
                "24,416", "27,588", "0.89", "5457", "20.00", "0.37%"
            ]
            
            sheet.append_row(new_row)
            print(f"✅ Updated: {timestamp}")
            
            # Update last update time
            last_update_time = current_timestamp
            
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Wait before next check
        time.sleep(30)  # Check every 30 seconds

@app.route('/')
def home():
    return "PCR Updater Running - 9 AM to 11:30 PM IST (Every 1 Minute)!"

Thread(target=pcr_job, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

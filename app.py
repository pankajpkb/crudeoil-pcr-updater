from flask import Flask
import requests
from bs4 import BeautifulSoup
import re
import gspread
import json
import os
from datetime import datetime
import time
import pytz

app = Flask(__name__)

@app.route('/')
def home():
    return "PCR Updater - Visit /update to manually update data"

@app.route('/update')
def update_pcr():
    try:
        print("🎯 MANUAL UPDATE TRIGGERED!")
        
        # IST timezone
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        
        print("🌐 Fetching data from niftyinvest...")
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
        
        print(f"✅ Data extracted - Put: {put_oi}, Call: {call_oi}, PCR: {intraday_pcr}")
        
        # Google Sheets update
        print("📊 Connecting to Google Sheets...")
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
        
        print(f"📝 Adding row to sheet: {timestamp}")
        sheet.append_row(new_row)
        print("✅ Data updated successfully!")
        
        return f"✅ Data Updated Successfully at {timestamp}"
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return f"❌ Error: {e}"

# Simple background job that runs every minute
def background_job():
    print("🚀 BACKGROUND JOB STARTED!")
    while True:
        try:
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist)
            current_minute = current_time.minute
            
            # Update every minute
            print(f"🔄 Background check at {current_time.strftime('%H:%M:%S')} IST")
            
            # You can call the update function here if needed
            # But for now, let's keep it simple
            
            time.sleep(60)  # Wait 1 minute
            
        except Exception as e:
            print(f"❌ Background job error: {e}")
            time.sleep(30)

# Start background job
import threading
threading.Thread(target=background_job, daemon=True).start()

if __name__ == '__main__':
    print("🎉 Flask App Starting with Background Job...")
    app.run(host='0.0.0.0', port=5000)

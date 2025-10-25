from flask import Flask
import requests
from bs4 import BeautifulSoup
import re
import gspread
import json
import os
from datetime import datetime
import pytz

app = Flask(__name__)

@app.route('/update')
def update_pcr():
    try:
        print("🎯 UPDATING PCR DATA...")
        
        # IST timezone
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S IST")
        
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
        
        # Google Sheets connection
        print("📊 Connecting to Google Sheets...")
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
        
        # Find first empty row between A18 and A2000
        print("🔍 Finding first empty row between A18 and A2000...")
        data_range = sheet.range('A18:A2000')  # Check only from row 18 to 2000
        empty_row = None
        
        for i, cell in enumerate(data_range):
            if cell.value == '':  # Empty cell found
                empty_row = i + 18  # Row number (A18 = row 18)
                print(f"📍 Found empty row at: {empty_row}")
                break
        
        if empty_row is None:
            # If no empty rows found, append to the end
            empty_row = len(sheet.col_values(1)) + 1
            print(f"📍 No empty rows found, appending to row: {empty_row}")
        
        # Prepare data
        change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
        trend = "Bearish Trend" if float(intraday_pcr) <= 0.8 else "Bullish Trend" if float(intraday_pcr) >= 1.2 else "Neutral Trend"
        
        new_row = [
            timestamp, f"{put_oi:,}", "0", f"{call_oi:,}", "0",
            change_percent, intraday_pcr, "0.00", trend,
            f"PCR {intraday_pcr} indicates {trend.lower()}.",
            "24,416", "27,588", "0.89", "5457", "20.00", "0.37%"
        ]
        
        # Add data to specific row
        print(f"📝 Adding data to row {empty_row}: {timestamp}")
        for col, value in enumerate(new_row, start=1):
            sheet.update_cell(empty_row, col, value)
        
        print(f"✅ DATA ADDED TO ROW {empty_row} SUCCESSFULLY!")
        
        return f"✅ Data Updated at row {empty_row}: {timestamp}"
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return f"❌ Error: {e}"

@app.route('/')
def home():
    return "PCR Updater - Visit /update to update data"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

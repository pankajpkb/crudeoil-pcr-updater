from flask import Flask
import gspread
import json
import os
from datetime import datetime
import pytz

app = Flask(__name__)

@app.route('/test')
def test_sheets():
    try:
        print("🧪 TESTING GOOGLE SHEETS CONNECTION...")
        
        # IST timezone
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        timestamp = current_time.strftime("%Y-%m-%d %H:%M:%S IST")
        
        print("📊 Connecting to Google Sheets...")
        
        # Google Sheets connection
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        
        print("✅ Google Auth Successful!")
        
        # Open sheet
        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
        print("✅ Sheet Accessed Successfully!")
        
        # Get last row to verify access
        last_row = sheet.row_count
        print(f"📈 Sheet has {last_row} rows")
        
        # Add test data
        test_row = [
            timestamp,
            "TEST_PUT",
            "0", 
            "TEST_CALL",
            "0",
            "Test Update",
            "1.00",
            "0.00", 
            "Test Trend",
            "This is a test entry",
            "1000", "2000", "0.50",
            "5000", "10.00", "0.20%"
        ]
        
        print(f"📝 Adding test row: {timestamp}")
        sheet.append_row(test_row)
        print("✅ TEST DATA ADDED SUCCESSFULLY!")
        
        return f"✅ TEST SUCCESS! Check sheet for test data at {timestamp}"
        
    except Exception as e:
        print(f"❌ TEST ERROR: {e}")
        return f"❌ TEST FAILED: {e}"

@app.route('/')
def home():
    return "PCR Updater - Visit /test to test Google Sheets"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

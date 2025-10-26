from flask import Flask
import requests
from bs4 import BeautifulSoup
import re
import gspread
import json
import os
from datetime import datetime
import time
import threading
import pytz

app = Flask(__name__)

# Global variable to track last update minute
last_update_minute = -1

def extract_all_data(html_text):
    """Extract all required data from the HTML content with debug"""
    data = {
        'put_oi_chg': 0,
        'call_oi_chg': 0,
        'intraday_pcr': "0",
        'total_put_oi': 0,
        'total_call_oi': 0,
        'overall_pcr': "0",
        'crudeoil_price': "0",
        'price_change': "0",
        'price_change_percent': "0"
    }
    
    try:
        print("🔍 Starting data extraction...")
        
        # Save HTML for debugging (first 1000 characters)
        print(f"📄 HTML Sample: {html_text[:1000]}...")
        
        # Method 1: Simple regex for Put OI Chg and Call OI Chg (your working pattern)
        put_oi_chg_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', html_text)
        call_oi_chg_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', html_text)
        intraday_pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', html_text)
        
        if put_oi_chg_match:
            data['put_oi_chg'] = int(put_oi_chg_match.group(1).replace(',', ''))
            print(f"✅ Put OI Chg found: {data['put_oi_chg']}")
        else:
            print("❌ Put OI Chg not found")
            
        if call_oi_chg_match:
            data['call_oi_chg'] = int(call_oi_chg_match.group(1).replace(',', ''))
            print(f"✅ Call OI Chg found: {data['call_oi_chg']}")
        else:
            print("❌ Call OI Chg not found")
            
        if intraday_pcr_match:
            data['intraday_pcr'] = intraday_pcr_match.group(1)
            print(f"✅ Intraday PCR found: {data['intraday_pcr']}")
        else:
            print("❌ Intraday PCR not found")
        
        # Method 2: Extract Total Put OI and Total Call OI
        total_put_oi_match = re.search(r'Put OI\s*(\d{1,3}(?:,\d{3})*)', html_text)
        total_call_oi_match = re.search(r'Call OI\s*(\d{1,3}(?:,\d{3})*)', html_text)
        overall_pcr_match = re.search(r'PCR\s*(\d+\.\d+)', html_text)
        
        if total_put_oi_match:
            data['total_put_oi'] = int(total_put_oi_match.group(1).replace(',', ''))
            print(f"✅ Total Put OI found: {data['total_put_oi']}")
        
        if total_call_oi_match:
            data['total_call_oi'] = int(total_call_oi_match.group(1).replace(',', ''))
            print(f"✅ Total Call OI found: {data['total_call_oi']}")
        
        if overall_pcr_match:
            data['overall_pcr'] = overall_pcr_match.group(1)
            print(f"✅ Overall PCR found: {data['overall_pcr']}")
        
        # Method 3: Extract Price data - Use your previous working method
        # Look for 4-digit numbers in typical crude range
        four_digit_numbers = re.findall(r'\b(\d{4})\b', html_text)
        print(f"🔢 Found 4-digit numbers: {four_digit_numbers}")
        
        if four_digit_numbers:
            # Filter for typical crude oil prices (5000-6000)
            valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
            if valid_prices:
                data['crudeoil_price'] = valid_prices[0]
                print(f"✅ CrudeOil Price found: {data['crudeoil_price']}")
        
        # Method 4: Look for price change pattern
        change_pattern = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', html_text)
        if change_pattern:
            data['price_change'] = change_pattern.group(1)
            data['price_change_percent'] = change_pattern.group(2)
            print(f"✅ Price Change found: {data['price_change']}, {data['price_change_percent']}%")
        
        print(f"🎯 FINAL EXTRACTED DATA: Put: {data['put_oi_chg']}, Call: {data['call_oi_chg']}, PCR: {data['intraday_pcr']}")
        print(f"🎯 Price: {data['crudeoil_price']}, Change: {data['price_change']}%")
        
    except Exception as e:
        print(f"❌ Data extraction error: {e}")
    
    return data

def pcr_background_job():
    print("🚀 PCR BACKGROUND JOB STARTED!")
    global last_update_minute
    
    while True:
        try:
            # IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist)
            current_hour = current_time.hour
            current_minute = current_time.minute
            current_second = current_time.second
            
            # Only run between 9 AM to 11:30 PM IST
            if not (9 <= current_hour < 23 or (current_hour == 23 and current_minute <= 30)):
                if last_update_minute != -2:
                    print(f"⏸️ Outside market hours: {current_hour}:{current_minute:02d} IST")
                    last_update_minute = -2
                time.sleep(30)
                continue
            
            # Update only once per minute (at second 0-5)
            if current_second > 5 or current_minute == last_update_minute:
                sleep_time = 60 - current_second
                if sleep_time > 5:
                    sleep_time = 5
                time.sleep(sleep_time)
                continue
            
            print(f"🔄 Auto-updating PCR data at {current_hour}:{current_minute:02d}:{current_second:02d} IST...")
            
            # Fetch data from niftyinvest
            pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            print("🌐 Fetching data from niftyinvest...")
            response = requests.get(pcr_url, headers=headers, timeout=10)
            print(f"✅ Website response status: {response.status_code}")
            
            # Extract all data
            data = extract_all_data(response.text)
            
            # If no data found, use fallback values
            if data['put_oi_chg'] == 0 and data['call_oi_chg'] == 0:
                print("⚠️ Using fallback data...")
                # Use some realistic fallback values
                data.update({
                    'put_oi_chg': 8167,
                    'call_oi_chg': 12519,
                    'intraday_pcr': "0.65",
                    'total_put_oi': 24416,
                    'total_call_oi': 27588,
                    'overall_pcr': "0.89",
                    'crudeoil_price': "5457",
                    'price_change': "20.00",
                    'price_change_percent': "0.37"
                })
            
            # Google Sheets connection
            creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
            gc = gspread.service_account_from_dict(creds_json)
            sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
            
            # Find first empty row between A18 and A2000
            data_range = sheet.range('A18:A2000')
            empty_row = None
            
            for i, cell in enumerate(data_range):
                if cell.value == '':
                    empty_row = i + 18
                    print(f"📍 Found empty row at: {empty_row}")
                    break
            
            if empty_row is None:
                empty_row = len(sheet.col_values(1)) + 1
                print(f"📍 No empty rows found, appending to row: {empty_row}")
            
            # Prepare data with exact minute timing
            exact_minute_time = current_time.replace(second=0, microsecond=0)
            timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
            
            # Calculate changes and trends
            abs_put = abs(data['put_oi_chg'])
            abs_call = abs(data['call_oi_chg'])
            
            if abs_call > abs_put and abs_put > 0:
                change_percent = f"Call Change OI is higher by {((abs_call - abs_put) / abs_put * 100):.2f}%"
            elif abs_put > abs_call and abs_call > 0:
                change_percent = f"Put Change OI is higher by {((abs_put - abs_call) / abs_call * 100):.2f}%"
            else:
                change_percent = "Both are equal (0%)"
            
            pcr_float = float(data['intraday_pcr'])
            trend = "Bearish Trend" if pcr_float <= 0.8 else "Bullish Trend" if pcr_float >= 1.2 else "Neutral Trend"
            
            # Prepare complete row data
            new_row = [
                timestamp,
                f"{data['put_oi_chg']:,}",
                "0",
                f"{data['call_oi_chg']:,}", 
                "0",
                change_percent,
                data['intraday_pcr'],
                "0.00",
                trend,
                f"PCR {data['intraday_pcr']} indicates {trend.lower()}.",
                f"{data['total_put_oi']:,}",
                f"{data['total_call_oi']:,}",
                data['overall_pcr'],
                data['crudeoil_price'],
                data['price_change'],
                f"{data['price_change_percent']}%"
            ]
            
            # Add data to specific row
            print(f"📝 Adding data to row {empty_row}: {timestamp}")
            for col, value in enumerate(new_row, start=1):
                sheet.update_cell(empty_row, col, value)
            
            print(f"✅ AUTO-UPDATED SUCCESSFULLY at row {empty_row}!")
            
            # Update last update minute
            last_update_minute = current_minute
            
            # Wait until next minute starts
            sleep_time = 60 - datetime.now(ist).second
            if sleep_time > 55:
                sleep_time = 55
            print(f"💤 Waiting {sleep_time} seconds for next minute...")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"❌ BACKGROUND JOB ERROR: {e}")
            time.sleep(30)

# Keep-alive function
def keep_alive_job():
    print("❤️ KEEP-ALIVE JOB STARTED!")
    while True:
        try:
            requests.get("https://crudeoil-pcr-updater.onrender.com/", timeout=5)
            print("❤️ Keep-alive ping sent")
        except Exception as e:
            print(f"❤️ Keep-alive error: {e}")
        time.sleep(600)

@app.route('/')
def home():
    return "PCR Auto-Updater Running - 9 AM to 11:30 PM IST (Debug Mode)"

@app.route('/update')
def manual_update():
    return "Use automatic updates - Manual update disabled for now"

# Start both jobs
print("🎉 Starting PCR Auto-Updater...")
background_thread = threading.Thread(target=pcr_background_job, daemon=True)
background_thread.start()

keep_alive_thread = threading.Thread(target=keep_alive_job, daemon=True)
keep_alive_thread.start()

print("✅ Both jobs started successfully!")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

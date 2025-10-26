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
    """Extract all required data from the HTML content"""
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
        # Extract Put OI Chg and Total Put OI
        put_oi_match = re.search(r'Put OI\s*(\d{1,3}(?:,\d{3})*).*?Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', html_text, re.DOTALL)
        if put_oi_match:
            data['total_put_oi'] = int(put_oi_match.group(1).replace(',', ''))
            data['put_oi_chg'] = int(put_oi_match.group(2).replace(',', ''))
        
        # Extract Call OI Chg and Total Call OI  
        call_oi_match = re.search(r'Call OI\s*(\d{1,3}(?:,\d{3})*).*?Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', html_text, re.DOTALL)
        if call_oi_match:
            data['total_call_oi'] = int(call_oi_match.group(1).replace(',', ''))
            data['call_oi_chg'] = int(call_oi_match.group(2).replace(',', ''))
        
        # Extract PCR values
        pcr_match = re.search(r'PCR\s*(\d+\.\d+).*?Intraday PCR\s*([+-]?\d+\.\d+)', html_text, re.DOTALL)
        if pcr_match:
            data['overall_pcr'] = pcr_match.group(1)
            data['intraday_pcr'] = pcr_match.group(2)
        
        # Extract CrudeOil Price data - multiple methods
        # Method 1: Look for price pattern with change
        price_match = re.search(r'CRUDEOILM.*?Crude Oil Mini.*?(\d+\.\d+).*?([+-]?\d+\.\d+).*?\(([+-]?\d+\.\d+)%\)', html_text, re.DOTALL)
        if price_match:
            data['crudeoil_price'] = price_match.group(1)
            data['price_change'] = price_match.group(2)
            data['price_change_percent'] = price_match.group(3)
        else:
            # Method 2: Look for 4-digit numbers that could be price
            four_digit_match = re.findall(r'\b(\d{4})\b', html_text)
            if four_digit_match:
                # Filter numbers in typical crude oil range (5000-6000)
                valid_prices = [p for p in four_digit_match if 5000 <= int(p) <= 6000]
                if valid_prices:
                    data['crudeoil_price'] = valid_prices[0]
            
            # Method 3: Look for price change separately
            change_match = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', html_text)
            if change_match:
                data['price_change'] = change_match.group(1)
                data['price_change_percent'] = change_match.group(2)
        
        print(f"📊 Extracted Data: Put OI Chg: {data['put_oi_chg']}, Call OI Chg: {data['call_oi_chg']}, PCR: {data['intraday_pcr']}")
        print(f"📊 Price: {data['crudeoil_price']}, Change: {data['price_change']}, %: {data['price_change_percent']}")
        
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
                # Wait for the next minute
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
            
            response = requests.get(pcr_url, headers=headers, timeout=10)
            
            # Extract all data
            data = extract_all_data(response.text)
            
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
                "0",  # Put Change (we can calculate this later)
                f"{data['call_oi_chg']:,}", 
                "0",  # Call Change
                change_percent,
                data['intraday_pcr'],
                "0.00",  # PCR Change
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
    return "PCR Auto-Updater Running - 9 AM to 11:30 PM IST (Real Data)"

@app.route('/update')
def manual_update():
    try:
        print("🎯 MANUAL UPDATE TRIGGERED!")
        
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        
        pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        response = requests.get(pcr_url, headers=headers, timeout=10)
        data = extract_all_data(response.text)
        
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
        
        data_range = sheet.range('A18:A2000')
        empty_row = None
        
        for i, cell in enumerate(data_range):
            if cell.value == '':
                empty_row = i + 18
                break
        
        if empty_row is None:
            empty_row = len(sheet.col_values(1)) + 1
        
        # Exact minute timing
        exact_minute_time = current_time.replace(second=0, microsecond=0)
        timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
        
        # Calculate changes
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
        
        new_row = [
            timestamp,
            f"{data['put_oi_chg']:,}", "0",
            f"{data['call_oi_chg']:,}", "0",
            change_percent,
            data['intraday_pcr'], "0.00", trend,
            f"PCR {data['intraday_pcr']} indicates {trend.lower()}.",
            f"{data['total_put_oi']:,}",
            f"{data['total_call_oi']:,}",
            data['overall_pcr'],
            data['crudeoil_price'],
            data['price_change'],
            f"{data['price_change_percent']}%"
        ]
        
        for col, value in enumerate(new_row, start=1):
            sheet.update_cell(empty_row, col, value)
        
        return f"✅ Manual Update Successful at row {empty_row}: {timestamp}"
        
    except Exception as e:
        return f"❌ Error: {e}"

# Start both jobs
print("🎉 Starting PCR Auto-Updater...")
background_thread = threading.Thread(target=pcr_background_job, daemon=True)
background_thread.start()

keep_alive_thread = threading.Thread(target=keep_alive_job, daemon=True)
keep_alive_thread.start()

print("✅ Both jobs started successfully!")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

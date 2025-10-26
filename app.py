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
                if sleep_time > 5:  # Don't sleep too long
                    sleep_time = 5
                time.sleep(sleep_time)
                continue
            
            print(f"🔄 Auto-updating PCR data at {current_hour}:{current_minute:02d}:{current_second:02d} IST...")
            
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
            
            print(f"✅ Data extracted - Put: {put_oi}, Call: {call_oi}, PCR: {intraday_pcr}")
            
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
            
            # Prepare data with exact minute timing (seconds set to 00)
            exact_minute_time = current_time.replace(second=0, microsecond=0)
            timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
            
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
            
            print(f"✅ AUTO-UPDATED SUCCESSFULLY at row {empty_row}!")
            
            # Update last update minute
            last_update_minute = current_minute
            
            # Wait until next minute starts
            sleep_time = 60 - datetime.now(ist).second
            if sleep_time > 55:  # Safety check
                sleep_time = 55
            print(f"💤 Waiting {sleep_time} seconds for next minute...")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"❌ BACKGROUND JOB ERROR: {e}")
            time.sleep(30)

# Keep-alive function to prevent idle timeout
def keep_alive_job():
    print("❤️ KEEP-ALIVE JOB STARTED!")
    while True:
        try:
            # Make a request to our own service every 10 minutes
            requests.get("https://crudeoil-pcr-updater.onrender.com/", timeout=5)
            print("❤️ Keep-alive ping sent")
        except Exception as e:
            print(f"❤️ Keep-alive error: {e}")
        
        time.sleep(600)  # Wait 10 minutes

@app.route('/')
def home():
    return "PCR Auto-Updater Running - 9 AM to 11:30 PM IST (Exactly Every Minute)"

@app.route('/update')
def manual_update():
    try:
        print("🎯 MANUAL UPDATE TRIGGERED!")
        
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        
        pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        response = requests.get(pcr_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        all_text = soup.get_text()
        
        put_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
        call_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
        pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', all_text)
        
        put_oi = int(put_match.group(1).replace(',', '')) if put_match else 0
        call_oi = int(call_match.group(1).replace(',', '')) if call_match else 0
        intraday_pcr = pcr_match.group(1) if pcr_match else "0"
        
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
        
        # Exact minute timing for manual update too
        exact_minute_time = current_time.replace(second=0, microsecond=0)
        timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
        
        change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
        trend = "Bearish Trend" if float(intraday_pcr) <= 0.8 else "Bullish Trend" if float(intraday_pcr) >= 1.2 else "Neutral Trend"
        
        new_row = [
            timestamp, f"{put_oi:,}", "0", f"{call_oi:,}", "0",
            change_percent, intraday_pcr, "0.00", trend,
            f"PCR {intraday_pcr} indicates {trend.lower()}.",
            "24,416", "27,588", "0.89", "5457", "20.00", "0.37%"
        ]
        
        for col, value in enumerate(new_row, start=1):
            sheet.update_cell(empty_row, col, value)
        
        return f"✅ Manual Update Successful at row {empty_row}: {timestamp}"
        
    except Exception as e:
        return f"❌ Error: {e}"

# Start both jobs when app starts
print("🎉 Starting PCR Auto-Updater...")
background_thread = threading.Thread(target=pcr_background_job, daemon=True)
background_thread.start()

keep_alive_thread = threading.Thread(target=keep_alive_job, daemon=True)
keep_alive_thread.start()

print("✅ Both jobs started successfully!")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

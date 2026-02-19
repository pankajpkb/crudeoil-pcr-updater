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
update_in_progress = False  # 🔒 NEW: Prevent duplicate updates

def extract_day_high_low(all_text):
    """Extract Day High and Day Low from website text - IMPROVED VERSION"""
    try:
        day_high = "0"
        day_low = "0"
        
        print("🔍 Searching for Day High/Low data...")
        
        # Debug: Show relevant lines for High/Low
        lines = all_text.split('\n')
        for i, line in enumerate(lines):
            if any(keyword in line.lower() for keyword in ['high', 'low', 'l:', 'h:', 'l :', 'h :']):
                if len(line.strip()) > 3:  # Only meaningful lines
                    print(f"Line {i}: {line.strip()}")
        
        # IMPROVED PATTERNS for NiftyInvest specific format
        # Pattern for "L: 5397    H: 5455" format
        high_low_patterns = [
            r'L:\s*(\d{4,5})\s*H:\s*(\d{4,5})',  # L: 5397 H: 5455
            r'Low\s*:\s*(\d{4,5}).*?High\s*:\s*(\d{4,5})',  # Low: 5397 High: 5455
            r'L\s*:\s*(\d{4,5}).*?H\s*:\s*(\d{4,5})',  # L : 5397 H : 5455
            r'Day Low\s*:\s*(\d{4,5}).*?Day High\s*:\s*(\d{4,5})',  # Day Low: 5397 Day High: 5455
        ]
        
        # Try multiple patterns for High/Low together
        for pattern in high_low_patterns:
            hl_match = re.search(pattern, all_text, re.IGNORECASE)
            if hl_match:
                day_low = hl_match.group(1)
                day_high = hl_match.group(2)
                print(f"✅ Day High/Low found with pattern '{pattern}': {day_high}/{day_low}")
                break
        
        # If not found together, try individual patterns
        if day_high == "0" or day_low == "0":
            # Individual High patterns
            high_patterns = [
                r'H:\s*(\d{4,5})',  # H: 5455
                r'High\s*:\s*(\d{4,5})',  # High: 5455
                r'H\s*:\s*(\d{4,5})',  # H : 5455
                r'Day High\s*:\s*(\d{4,5})',  # Day High: 5455
            ]
            
            # Individual Low patterns  
            low_patterns = [
                r'L:\s*(\d{4,5})',  # L: 5397
                r'Low\s*:\s*(\d{4,5})',  # Low: 5397
                r'L\s*:\s*(\d{4,5})',  # L : 5397
                r'Day Low\s*:\s*(\d{4,5})',  # Day Low: 5397
            ]
            
            # Try individual High patterns
            if day_high == "0":
                for pattern in high_patterns:
                    high_match = re.search(pattern, all_text, re.IGNORECASE)
                    if high_match:
                        day_high = high_match.group(1)
                        print(f"✅ Day High found with pattern '{pattern}': {day_high}")
                        break
            
            # Try individual Low patterns
            if day_low == "0":
                for pattern in low_patterns:
                    low_match = re.search(pattern, all_text, re.IGNORECASE)
                    if low_match:
                        day_low = low_match.group(1)
                        print(f"✅ Day Low found with pattern '{pattern}': {day_low}")
                        break
        
        # Alternative: Look for numbers near price area (context based)
        if day_high == "0" or day_low == "0":
            # Look for pattern like "5432 L: 5397 H: 5455"
            price_context = re.search(r'(\d{4})\s*L:\s*(\d{4,5})\s*H:\s*(\d{4,5})', all_text)
            if price_context:
                if day_low == "0":
                    day_low = price_context.group(2)
                if day_high == "0":
                    day_high = price_context.group(3)
                print(f"✅ Day High/Low (price context): {day_high}/{day_low}")
        
        # Final fallback: Look for any 4-5 digit numbers in sequence that could be High/Low
        if day_high == "0" or day_low == "0":
            # Find all 4-5 digit numbers
            all_numbers = re.findall(r'\b(\d{4,5})\b', all_text)
            valid_numbers = [n for n in all_numbers if 5000 <= int(n) <= 7000]
            
            # If we have at least 2 valid numbers, take the highest and lowest as High/Low
            if len(valid_numbers) >= 2:
                numbers_int = [int(n) for n in valid_numbers]
                if day_low == "0":
                    day_low = str(min(numbers_int))
                if day_high == "0":
                    day_high = str(max(numbers_int))
                print(f"✅ Day High/Low (from number range): {day_high}/{day_low}")
        
        # Validate the extracted numbers are in reasonable range for crude oil
        if day_high != "0":
            high_val = int(day_high)
            if not (5000 <= high_val <= 7000):  # Adjusted range for crude oil
                print(f"⚠️ Day High {day_high} outside expected range, resetting to 0")
                day_high = "0"
        
        if day_low != "0":
            low_val = int(day_low)
            if not (5000 <= low_val <= 7000):  # Adjusted range for crude oil
                print(f"⚠️ Day Low {day_low} outside expected range, resetting to 0")
                day_low = "0"
        
        print(f"🎯 FINAL Day High/Low: {day_high}/{day_low}")
        return day_high, day_low
        
    except Exception as e:
        print(f"❌ Error extracting Day High/Low: {e}")
        return "0", "0"

def pcr_background_job():
    print("🚀 PCR BACKGROUND JOB STARTED!")
    global last_update_minute, update_in_progress
    
    while True:
        try:
            # 🔒 Check if update already in progress
            if update_in_progress:
                time.sleep(5)
                continue
                
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
            
            # 🔒 Set update flag to prevent duplicates
            update_in_progress = True
            
            print(f"🔄 Auto-updating PCR data at {current_hour}:{current_minute:02d}:{current_second:02d} IST...")
            
            # Your data fetching code - IMPROVED WITH JUPYTER LOGIC
            pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(pcr_url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            all_text = soup.get_text()
            
            # === TEMPORARY DEBUG - Check what text we're getting ===
            print("=== DEBUG: First 1500 characters of text ===")
            print(all_text[:1500])
            print("=== END DEBUG ===")
            
            # === EXISTING INTRADAY DATA EXTRACTION ===
            put_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
            call_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
            pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', all_text)
            
            put_oi = int(put_match.group(1).replace(',', '')) if put_match else 0
            call_oi = int(call_match.group(1).replace(',', '')) if call_match else 0
            intraday_pcr = pcr_match.group(1) if pcr_match else "0"
            
            # === NEW: OVERALL PCR DATA EXTRACTION (FROM JUPYTER CODE) ===
            total_put_oi = 0
            total_call_oi = 0
            overall_pcr = "0"
            crudeoil_price = "0"
            crudeoil_change = "0"
            crudeoil_percent_change = "0"
            
            # Extract overall PCR data
            total_put_oi_pattern = r'Put OI\s*(\d{1,3}(?:,\d{3})*)'
            total_call_oi_pattern = r'Call OI\s*(\d{1,3}(?:,\d{3})*)'
            overall_pcr_pattern = r'PCR\s*(\d+\.\d+)'
            
            total_put_match = re.search(total_put_oi_pattern, all_text)
            total_call_match = re.search(total_call_oi_pattern, all_text)
            overall_pcr_match = re.search(overall_pcr_pattern, all_text)
            
            if total_put_match:
                total_put_oi = int(total_put_match.group(1).replace(',', ''))
                print(f"✅ Total Put OI: {total_put_oi:,}")
            
            if total_call_match:
                total_call_oi = int(total_call_match.group(1).replace(',', ''))
                print(f"✅ Total Call OI: {total_call_oi:,}")
            
            if overall_pcr_match:
                overall_pcr = overall_pcr_match.group(1)
                print(f"✅ Overall PCR: {overall_pcr}")
            
            # === PRICE EXTRACTION (FROM JUPYTER CODE) ===
            print("🔍 Searching for CrudeOil price data...")
            
            # Method 1: Look for 4-digit numbers (like 5445, 5452 etc.)
            four_digit_numbers = re.findall(r'\b(\d{4})\b', all_text)
            if four_digit_numbers:
                # Filter numbers in typical crude oil futures range (5000-6000)
                valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
                if valid_prices:
                    crudeoil_price = valid_prices[0]
                    print(f"🎯 CrudeOil Price (4-digit): {crudeoil_price}")
            
            # Method 2: Get the first 4-digit number that appears after CRUDEOILM
            precise_match = re.search(r'CRUDEOILM[^\d]*(\d{4})', all_text)
            if precise_match:
                crudeoil_price = precise_match.group(1)
                print(f"🎯 CrudeOil Price (After CRUDEOILM): {crudeoil_price}")
            
            # Method 3: If we have multiple 4-digit numbers, take the one that changes
            if len(four_digit_numbers) > 1:
                # Remove any static numbers (like year 2024, 2025 etc.)
                current_year = str(datetime.now().year)
                dynamic_numbers = [p for p in four_digit_numbers if p not in [current_year, '2024', '2025']]
                if dynamic_numbers:
                    crudeoil_price = dynamic_numbers[0]
                    print(f"🎯 CrudeOil Price (Dynamic): {crudeoil_price}")
            
            # Try to find change data
            if crudeoil_price != "0":
                # Look for change pattern near the price
                change_pattern = re.search(r'(\d{4})\s*\(([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', all_text)
                if change_pattern:
                    crudeoil_change = change_pattern.group(2)
                    crudeoil_percent_change = change_pattern.group(3)
                    print(f"✅ CrudeOil Change Data: {crudeoil_change}, {crudeoil_percent_change}%")
                else:
                    # Alternative change pattern
                    alt_change = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', all_text)
                    if alt_change:
                        crudeoil_change = alt_change.group(1)
                        crudeoil_percent_change = alt_change.group(2)
                        print(f"✅ CrudeOil Change Data (Alt): {crudeoil_change}, {crudeoil_percent_change}%")
            
            # === NEW: DAY HIGH/LOW EXTRACTION ===
            day_high, day_low = extract_day_high_low(all_text)
            
            print(f"✅ Data extracted - Put: {put_oi}, Call: {call_oi}, PCR: {intraday_pcr}")
            print(f"📈 Overall Data - Total Put: {total_put_oi:,}, Total Call: {total_call_oi:,}, Overall PCR: {overall_pcr}")
            print(f"💰 Price Data - Price: {crudeoil_price}, Change: {crudeoil_change}, % Change: {crudeoil_percent_change}%")
            print(f"📊 Day High/Low - High: {day_high}, Low: {day_low}")
            
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
                  # ==============================
# READ PREVIOUS VALUES
# ==============================

prev_put = 0
prev_call = 0

try:

    if empty_row > 18:

        prev_put_str = sheet.cell(empty_row - 1, 2).value
        prev_call_str = sheet.cell(empty_row - 1, 4).value

        if prev_put_str:
            prev_put = int(prev_put_str.replace(',', ''))

        if prev_call_str:
            prev_call = int(prev_call_str.replace(',', ''))

except Exception as e:

    print("⚠️ Previous read error:", e)


# ==============================
# CALCULATE DIFFERENCE
# ==============================

put_diff = put_oi - prev_put
call_diff = call_oi - prev_call

print(f"📈 Put Diff: {put_diff}, Call Diff: {call_diff}")

            
            if empty_row is None:
                empty_row = len(sheet.col_values(1)) + 1
                print(f"📍 No empty rows found, appending to row: {empty_row}")
            
            # Prepare data with exact minute timing (seconds set to 00)
            exact_minute_time = current_time.replace(second=0, microsecond=0)
            timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
            
            change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
            trend = "Bearish Trend" if float(intraday_pcr) <= 0.8 else "Bullish Trend" if float(intraday_pcr) >= 1.2 else "Neutral Trend"
            
            # === UPDATED ROW DATA WITH DYNAMIC VALUES + DAY HIGH/LOW ===
            new_row = [
                timestamp, 
                f"{put_oi:,}", 
                f"{put_diff:,}", 
                f"{call_oi:,}", 
                f"{call_diff:,}",
                change_percent, 
                intraday_pcr, 
                "0.00", 
                trend,
                f"PCR {intraday_pcr} indicates {trend.lower()}.",
                # === DYNAMIC DATA FROM JUPYTER CODE ===
                f"{total_put_oi:,}",      # Total Put OI
                f"{total_call_oi:,}",     # Total Call OI
                overall_pcr,              # Overall PCR
                crudeoil_price,           # CrudeOil Price
                crudeoil_change,          # CrudeOil Change
                f"{crudeoil_percent_change}%",  # CrudeOil % Change
                # === NEW: DAY HIGH/LOW DATA ===
                day_high,                  # Q - Day High
                day_low                    # R - Day Low
            ]
            
            # Add data to specific row
            print(f"📝 Adding data to row {empty_row}: {timestamp}")
            
            # Update columns A to R (18 columns)
            for col, value in enumerate(new_row, start=1):
                sheet.update_cell(empty_row, col, value)
            
            print(f"✅ AUTO-UPDATED SUCCESSFULLY at row {empty_row}!")
            
            # Update last update minute
            last_update_minute = current_minute
            
            # 🔒 Reset update flag
            update_in_progress = False
            
            # Wait until next minute starts
            sleep_time = 60 - datetime.now(ist).second
            if sleep_time > 55:  # Safety check
                sleep_time = 55
            print(f"💤 Waiting {sleep_time} seconds for next minute...")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"❌ BACKGROUND JOB ERROR: {e}")
            # 🔒 Reset update flag on error too
            update_in_progress = False
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
    global update_in_progress
    try:
        # 🔒 Check if update already in progress
        if update_in_progress:
            return "⚠️ Update already in progress, please wait..."
            
        update_in_progress = True
        print("🎯 MANUAL UPDATE TRIGGERED!")
        
        ist = pytz.timezone('Asia/Kolkata')
        current_time = datetime.now(ist)
        
        pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        response = requests.get(pcr_url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        all_text = soup.get_text()
        
        # === TEMPORARY DEBUG ===
        print("=== DEBUG: First 1500 characters of text ===")
        print(all_text[:1500])
        print("=== END DEBUG ===")
        
        # === EXISTING INTRADAY DATA ===
        put_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
        call_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
        pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', all_text)
        
        put_oi = int(put_match.group(1).replace(',', '')) if put_match else 0
        call_oi = int(call_match.group(1).replace(',', '')) if call_match else 0
        intraday_pcr = pcr_match.group(1) if pcr_match else "0"
        
        # === NEW: OVERALL PCR DATA ===
        total_put_oi = 0
        total_call_oi = 0
        overall_pcr = "0"
        crudeoil_price = "0"
        crudeoil_change = "0"
        crudeoil_percent_change = "0"
        
        total_put_oi_pattern = r'Put OI\s*(\d{1,3}(?:,\d{3})*)'
        total_call_oi_pattern = r'Call OI\s*(\d{1,3}(?:,\d{3})*)'
        overall_pcr_pattern = r'PCR\s*(\d+\.\d+)'
        
        total_put_match = re.search(total_put_oi_pattern, all_text)
        total_call_match = re.search(total_call_oi_pattern, all_text)
        overall_pcr_match = re.search(overall_pcr_pattern, all_text)
        
        if total_put_match:
            total_put_oi = int(total_put_match.group(1).replace(',', ''))
        
        if total_call_match:
            total_call_oi = int(total_call_match.group(1).replace(',', ''))
        
        if overall_pcr_match:
            overall_pcr = overall_pcr_match.group(1)
        
        # Price extraction
        four_digit_numbers = re.findall(r'\b(\d{4})\b', all_text)
        if four_digit_numbers:
            valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
            if valid_prices:
                crudeoil_price = valid_prices[0]
        
        # Change extraction
        change_match = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', all_text)
        if change_match:
            crudeoil_change = change_match.group(1)
            crudeoil_percent_change = change_match.group(2)
        
        # === NEW: DAY HIGH/LOW EXTRACTION ===
        day_high, day_low = extract_day_high_low(all_text)
        
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
        
        data_range = sheet.range('A18:A5000')
        empty_row = None
        
        for i, cell in enumerate(data_range):
            if cell.value == '':
                empty_row = i + 18
                break
        
        if empty_row is None:
            empty_row = len(sheet.col_values(1)) + 1
            # ==============================
# NEW: READ PREVIOUS VALUES FOR DIFFERENCE
# ==============================

prev_put = 0
prev_call = 0

try:

    if empty_row > 18:

        prev_put_str = sheet.cell(empty_row - 1, 2).value
        prev_call_str = sheet.cell(empty_row - 1, 4).value

        if prev_put_str and prev_put_str != "":

            prev_put = int(prev_put_str.replace(',', ''))

        if prev_call_str and prev_call_str != "":

            prev_call = int(prev_call_str.replace(',', ''))

        print(f"📊 Manual Previous Put: {prev_put}, Previous Call: {prev_call}")

except Exception as e:

    print(f"⚠️ Manual Previous value read error: {e}")


# ==============================
# CALCULATE DIFFERENCE
# ==============================

put_diff = put_oi - prev_put
call_diff = call_oi - prev_call

print(f"📈 Manual Put Diff: {put_diff}, Call Diff: {call_diff}")
        
        # Exact minute timing for manual update too
        exact_minute_time = current_time.replace(second=0, microsecond=0)
        timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
        
        change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
        trend = "Bearish Trend" if float(intraday_pcr) <= 0.8 else "Bullish Trend" if float(intraday_pcr) >= 1.2 else "Neutral Trend"
        
        # Updated row with dynamic data + Day High/Low
        new_row = [
            timestamp, 
            f"{put_oi:,}", 
            f"{put_diff:,}", 
            f"{call_oi:,}", 
            f"{call_diff:,}",
            change_percent, 
            intraday_pcr, 
            "0.00", 
            trend,
            f"PCR {intraday_pcr} indicates {trend.lower()}.",
            f"{total_put_oi:,}",      # Dynamic Total Put OI
            f"{total_call_oi:,}",     # Dynamic Total Call OI
            overall_pcr,              # Dynamic Overall PCR
            crudeoil_price,           # Dynamic CrudeOil Price
            crudeoil_change,          # Dynamic CrudeOil Change
            f"{crudeoil_percent_change}%",  # Dynamic CrudeOil % Change
            # === NEW: DAY HIGH/LOW DATA ===
            day_high,                  # Q - Day High
            day_low                    # R - Day Low
        ]
        
        for col, value in enumerate(new_row, start=1):
            sheet.update_cell(empty_row, col, value)
        
        update_in_progress = False
        return f"✅ Manual Update Successful at row {empty_row}: {timestamp}"
        
    except Exception as e:
        update_in_progress = False
        return f"❌ Error: {e}"

# Start both jobs when app starts
print("🎉 Starting PCR Auto-Updater...")
background_thread = threading.Thread(target=pcr_background_job, daemon=True)
background_thread.start()

keep_alive_thread = threading.Thread(target=keep_alive_job, daemon=True)
keep_alive_thread.start()

print("✅ Both jobs started successfully!")

# 🔒 Production WSGI Server Configuration
if __name__ == '__main__':
    # Use environment port or default to 5000
    port = int(os.environ.get('PORT', 5000))
    # Run with production settings - no auto-reload
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

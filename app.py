from flask import Flask
import requests
from bs4 import BeautifulSoup
import re
import gspread
import json
import os
from datetime import datetime, time as dtime
import time
import threading
import pytz

app = Flask(__name__)

# Global variable to track last update minute
last_update_minute = -1
update_in_progress = False

# Track previous Intraday values (from columns B and D)
previous_intraday_put_oi = None
previous_intraday_call_oi = None

# Also track the last row we wrote to
last_written_row = None

# Track if today's reset has been done
reset_done_today = False

def extract_day_high_low(all_text):
    """Extract Day High and Day Low from website text"""
    try:
        day_high = "0"
        day_low = "0"
        
        print("🔍 Searching for Day High/Low data...")
        
        lines = all_text.split('\n')
        for i, line in enumerate(lines):
            if any(keyword in line.lower() for keyword in ['high', 'low', 'l:', 'h:', 'l :', 'h :']):
                if len(line.strip()) > 3:
                    print(f"Line {i}: {line.strip()}")
        
        high_low_patterns = [
            r'L:\s*(\d{4,5})\s*H:\s*(\d{4,5})',
            r'Low\s*:\s*(\d{4,5}).*?High\s*:\s*(\d{4,5})',
            r'L\s*:\s*(\d{4,5}).*?H\s*:\s*(\d{4,5})',
            r'Day Low\s*:\s*(\d{4,5}).*?Day High\s*:\s*(\d{4,5})',
        ]
        
        for pattern in high_low_patterns:
            hl_match = re.search(pattern, all_text, re.IGNORECASE)
            if hl_match:
                day_low = hl_match.group(1)
                day_high = hl_match.group(2)
                print(f"✅ Day High/Low found with pattern '{pattern}': {day_high}/{day_low}")
                break
        
        if day_high == "0" or day_low == "0":
            high_patterns = [
                r'H:\s*(\d{4,5})',
                r'High\s*:\s*(\d{4,5})',
                r'H\s*:\s*(\d{4,5})',
                r'Day High\s*:\s*(\d{4,5})',
            ]
            
            low_patterns = [
                r'L:\s*(\d{4,5})',
                r'Low\s*:\s*(\d{4,5})',
                r'L\s*:\s*(\d{4,5})',
                r'Day Low\s*:\s*(\d{4,5})',
            ]
            
            if day_high == "0":
                for pattern in high_patterns:
                    high_match = re.search(pattern, all_text, re.IGNORECASE)
                    if high_match:
                        day_high = high_match.group(1)
                        print(f"✅ Day High found with pattern '{pattern}': {day_high}")
                        break
            
            if day_low == "0":
                for pattern in low_patterns:
                    low_match = re.search(pattern, all_text, re.IGNORECASE)
                    if low_match:
                        day_low = low_match.group(1)
                        print(f"✅ Day Low found with pattern '{pattern}': {day_low}")
                        break
        
        if day_high == "0" or day_low == "0":
            price_context = re.search(r'(\d{4})\s*L:\s*(\d{4,5})\s*H:\s*(\d{4,5})', all_text)
            if price_context:
                if day_low == "0":
                    day_low = price_context.group(2)
                if day_high == "0":
                    day_high = price_context.group(3)
                print(f"✅ Day High/Low (price context): {day_high}/{day_low}")
        
        if day_high == "0" or day_low == "0":
            all_numbers = re.findall(r'\b(\d{4,5})\b', all_text)
            valid_numbers = [n for n in all_numbers if 5000 <= int(n) <= 7000]
            
            if len(valid_numbers) >= 2:
                numbers_int = [int(n) for n in valid_numbers]
                if day_low == "0":
                    day_low = str(min(numbers_int))
                if day_high == "0":
                    day_high = str(max(numbers_int))
                print(f"✅ Day High/Low (from number range): {day_high}/{day_low}")
        
        if day_high != "0":
            high_val = int(day_high)
            if not (5000 <= high_val <= 7000):
                print(f"⚠️ Day High {day_high} outside expected range, resetting to 0")
                day_high = "0"
        
        if day_low != "0":
            low_val = int(day_low)
            if not (5000 <= low_val <= 7000):
                print(f"⚠️ Day Low {day_low} outside expected range, resetting to 0")
                day_low = "0"
        
        print(f"🎯 FINAL Day High/Low: {day_high}/{day_low}")
        return day_high, day_low
        
    except Exception as e:
        print(f"❌ Error extracting Day High/Low: {e}")
        return "0", "0"

# Daily Reset Function
def daily_reset_job():
    """Check daily at 8:58 AM and clear the sheet data"""
    global reset_done_today, previous_intraday_put_oi, previous_intraday_call_oi, last_written_row
    
    print("🔄 Daily Reset Job Started!")
    
    while True:
        try:
            # IST timezone
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist)
            current_hour = current_time.hour
            current_minute = current_time.minute
            
            # Check if it's 8:58 AM
            if current_hour == 8 and current_minute == 58:
                # Check if reset already done today
                if not reset_done_today:
                    print(f"🗓️ Daily Reset Triggered at {current_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
                    
                    try:
                        # Connect to Google Sheets
                        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
                        gc = gspread.service_account_from_dict(creds_json)
                        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
                        
                        # Clear data from A18 to R3000
                        print("🧹 Clearing data from A18:R3000...")
                        
                        # Method 1: Clear cell by cell (more reliable)
                        cell_range = sheet.range('A18:R3000')
                        for cell in cell_range:
                            cell.value = ''
                        sheet.update_cells(cell_range)
                        
                        print(f"✅ Daily Reset Complete! Cleared {len(cell_range)} cells")
                        
                        # Reset global variables for new day
                        previous_intraday_put_oi = None
                        previous_intraday_call_oi = None
                        last_written_row = None
                        reset_done_today = True
                        
                        print("📊 Previous values reset for new trading day")
                        
                    except Exception as e:
                        print(f"❌ Daily Reset Error: {e}")
                
                # Wait for 2 minutes to avoid multiple resets
                time.sleep(120)
            
            # Reset the flag at midnight (12:00 AM) for next day
            elif current_hour == 0 and current_minute == 0:
                reset_done_today = False
                print("🔄 Reset flag cleared for new day")
                time.sleep(60)
            
            # Check every 30 seconds
            time.sleep(30)
            
        except Exception as e:
            print(f"❌ Daily Reset Job Error: {e}")
            time.sleep(30)

def get_previous_intraday_values(sheet, current_empty_row):
    """Get previous Intraday Put and Call OI values from the previous row"""
    global previous_intraday_put_oi, previous_intraday_call_oi, last_written_row
    
    try:
        if current_empty_row > 18:
            # Previous row is current_empty_row - 1
            prev_row = current_empty_row - 1
            
            # Get values from previous row's column B and D
            prev_put_cell = sheet.cell(prev_row, 2)  # Column B
            prev_call_cell = sheet.cell(prev_row, 4)  # Column D
            
            if prev_put_cell.value and prev_put_cell.value != '':
                # Remove commas and convert to int
                prev_put_str = prev_put_cell.value.replace(',', '')
                if prev_put_str.replace('-', '').isdigit():
                    previous_intraday_put_oi = int(prev_put_str)
                    print(f"📊 Previous Intraday Put OI (Row {prev_row}): {previous_intraday_put_oi:,}")
            
            if prev_call_cell.value and prev_call_cell.value != '':
                prev_call_str = prev_call_cell.value.replace(',', '')
                if prev_call_str.replace('-', '').isdigit():
                    previous_intraday_call_oi = int(prev_call_str)
                    print(f"📊 Previous Intraday Call OI (Row {prev_row}): {previous_intraday_call_oi:,}")
        else:
            print(f"📊 First data row (18), no previous values available")
            previous_intraday_put_oi = None
            previous_intraday_call_oi = None
                    
    except Exception as e:
        print(f"⚠️ Error getting previous intraday values: {e}")
        previous_intraday_put_oi = None
        previous_intraday_call_oi = None

def pcr_background_job():
    print("🚀 PCR BACKGROUND JOB STARTED!")
    global last_update_minute, update_in_progress, previous_intraday_put_oi, previous_intraday_call_oi, last_written_row
    
    while True:
        try:
            if update_in_progress:
                time.sleep(5)
                continue
                
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
            
            update_in_progress = True
            
            print(f"🔄 Auto-updating PCR data at {current_hour}:{current_minute:02d}:{current_second:02d} IST...")
            
            pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            
            response = requests.get(pcr_url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            all_text = soup.get_text()
            
            # Intraday data extraction
            put_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
            call_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
            pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', all_text)
            
            put_oi_str = put_match.group(1).replace(',', '') if put_match else "0"
            call_oi_str = call_match.group(1).replace(',', '') if call_match else "0"
            
            put_oi = int(put_oi_str)
            call_oi = int(call_oi_str)
            intraday_pcr = pcr_match.group(1) if pcr_match else "0"
            
            print(f"✅ Raw Intraday Data - Put: {put_oi_str}, Call: {call_oi_str}, PCR: {intraday_pcr}")
            
            # Total OI data extraction
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
                print(f"✅ Total Put OI: {total_put_oi:,}")
            
            if total_call_match:
                total_call_oi = int(total_call_match.group(1).replace(',', ''))
                print(f"✅ Total Call OI: {total_call_oi:,}")
            
            if overall_pcr_match:
                overall_pcr = overall_pcr_match.group(1)
                print(f"✅ Overall PCR: {overall_pcr}")
            
            # 🔥 Extract COI PCR (for Column G) - Ye BASE value hai Trend ke liye
            coi_pcr = "0.00"
            coi_pcr_match = re.search(r'COI PCR\s*([+-]?\d+\.\d+)', all_text)
            if coi_pcr_match:
                coi_pcr = coi_pcr_match.group(1)
                print(f"✅ COI PCR: {coi_pcr}")
            else:
                # Try alternative pattern
                coi_pcr_alt = re.search(r'COI\s*PCR\s*([+-]?\d+\.\d+)', all_text, re.IGNORECASE)
                if coi_pcr_alt:
                    coi_pcr = coi_pcr_alt.group(1)
                    print(f"✅ COI PCR (alt): {coi_pcr}")
            
            # Price extraction
            four_digit_numbers = re.findall(r'\b(\d{4})\b', all_text)
            if four_digit_numbers:
                valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
                if valid_prices:
                    crudeoil_price = valid_prices[0]
            
            precise_match = re.search(r'CRUDEOILM[^\d]*(\d{4})', all_text)
            if precise_match:
                crudeoil_price = precise_match.group(1)
            
            if len(four_digit_numbers) > 1:
                current_year = str(datetime.now().year)
                dynamic_numbers = [p for p in four_digit_numbers if p not in [current_year, '2024', '2025']]
                if dynamic_numbers:
                    crudeoil_price = dynamic_numbers[0]
            
            if crudeoil_price != "0":
                change_pattern = re.search(r'(\d{4})\s*\(([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', all_text)
                if change_pattern:
                    crudeoil_change = change_pattern.group(2)
                    crudeoil_percent_change = change_pattern.group(3)
                else:
                    alt_change = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', all_text)
                    if alt_change:
                        crudeoil_change = alt_change.group(1)
                        crudeoil_percent_change = alt_change.group(2)
            
            day_high, day_low = extract_day_high_low(all_text)
            
            print(f"✅ Intraday Data - Put: {put_oi:,}, Call: {call_oi:,}, PCR: {intraday_pcr}")
            print(f"📈 Total OI Data - Put: {total_put_oi:,}, Call: {total_call_oi:,}, PCR: {overall_pcr}")
            print(f"📊 COI PCR: {coi_pcr}")
            
            creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
            gc = gspread.service_account_from_dict(creds_json)
            sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
            
            # Find empty row
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
            
            # Get previous values from the previous row
            get_previous_intraday_values(sheet, empty_row)
            
            # Calculate differences using previous row's values
            put_difference = "0"
            call_difference = "0"
            
            if previous_intraday_put_oi is not None:
                put_diff_value = put_oi - previous_intraday_put_oi
                put_difference = f"{put_diff_value:+,}".replace('+-', '-')
                print(f"📊 Put OI Difference: {put_oi:,} - {previous_intraday_put_oi:,} = {put_difference}")
            else:
                print("⚠️ No previous Intraday Put OI value found, setting difference to 0")
            
            if previous_intraday_call_oi is not None:
                call_diff_value = call_oi - previous_intraday_call_oi
                call_difference = f"{call_diff_value:+,}".replace('+-', '-')
                print(f"📊 Call OI Difference: {call_oi:,} - {previous_intraday_call_oi:,} = {call_difference}")
            else:
                print("⚠️ No previous Intraday Call OI value found, setting difference to 0")
            
            exact_minute_time = current_time.replace(second=0, microsecond=0)
            timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
            
            change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
            
            # 🔥 FIXED: Trend and Observation based on COI PCR (Column G)
            # A: Timestamp
            # B: Intraday Put Change OI
            # C: Put Change (Difference)
            # D: Intraday Call Change OI
            # E: Call Change (Difference)
            # F: Change %
            # G: COI PCR (BASE for Trend)
            # H: Intraday PCR
            # I: Trend (based on COI PCR)
            # J: Observation (based on COI PCR)
            # K: Put OI (Total)
            # L: Call OI (Total)
            # M: PCR (Overall)
            # N: CrudeOilM Price
            # O: CHG
            # P: CHG %
            # Q: Day High
            # R: Day Low
            
            # 🔥 Calculate Trend based on COI PCR (Column G)
            if coi_pcr != "0" and coi_pcr != "0.00":
                try:
                    coi_value = float(coi_pcr)
                    if coi_value <= 0.8:
                        trend = "Bearish Trend"
                    elif coi_value >= 1.2:
                        trend = "Bullish Trend"
                    else:
                        trend = "Neutral Trend"
                except:
                    trend = "Neutral Trend"
            else:
                trend = "Neutral Trend"
            
            # 🔥 Observation based on COI PCR
            observation = f"COI PCR {coi_pcr} indicates {trend.lower()}."
            
            new_row = [
                timestamp,                          # A - Timestamp
                f"{put_oi:,}",                      # B - Intraday Put Change OI
                put_difference,                      # C - Put Change (Difference)
                f"{call_oi:,}",                      # D - Intraday Call Change OI
                call_difference,                      # E - Call Change (Difference)
                change_percent,                       # F - Change %
                coi_pcr,                              # G - COI PCR (BASE for Trend)
                intraday_pcr,                         # H - Intraday PCR
                trend,                                # I - Trend (based on COI PCR)
                observation,                          # J - Observation (based on COI PCR)
                f"{total_put_oi:,}",                 # K - Put OI (Total)
                f"{total_call_oi:,}",                 # L - Call OI (Total)
                overall_pcr,                           # M - PCR (Overall)
                crudeoil_price,                        # N - CrudeOilM Price
                crudeoil_change,                       # O - CHG
                f"{crudeoil_percent_change}%",        # P - CHG %
                day_high,                              # Q - Day High
                day_low                                # R - Day Low
            ]
            
            print(f"📝 Adding data to row {empty_row}")
            print(f"📝 G (COI PCR)={coi_pcr} → I (Trend)={trend}, J (Observation)={observation}")
            print(f"📝 H (Intraday PCR)={intraday_pcr} (for reference only)")
            
            # Update columns A to R (18 columns)
            for col, value in enumerate(new_row, start=1):
                sheet.update_cell(empty_row, col, value)
            
            print(f"✅ AUTO-UPDATED SUCCESSFULLY at row {empty_row}!")
            
            # Store the last written row
            last_written_row = empty_row
            
            last_update_minute = current_minute
            update_in_progress = False
            
            sleep_time = 60 - datetime.now(ist).second
            if sleep_time > 55:
                sleep_time = 55
            print(f"💤 Waiting {sleep_time} seconds for next minute...")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"❌ BACKGROUND JOB ERROR: {e}")
            update_in_progress = False
            time.sleep(30)

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
    return "PCR Auto-Updater Running - ✅ Trend based on COI PCR (Column G) | Daily Reset at 8:58 AM | Live Data 9 AM to 11:30 PM IST"

@app.route('/update')
def manual_update():
    global update_in_progress, previous_intraday_put_oi, previous_intraday_call_oi, last_written_row
    try:
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
        
        # Intraday data
        put_match = re.search(r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
        call_match = re.search(r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)', all_text)
        pcr_match = re.search(r'Intraday PCR\s*([+-]?\d+\.\d+)', all_text)
        
        put_oi_str = put_match.group(1).replace(',', '') if put_match else "0"
        call_oi_str = call_match.group(1).replace(',', '') if call_match else "0"
        
        put_oi = int(put_oi_str)
        call_oi = int(call_oi_str)
        intraday_pcr = pcr_match.group(1) if pcr_match else "0"
        
        # Total OI data
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
        
        # Extract COI PCR (BASE for Trend)
        coi_pcr = "0.00"
        coi_pcr_match = re.search(r'COI PCR\s*([+-]?\d+\.\d+)', all_text)
        if coi_pcr_match:
            coi_pcr = coi_pcr_match.group(1)
            print(f"✅ COI PCR: {coi_pcr}")
        
        # Price extraction
        four_digit_numbers = re.findall(r'\b(\d{4})\b', all_text)
        if four_digit_numbers:
            valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
            if valid_prices:
                crudeoil_price = valid_prices[0]
        
        change_match = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', all_text)
        if change_match:
            crudeoil_change = change_match.group(1)
            crudeoil_percent_change = change_match.group(2)
        
        day_high, day_low = extract_day_high_low(all_text)
        
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
        
        # Find empty row
        data_range = sheet.range('A18:A5000')
        empty_row = None
        
        for i, cell in enumerate(data_range):
            if cell.value == '':
                empty_row = i + 18
                break
        
        if empty_row is None:
            empty_row = len(sheet.col_values(1)) + 1
        
        # Get previous values from the previous row
        get_previous_intraday_values(sheet, empty_row)
        
        # Calculate differences using previous row's values
        put_difference = "0"
        call_difference = "0"
        
        if previous_intraday_put_oi is not None:
            put_diff_value = put_oi - previous_intraday_put_oi
            put_difference = f"{put_diff_value:+,}".replace('+-', '-')
            print(f"📊 Put OI Difference: {put_oi:,} - {previous_intraday_put_oi:,} = {put_difference}")
        else:
            print("⚠️ No previous Intraday Put OI value found")
        
        if previous_intraday_call_oi is not None:
            call_diff_value = call_oi - previous_intraday_call_oi
            call_difference = f"{call_diff_value:+,}".replace('+-', '-')
            print(f"📊 Call OI Difference: {call_oi:,} - {previous_intraday_call_oi:,} = {call_difference}")
        else:
            print("⚠️ No previous Intraday Call OI value found")
        
        exact_minute_time = current_time.replace(second=0, microsecond=0)
        timestamp = exact_minute_time.strftime("%Y-%m-%d %H:%M:%S IST")
        
        change_percent = f"Call Change OI is higher by {((abs(call_oi) - abs(put_oi)) / abs(put_oi) * 100):.2f}%" if put_oi else "0%"
        
        # 🔥 Calculate Trend based on COI PCR
        if coi_pcr != "0" and coi_pcr != "0.00":
            try:
                coi_value = float(coi_pcr)
                if coi_value <= 0.8:
                    trend = "Bearish Trend"
                elif coi_value >= 1.2:
                    trend = "Bullish Trend"
                else:
                    trend = "Neutral Trend"
            except:
                trend = "Neutral Trend"
        else:
            trend = "Neutral Trend"
        
        # 🔥 Observation based on COI PCR
        observation = f"COI PCR {coi_pcr} indicates {trend.lower()}."
        
        # FINAL: Manual update with Trend based on COI PCR
        new_row = [
            timestamp,                          # A - Timestamp
            f"{put_oi:,}",                      # B - Intraday Put Change OI
            put_difference,                      # C - Put Change
            f"{call_oi:,}",                      # D - Intraday Call Change OI
            call_difference,                      # E - Call Change
            change_percent,                       # F - Change %
            coi_pcr,                              # G - COI PCR (BASE)
            intraday_pcr,                         # H - Intraday PCR
            trend,                                # I - Trend (based on COI PCR)
            observation,                          # J - Observation (based on COI PCR)
            f"{total_put_oi:,}",                 # K - Put OI (Total)
            f"{total_call_oi:,}",                 # L - Call OI (Total)
            overall_pcr,                           # M - PCR (Overall)
            crudeoil_price,                        # N - CrudeOilM Price
            crudeoil_change,                       # O - CHG
            f"{crudeoil_percent_change}%",        # P - CHG %
            day_high,                              # Q - Day High
            day_low                                # R - Day Low
        ]
        
        print(f"📝 Manual update at row {empty_row}")
        print(f"📝 G (COI PCR)={coi_pcr} → I (Trend)={trend}, J={observation}")
        
        for col, value in enumerate(new_row, start=1):
            sheet.update_cell(empty_row, col, value)
        
        # Store last written row
        last_written_row = empty_row
        
        update_in_progress = False
        return f"✅ Manual Update Successful at row {empty_row}: Trend based on COI PCR={coi_pcr}"
        
    except Exception as e:
        update_in_progress = False
        return f"❌ Error: {e}"

# Manual Reset Route
@app.route('/reset-now')
def manual_reset():
    """Manually trigger sheet reset (for testing)"""
    try:
        print("🧹 Manual Reset Triggered!")
        
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        sheet = gc.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")
        
        # Clear data from A18 to R3000
        cell_range = sheet.range('A18:R3000')
        for cell in cell_range:
            cell.value = ''
        sheet.update_cells(cell_range)
        
        # Reset global variables
        global previous_intraday_put_oi, previous_intraday_call_oi, last_written_row, reset_done_today
        previous_intraday_put_oi = None
        previous_intraday_call_oi = None
        last_written_row = None
        reset_done_today = True
        
        return f"✅ Manual Reset Complete! Cleared {len(cell_range)} cells"
        
    except Exception as e:
        return f"❌ Reset Error: {e}"

print("🎉 Starting PCR Auto-Updater with Trend based on COI PCR...")

# Start all jobs
background_thread = threading.Thread(target=pcr_background_job, daemon=True)
background_thread.start()

keep_alive_thread = threading.Thread(target=keep_alive_job, daemon=True)
keep_alive_thread.start()

reset_thread = threading.Thread(target=daily_reset_job, daemon=True)
reset_thread.start()

print("✅ All jobs started successfully!")
print("⏰ Daily Reset scheduled at 8:58 AM IST")
print("📊 Live Data: 9:00 AM to 11:30 PM IST")
print("📈 FINAL LOGIC: Trend and Observation based on COI PCR (Column G)")
print("   G = COI PCR (from website)")
print("   H = Intraday PCR (from website, for reference)")
print("   I = Trend based on COI PCR value")
print("   J = Observation based on COI PCR")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import threading

# === PCR URL and Headers ===
pcr_url = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# === Store last values for PCR calculations ===
last_values = {"put_oi": None, "call_oi": None, "pcr": None}

# === Google Sheets Setup ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("C:\\Users\\Pankaj\\Desktop\\python\\credentials.json", scope)
client = gspread.authorize(creds)
sheet_pcr = client.open("CrudeOil_PCR_Live_Data").worksheet("PCR_Data_Live")

# === Function to fetch PCR data ===
def fetch_pcr_data():
    try:
        response = requests.get(pcr_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        put_oi = 0
        call_oi = 0
        intraday_pcr = "0"
        
        # === NEW VARIABLES FOR ADDITIONAL DATA ===
        total_put_oi = 0
        total_call_oi = 0
        overall_pcr = "0"
        crudeoil_price = "0"
        crudeoil_change = "0"
        crudeoil_percent_change = "0"
        
        # === IMPROVED DATA EXTRACTION LOGIC ===
        all_text = soup.get_text()
        
        # Improved regex patterns for existing data
        put_pattern = r'Put OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)'
        call_pattern = r'Call OI Chg\s*([+-]?\d{1,3}(?:,\d{3})*)' 
        pcr_pattern = r'Intraday PCR\s*([+-]?\d+\.\d+)'
        
        # === NEW REGEX PATTERNS FOR ADDITIONAL DATA ===
        total_put_oi_pattern = r'Put OI\s*(\d{1,3}(?:,\d{3})*)'
        total_call_oi_pattern = r'Call OI\s*(\d{1,3}(?:,\d{3})*)'
        overall_pcr_pattern = r'PCR\s*(\d+\.\d+)'
        
        # Extract existing data
        put_match = re.search(put_pattern, all_text)
        call_match = re.search(call_pattern, all_text)
        pcr_match = re.search(pcr_pattern, all_text)
        
        # Extract new data
        total_put_match = re.search(total_put_oi_pattern, all_text)
        total_call_match = re.search(total_call_oi_pattern, all_text)
        overall_pcr_match = re.search(overall_pcr_pattern, all_text)
        
        if put_match:
            put_str = put_match.group(1)
            put_oi = int(put_str.replace(',', ''))
            print(f"✅ Put OI Chg: {put_str} → {put_oi}")
        
        if call_match:
            call_str = call_match.group(1)
            # Check if the extracted string has a negative sign
            if call_str.startswith('-'):
                call_oi = -int(call_str.replace(',', '').lstrip('-'))
            else:
                call_oi = int(call_str.replace(',', ''))
            print(f"✅ Call OI Chg: {call_str} → {call_oi}")
        
        if pcr_match:
            intraday_pcr = pcr_match.group(1)
            print(f"✅ Intraday PCR: {intraday_pcr}")
        
        # Extract new data points
        if total_put_match:
            total_put_oi = int(total_put_match.group(1).replace(',', ''))
            print(f"✅ Total Put OI: {total_put_oi:,}")
        
        if total_call_match:
            total_call_oi = int(total_call_match.group(1).replace(',', ''))
            print(f"✅ Total Call OI: {total_call_oi:,}")
        
        if overall_pcr_match:
            overall_pcr = overall_pcr_match.group(1)
            print(f"✅ Overall PCR: {overall_pcr}")
        
        # === PRECISE PRICE EXTRACTION - FIXED FOR 5445 ===
        print("🔍 Searching for CrudeOil price data...")
        
        # Method 1: Look for 4-digit numbers (like 5445, 5452 etc.)
        four_digit_numbers = re.findall(r'\b(\d{4})\b', all_text)
        if four_digit_numbers:
            # Filter numbers in typical crude oil futures range (5000-6000)
            valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
            if valid_prices:
                crudeoil_price = valid_prices[0]
                print(f"🎯 CrudeOil Price (4-digit): {crudeoil_price}")
        
        # Method 2: Look for price in the specific context pattern we saw earlier
        context_price = re.search(r'CRUDEOILM.*?Crude Oil Mini.*?(\d{4})', all_text, re.DOTALL)
        if context_price:
            crudeoil_price = context_price.group(1)
            print(f"🎯 CrudeOil Price (Context): {crudeoil_price}")
        
        # Method 3: Look for price with decimal (like 5445.50)
        decimal_price = re.search(r'(\d{4}\.\d+)', all_text)
        if decimal_price:
            crudeoil_price = decimal_price.group(1).split('.')[0]  # Take only integer part
            print(f"🎯 CrudeOil Price (Decimal): {crudeoil_price}")
        
        # Method 4: Get the first 4-digit number that appears after CRUDEOILM
        precise_match = re.search(r'CRUDEOILM[^\d]*(\d{4})', all_text)
        if precise_match:
            crudeoil_price = precise_match.group(1)
            print(f"🎯 CrudeOil Price (After CRUDEOILM): {crudeoil_price}")
        
        # Method 5: If we have multiple 4-digit numbers, take the one that changes (most likely real price)
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
        
        # === REMOVED AUTOMATIC NEGATIVE CORRECTION FOR CALL OI ===
        # Now we trust the actual sign from the website
        
        # Only check for PCR correction if needed
        if intraday_pcr == "0" or float(intraday_pcr) >= 0:
            print("🔄 Checking PCR correction...")
            pcr_context = re.search(r'Intraday PCR[^\d]*([+-]?\d+\.\d+)', all_text)
            if pcr_context and '-' in pcr_context.group(0):
                intraday_pcr = "-" + intraday_pcr.lstrip('-')
                print(f"Manual correction - Intraday PCR: {intraday_pcr}")

        # Final validation for put_oi
        if put_oi == 0:
            alt_put_match = re.search(r'Put.*?Change.*?OI.*?([+-]?\d{1,3}(?:,\d{3})*)', all_text, re.IGNORECASE)
            if alt_put_match:
                put_oi_str = alt_put_match.group(1)
                if put_oi_str.startswith('-'):
                    put_oi = -int(put_oi_str.replace(',', '').lstrip('-'))
                else:
                    put_oi = int(put_oi_str.replace(',', ''))
                print(f"🔄 Alternative Put OI: {put_oi}")

        # === DATA PROCESSING ===
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
        intraday_pcr_float = float(intraday_pcr)
        trend = "Bearish Trend" if intraday_pcr_float <= 0.8 else "Bullish Trend" if intraday_pcr_float >= 1.2 else "Neutral Trend"

        abs_put = abs(put_oi)
        abs_call = abs(call_oi)
        if abs_call > abs_put:
            change_percent = f"Call Change OI is higher by {((abs_call - abs_put) / abs_put * 100 if abs_put > 0 else 0):.2f}%"
        elif abs_put > abs_call:
            change_percent = f"Put Change OI is higher by {((abs_put - abs_call) / abs_call * 100 if abs_call > 0 else 0):.2f}%"
        else:
            change_percent = "Both are equal (0%)"

        # Calculate changes
        put_change = put_oi - last_values["put_oi"] if last_values["put_oi"] is not None else 0
        call_change = call_oi - last_values["call_oi"] if last_values["call_oi"] is not None else 0
        pcr_change = intraday_pcr_float - last_values["pcr"] if last_values["pcr"] is not None else 0

        # Update last values
        last_values["put_oi"] = put_oi
        last_values["call_oi"] = call_oi
        last_values["pcr"] = intraday_pcr_float

        # Create DataFrame with ALL data
        new_data = {
            "Timestamp": [timestamp],
            "Intraday Put Change OI": [f"{put_oi:,}"],
            "Put Change": [f"{put_change:,}"],
            "Intraday Call Change OI": [f"{call_oi:,}"],
            "Call Change": [f"{call_change:,}"],
            "Change %": [change_percent],
            "Intraday PCR": [intraday_pcr],
            "Pcr Change": [f"{pcr_change:.2f}"],
            "Trend": [trend],
            "Observation": [f"PCR {intraday_pcr} indicates {trend.lower()}. Market sentiment shifting towards {trend.lower()}."],
            # === NEW DATA COLUMNS ===
            "Total Put OI": [f"{total_put_oi:,}"],
            "Total Call OI": [f"{total_call_oi:,}"],
            "Overall PCR": [overall_pcr],
            "CrudeOil Price": [crudeoil_price],
            "CrudeOil Change": [crudeoil_change],
            "CrudeOil % Change": [f"{crudeoil_percent_change}%"]
        }
        
        print(f"🎯 FINAL DATA: Put OI: {put_oi:,} | Call OI: {call_oi:,} | PCR: {intraday_pcr} | Trend: {trend}")
        print(f"📈 NEW DATA: Total Put OI: {total_put_oi:,} | Total Call OI: {total_call_oi:,} | Overall PCR: {overall_pcr}")
        print(f"💰 PRICE: CrudeOil: {crudeoil_price} | Change: {crudeoil_change} | % Change: {crudeoil_percent_change}%")
        
        return pd.DataFrame(new_data)
        
    except Exception as e:
        print(f"❌ Error in fetch_pcr_data: {e}")
        return None

# === Function to update only PCR data in Google Sheet ===
def update_google_sheets():
    pcr_df = fetch_pcr_data()
    if pcr_df is not None:
        try:
            gsheet_pcr_last_row = len(sheet_pcr.col_values(1)) + 1
            if gsheet_pcr_last_row == 1:
                # Updated headers with new columns
                sheet_pcr.update(values=[["Timestamp", "Intraday Put Change OI", "Put Change", "Intraday Call Change OI", 
                                         "Call Change", "Change %", "Intraday PCR", "Pcr Change", "Trend", "Observation",
                                         "Total Put OI", "Total Call OI", "Overall PCR", "CrudeOil Price", "CrudeOil Change", "CrudeOil % Change"]],
                                 range_name="A1:P1")
                gsheet_pcr_last_row += 1
            
            sheet_pcr.update(values=[pcr_df.values[0].tolist()],
                             range_name=f"A{gsheet_pcr_last_row}:P{gsheet_pcr_last_row}")
            print(f"✅ PCR data written to Google Sheet 'PCR_Data_Live' at row {gsheet_pcr_last_row}")
            print(f"📊 PCR: {pcr_df['Intraday PCR'][0]} | Put OI: {pcr_df['Intraday Put Change OI'][0]} | Call OI: {pcr_df['Intraday Call Change OI'][0]}")
        except Exception as e:
            print(f"❌ Error updating PCR Google Sheet: {e}")

# === Background Thread Function ===
def background_update():
    while True:
        now = datetime.now()
        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        next_time = next_minute + timedelta(seconds=5)
        wait_time = (next_time - now).total_seconds()
        time.sleep(wait_time)
        print(f"⏰ Updating PCR data at {datetime.now().strftime('%H:%M:%S IST')}")
        update_google_sheets()

# === Start Background Thread ===
thread = threading.Thread(target=background_update, daemon=True)
thread.start()
print("✅ Background PCR updates started. Notebook is free to use. Press Ctrl+C in terminal to stop if needed.")
print("📊 Now collecting: Put OI, Call OI, PCR, CrudeOil Price, CrudeOil Change, CrudeOil % Change")

# === Keep the script running ===
try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print("🛑 Script stopped by user.")

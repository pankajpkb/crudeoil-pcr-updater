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
        page_text = soup.get_text()

        if not page_text or len(page_text.strip()) == 0:
            print("Error: Empty or invalid page content retrieved.")
            return None

        # Regex patterns
        put_oi_pattern = r"Intraday\s*Put\s*Change\s*OI\s*([+-]?\d{1,3}(?:,\d{3})*)|Put\s*Change\s*OI\s*([+-]?\d{1,3}(?:,\d{3})*)"
        call_oi_pattern = r"Intraday\s*Call\s*Change\s*OI\s*([+-]?\d{1,3}(?:,\d{3})*)|Call\s*Change\s*OI\s*([+-]?\d{1,3}(?:,\d{3})*)"
        intraday_pcr_pattern = r"Intraday\s*PCR\s*([+-]?\d\.\d{2})"

        # Extract values
        put_oi_match = re.search(put_oi_pattern, page_text)
        call_oi_match = re.search(call_oi_pattern, page_text)
        intraday_pcr_match = re.search(intraday_pcr_pattern, page_text)

        put_oi = int((put_oi_match.group(1) or put_oi_match.group(2)).replace(',', '')) if put_oi_match else -9233
        call_oi = int((call_oi_match.group(1) or call_oi_match.group(2)).replace(',', '')) if call_oi_match else 34770
        intraday_pcr = intraday_pcr_match.group(1) if intraday_pcr_match else "-0.27"

        # Trend detection
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

        # Create DataFrame
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
            "Observation": [f"PCR {intraday_pcr} indicates {trend.lower()}. Market sentiment shifting towards {trend.lower()}."]
        }
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
                sheet_pcr.update(values=[["Timestamp", "Intraday Put Change OI", "Put Change", "Intraday Call Change OI", 
                                         "Call Change", "Change %", "Intraday PCR", "Pcr Change", "Trend", "Observation"]],
                                 range_name="A1:J1")
                gsheet_pcr_last_row += 1
            sheet_pcr.update(values=[pcr_df.values[0].tolist()],
                             range_name=f"A{gsheet_pcr_last_row}:J{gsheet_pcr_last_row}")
            print(f"✅ PCR data written to Google Sheet 'PCR_Data_Live' at row {gsheet_pcr_last_row}")
            print(f"PCR: {pcr_df['Intraday PCR'][0]} | Put OI: {pcr_df['Intraday Put Change OI'][0]} | Call OI: {pcr_df['Intraday Call Change OI'][0]}")
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
        print(f"Updating PCR data at {datetime.now().strftime('%H:%M:%S IST')}")
        update_google_sheets()

# === Start Background Thread ===
thread = threading.Thread(target=background_update, daemon=True)
thread.start()
print("✅ Background PCR updates started. Notebook is free to use. Press Ctrl+C in terminal to stop if needed.")

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import threading
import os
import json
import logging

# === CONFIGURATION ===
class Config:
    PCR_URL = "https://niftyinvest.com/put-call-ratio/CRUDEOILM"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    UPDATE_INTERVAL = 60  # seconds
    
    # Google Sheets Configuration
    CREDENTIALS_FILE = "data/credentials.json"
    SHEET_NAME = "CrudeOil_PCR_Live_Data"
    WORKSHEET_NAME = "PCR_Data_Live"

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pcr_tracker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PCRTracker:
    def __init__(self):
        self.last_values = {"put_oi": None, "call_oi": None, "pcr": None}
        self.setup_google_sheets()
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            if not os.path.exists(Config.CREDENTIALS_FILE):
                raise FileNotFoundError(f"Credentials file not found: {Config.CREDENTIALS_FILE}")
                
            scope = ["https://spreadsheets.google.com/feeds", 
                    "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                Config.CREDENTIALS_FILE, scope)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open(Config.SHEET_NAME).worksheet(Config.WORKSHEET_NAME)
            logger.info("✅ Google Sheets connection established")
        except Exception as e:
            logger.error(f"❌ Google Sheets setup failed: {e}")
            raise

    def fetch_pcr_data(self):
        """Fetch PCR data from website with improved error handling"""
        try:
            response = requests.get(Config.PCR_URL, headers=Config.HEADERS, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            all_text = soup.get_text()
            
            # Extract data using multiple methods
            data = self.extract_pcr_data(all_text)
            data.update(self.extract_price_data(all_text))
            
            return self.process_data(data)
            
        except requests.RequestException as e:
            logger.error(f"❌ Network error: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Data extraction error: {e}")
            return None

    def extract_pcr_data(self, text):
        """Extract PCR related data"""
        data = {
            'put_oi': 0,
            'call_oi': 0, 
            'intraday_pcr': "0",
            'total_put_oi': 0,
            'total_call_oi': 0,
            'overall_pcr': "0"
        }
        
        # Multiple patterns for PCR
        pcr_patterns = [
            r'PCR[^\d]*([+-]?\d+\.\d+)',
            r'Put.*?Call.*?Ratio[^\d]*([+-]?\d+\.\d+)',
            r'Intraday PCR[^\d]*([+-]?\d+\.\d+)',
        ]
        
        for pattern in pcr_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['intraday_pcr'] = match.group(1)
                logger.info(f"✅ PCR found: {data['intraday_pcr']}")
                break
        
        # OI Patterns
        oi_patterns = [
            r'Put\s*OI\s*Chg[^\d]*([+-]?\d{1,3}(?:,\d{3})*)',
            r'Put.*?Change[^\d]*([+-]?\d{1,3}(?:,\d{3})*)',
        ]
        
        for pattern in oi_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['put_oi'] = int(match.group(1).replace(',', '').replace('+', ''))
                logger.info(f"✅ Put OI found: {data['put_oi']}")
                break
        
        return data

    def extract_price_data(self, text):
        """Extract price related data"""
        data = {
            'crudeoil_price': "0",
            'crudeoil_change': "0", 
            'crudeoil_percent_change': "0"
        }
        
        # Price extraction
        four_digit_numbers = re.findall(r'\b(\d{4})\b', text)
        valid_prices = [p for p in four_digit_numbers if 5000 <= int(p) <= 6000]
        
        if valid_prices:
            data['crudeoil_price'] = valid_prices[0]
            logger.info(f"✅ Price found: {data['crudeoil_price']}")
        
        # Change extraction
        change_match = re.search(r'([+-]?\d+\.\d+)\s*\(([+-]?\d+\.\d+)%\)', text)
        if change_match:
            data['crudeoil_change'] = change_match.group(1)
            data['crudeoil_percent_change'] = change_match.group(2)
            logger.info(f"✅ Change found: {data['crudeoil_change']}, {data['crudeoil_percent_change']}%")
        
        return data

    def process_data(self, data):
        """Process and format the extracted data"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
        
        # Calculate trends and changes
        intraday_pcr_float = float(data['intraday_pcr']) if data['intraday_pcr'] != "0" else 0.0
        trend = self.calculate_trend(intraday_pcr_float)
        
        # Calculate changes from last values
        put_change = data['put_oi'] - self.last_values["put_oi"] if self.last_values["put_oi"] else 0
        call_change = data['call_oi'] - self.last_values["call_oi"] if self.last_values["call_oi"] else 0
        pcr_change = intraday_pcr_float - self.last_values["pcr"] if self.last_values["pcr"] else 0
        
        # Update last values
        self.last_values.update({
            "put_oi": data['put_oi'],
            "call_oi": data['call_oi'], 
            "pcr": intraday_pcr_float
        })
        
        return {
            "Timestamp": timestamp,
            "Intraday Put Change OI": f"{data['put_oi']:,}",
            "Put Change": f"{put_change:,}",
            "Intraday Call Change OI": f"{data['call_oi']:,}",
            "Call Change": f"{call_change:,}",
            "Change %": self.calculate_change_percentage(data['put_oi'], data['call_oi']),
            "Intraday PCR": data['intraday_pcr'],
            "Pcr Change": f"{pcr_change:.2f}",
            "Trend": trend,
            "Observation": f"PCR {data['intraday_pcr']} indicates {trend.lower()}",
            "Total Put OI": f"{data['total_put_oi']:,}",
            "Total Call OI": f"{data['total_call_oi']:,}",
            "Overall PCR": data['overall_pcr'],
            "CrudeOil Price": data['crudeoil_price'],
            "CrudeOil Change": data['crudeoil_change'],
            "CrudeOil % Change": f"{data['crudeoil_percent_change']}%"
        }

    def calculate_trend(self, pcr_value):
        """Calculate market trend based on PCR"""
        if pcr_value <= 0.8:
            return "Bearish Trend"
        elif pcr_value >= 1.2:
            return "Bullish Trend"
        else:
            return "Neutral Trend"

    def calculate_change_percentage(self, put_oi, call_oi):
        """Calculate percentage difference between Put and Call OI"""
        abs_put = abs(put_oi)
        abs_call = abs(call_oi)
        
        if abs_call > abs_put and abs_put > 0:
            return f"Call higher by {((abs_call - abs_put) / abs_put * 100):.2f}%"
        elif abs_put > abs_call and abs_call > 0:
            return f"Put higher by {((abs_put - abs_call) / abs_call * 100):.2f}%"
        else:
            return "Both equal (0%)"

    def update_google_sheets(self, data):
        """Update Google Sheets with new data"""
        try:
            last_row = len(self.sheet.col_values(1)) + 1
            
            # Add headers if first row
            if last_row == 1:
                headers = list(data.keys())
                self.sheet.update(values=[headers], range_name="A1:P1")
                last_row += 1
            
            # Update data
            values = list(data.values())
            self.sheet.update(values=[values], range_name=f"A{last_row}:P{last_row}")
            logger.info(f"✅ Data updated at row {last_row}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Sheets update failed: {e}")
            return False

    def run_single_update(self):
        """Run a single update cycle"""
        data = self.fetch_pcr_data()
        if data:
            success = self.update_google_sheets(data)
            if success:
                logger.info("🔄 Update completed successfully")
            return success
        return False

    def start_continuous_updates(self):
        """Start continuous background updates"""
        def update_loop():
            while True:
                try:
                    self.run_single_update()
                    time.sleep(Config.UPDATE_INTERVAL)
                except Exception as e:
                    logger.error(f"❌ Update loop error: {e}")
                    time.sleep(30)  # Wait before retry
        
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()
        logger.info("✅ Continuous updates started")

# === MAIN EXECUTION ===
def main():
    """Main function with different run modes"""
    import sys
    
    tracker = PCRTracker()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "single":
            # Single update mode
            tracker.run_single_update()
        elif sys.argv[1] == "test":
            # Test mode
            data = tracker.fetch_pcr_data()
            if data:
                print("✅ Test successful - Data extracted:")
                for key, value in data.items():
                    print(f"   {key}: {value}")
            else:
                print("❌ Test failed")
    else:
        # Continuous mode (default)
        print("🚀 Starting CrudeOil PCR Tracker...")
        tracker.start_continuous_updates()
        
        # Keep main thread alive
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\n🛑 Tracker stopped by user")

if __name__ == "__main__":
    main()

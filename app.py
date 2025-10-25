@app.route('/find')
def find_data():
    try:
        print("🔍 FINDING WHERE DATA IS GOING...")
        
        # Google Sheets connection
        creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])
        gc = gspread.service_account_from_dict(creds_json)
        
        # List all available spreadsheets
        print("📋 All your Google Sheets:")
        for spreadsheet in gc.openall():
            print(f"   - {spreadsheet.title} (ID: {spreadsheet.id})")
        
        # Open our specific sheet
        sheet = gc.open("CrudeOil_PCR_Live_Data")
        print(f"✅ Opened: {sheet.title}")
        
        # List all worksheets
        print("📊 Worksheets in this sheet:")
        for worksheet in sheet.worksheets():
            print(f"   - {worksheet.title} (Rows: {worksheet.row_count})")
            
            # Show first 5 rows
            if worksheet.title == "PCR_Data_Live":
                data = worksheet.get_all_values()
                print(f"📝 First 5 rows of {worksheet.title}:")
                for i, row in enumerate(data[:5]):
                    print(f"   Row {i+1}: {row}")
        
        return "Check logs for sheet details"
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return f"Error: {e}"

# task9_push_to_gsheet.py – Single sheet export (full refined dataset)
# For Tableau / further analysis

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col, trim, year, month, hour, to_timestamp

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# ── Config ───────────────────────────────────────────────────
KEY_FILE = "/path/to/gsheet_key.json" # replace absolute path
SHEET_ID = "YOUR_SHEET_ID_HERE" # replace actual ID
SHEET_NAME    = "WhiteHouse" # replace with sheet name

# ── Google Sheets Auth ───────────────────────────────────────
def get_gsheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)
    client = gspread.authorize(creds)
    return client

# ── Helper: Write DataFrame to a specific worksheet tab ──────
def write_to_tab(spreadsheet, tab_name, df, description=""):
    print(f"    Writing to tab: '{tab_name}' ({description})...")
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows="5000", cols="20")

    rows = df.collect()
    headers = df.columns
    data = [headers] + [[str(v) if v is not None else "" for v in row] for row in rows]
    ws.update(data, "A1")
    print(f"        Rows written : {len(data) - 1}")
    time.sleep(2)

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task9 - Push to Google Sheets (single sheet)")

print("=" * 60)
print("  TASK 9: Push Whitehouse Visits (full dataset) to Google Sheets")
print("=" * 60)

# ── Load Hive Table ──────────────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")
wh_visits_hive = spark.table("whitehouse_visits_refined")
total_count    = wh_visits_hive.count()
print(f"    Total records: {total_count}")

# ── Connect to Google Sheets ─────────────────────────────────
print("\n[Step 2] Connecting to Google Sheets...")
client      = get_gsheet_client()
spreadsheet = client.open_by_key(SHEET_ID)
print(f"    Connected to : {SHEET_NAME} (ID: {SHEET_ID})")


# ── Apply cleaning and transformation ────────────────────────
# 1. Filter out rows where critical columns are empty (null or blank string)
cleaned_df = wh_visits_hive.filter(
    (col("time_of_arrival").isNotNull()) & (trim(col("time_of_arrival")) != "") &
    (col("info_comment").isNotNull()) & (trim(col("info_comment")) != "") &
    (col("lname").isNotNull()) & (trim(col("lname")) != "") &
    (col("fname").isNotNull()) & (trim(col("fname")) != "")
)

# 2. Drop any remaining rows that still have nulls in any column
cleaned_df = cleaned_df.dropna(how="any")

# 3. Add derived time columns for Tableau
final_df = (cleaned_df
            .withColumn("visit_year", 
                        year(to_timestamp(col("time_of_arrival"), "MM/dd/yyyy hh:mm")))
            .withColumn("visit_month", 
                        month(to_timestamp(col("time_of_arrival"), "MM/dd/yyyy hh:mm")))
            .withColumn("visit_hour", 
                        hour(to_timestamp(col("time_of_arrival"), "MM/dd/yyyy hh:mm")))
            .select("*"))   # keeps all original + new columns

print(f"Records after cleaning : {final_df.count()}")

# ── Write the final cleaned dataset to the single tab ──────
write_to_tab(spreadsheet, "WhiteHouse", final_df,
             "Cleaned POTUS visits with derived time fields")

print("\n" + "=" * 60)
print("  DATA SUCCESSFULLY WRITTEN TO GOOGLE SHEETS")
print("=" * 60)
print(f"\n  Sheet Name : {SHEET_NAME}")
print("  Tab Name   : Whitehouse Visits Data")
print("  Records    :", total_count)
print("\n  Open your Google Sheet – Tableau can connect directly.")

spark.stop()

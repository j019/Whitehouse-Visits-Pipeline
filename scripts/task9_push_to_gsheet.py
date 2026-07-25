# ============================================================
# task9_push_to_gsheet.py
# Task 9: Export cleaned Whitehouse Visits data to:
#         1. Google Sheets (single tab "WhiteHouse")
#         2. AWS S3  (auto-create bucket + auto-replace file)
# ============================================================

import sys
import subprocess

# ── Install required packages at runtime ─────────────────────
# sys.executable ensures the SAME python interpreter that runs
# this script is used to install — avoids wrong environment issues
print("[Setup] Installing required packages...")
subprocess.check_call([
    sys.executable, "-m", "pip", "install",
    "gspread==4.0.1",
    "oauth2client",
    "boto3",
    "--quiet"
])
print("[Setup] Packages installed successfully.")

sys.path.insert(0, "/path/to/your/scripts")            # ← set your scripts path

from spark_session_util import get_spark_session
from pyspark.sql.functions import col, trim, year, month, hour, to_timestamp

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
from botocore.exceptions import ClientError
import time
import csv
import io

# ── Config — Google Sheets ────────────────────────────────────
KEY_FILE   = "/path/to/your/gsheet_key.json"          # ← set your path
SHEET_ID   = "YOUR_GOOGLE_SHEET_ID_HERE"               # ← set your Sheet ID
SHEET_NAME = "WhiteHouse"
TAB_NAME   = "WhiteHouse"
BATCH_SIZE = 1000

# ── Config — AWS S3 ──────────────────────────────────────────
S3_BUCKET  = "YOUR_S3_BUCKET_NAME"                     # ← set your bucket
S3_KEY     = "whitehouse/potus_visits.csv"
AWS_REGION = "YOUR_AWS_REGION"                         # ← e.g. us-east-1

# ── Google Sheets Auth ───────────────────────────────────────
def get_gsheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE, scope)
    client = gspread.authorize(creds)
    return client

# ── Helper: Write to Google Sheets in batches ────────────────
def write_to_gsheet(spreadsheet, tab_name, data):
    print(f"\n[Google Sheets] Writing to tab '{tab_name}'...")
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print("    Existing tab cleared.")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows="30000", cols="15")
        print(f"    New tab created: '{tab_name}'")

    total_rows = len(data)
    ws.update(data[:BATCH_SIZE], "A1")
    print(f"    Rows 1 – {min(BATCH_SIZE, total_rows) - 1} written")

    for i in range(BATCH_SIZE, total_rows, BATCH_SIZE):
        batch     = data[i : i + BATCH_SIZE]
        start_row = i + 1
        ws.update(batch, f"A{start_row}")
        print(f"    Rows {start_row} – {start_row + len(batch) - 1} written")
        time.sleep(1)

    print(f"    Total rows written to Sheets : {total_rows - 1}")

# ── Helper: Check if S3 bucket exists ───────────────────────
def s3_bucket_exists(s3_client, bucket):
    try:
        s3_client.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ["404", "NoSuchBucket"]:
            return False
        raise

# ── Helper: Create S3 bucket if not exists ──────────────────
def s3_ensure_bucket(s3_client, bucket, region):
    if s3_bucket_exists(s3_client, bucket):
        print(f"    Bucket already exists: s3://{bucket}")
    else:
        print(f"    Bucket not found — creating s3://{bucket}...")
        if region == "us-east-1":
            # us-east-1 does NOT accept LocationConstraint
            s3_client.create_bucket(Bucket=bucket)
        else:
            s3_client.create_bucket(
                Bucket                    = bucket,
                CreateBucketConfiguration = {"LocationConstraint": region}
            )
        print(f"    Bucket created: s3://{bucket}")

# ── Helper: Check if file exists in S3 ──────────────────────
def s3_file_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

# ── Helper: Delete existing file from S3 ────────────────────
def s3_delete_file(s3_client, bucket, key):
    s3_client.delete_object(Bucket=bucket, Key=key)
    print(f"    Deleted : s3://{bucket}/{key}")

# ── Helper: Upload CSV data to S3 ───────────────────────────
def upload_to_s3(s3_client, data, bucket, key):
    print(f"\n[S3] Uploading to s3://{bucket}/{key}...")
    csv_buffer  = io.StringIO()
    writer      = csv.writer(csv_buffer)
    writer.writerows(data)
    csv_content = csv_buffer.getvalue()

    s3_client.put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = csv_content.encode("utf-8"),
        ContentType = "text/csv"
    )
    print(f"    Upload complete.")
    print(f"    Location : s3://{bucket}/{key}")
    print(f"    Rows     : {len(data) - 1}")

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task9 - Push to GSheets + S3")

print("=" * 60)
print("  TASK 9: Export Whitehouse Visits → Google Sheets + S3")
print("=" * 60)

# ── Step 1: Load Hive Table ──────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")
wh_visits_hive = spark.table("whitehouse_visits_refined")
total_count    = wh_visits_hive.count()
print(f"    Total records in Hive : {total_count}")

# ── Step 2: Clean Data ───────────────────────────────────────
print("\n[Step 2] Cleaning data...")
cleaned_df = wh_visits_hive.filter(
    col("lname").isNotNull()           & (trim(col("lname"))           != "") &
    col("fname").isNotNull()           & (trim(col("fname"))           != "") &
    col("time_of_arrival").isNotNull() & (trim(col("time_of_arrival")) != "") &
    col("info_comment").isNotNull()    & (trim(col("info_comment"))    != "")
)
cleaned_df  = cleaned_df.dropna(how="any")
clean_count = cleaned_df.count()
print(f"    Records after cleaning : {clean_count}")
print(f"    Records removed        : {total_count - clean_count}")

# ── Step 3: Add Derived Time Columns ─────────────────────────
print("\n[Step 3] Adding visit_year, visit_month, visit_hour...")
ts_col   = to_timestamp(col("time_of_arrival"), "MM/dd/yyyy hh:mm")
final_df = (cleaned_df
            .withColumn("visit_year",  year(ts_col))
            .withColumn("visit_month", month(ts_col))
            .withColumn("visit_hour",  hour(ts_col)))
final_count = final_df.count()
print(f"    Final export count : {final_count}")

# ── Step 4: Collect from Spark ───────────────────────────────
print("\n[Step 4] Collecting data from Spark...")
rows    = final_df.collect()
headers = list(final_df.columns)
data    = [headers] + [
    [str(v) if v is not None else "" for v in row]
    for row in rows
]
print(f"    Rows collected : {len(data) - 1}")

# ════════════════════════════════════════════════════════════
# DESTINATION 1 — Google Sheets
# ════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  DESTINATION 1: Google Sheets")
print("=" * 60)

client      = get_gsheet_client()
spreadsheet = client.open_by_key(SHEET_ID)
print(f"    Connected : {SHEET_NAME} (ID: {SHEET_ID})")
write_to_gsheet(spreadsheet, TAB_NAME, data)

# ════════════════════════════════════════════════════════════
# DESTINATION 2 — AWS S3
# Flow: ensure bucket → check file → delete if exists → upload
# ════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  DESTINATION 2: AWS S3")
print("=" * 60)

import warnings
warnings.filterwarnings("ignore")          # suppress boto3 Python 3.7 warning

s3_client = boto3.client("s3", region_name=AWS_REGION)

# 1. Ensure bucket exists — create if missing
print(f"\n[S3] Checking bucket s3://{S3_BUCKET}...")
s3_ensure_bucket(s3_client, S3_BUCKET, AWS_REGION)

# 2. Check if file already exists — delete if found
print(f"\n[S3] Checking file s3://{S3_BUCKET}/{S3_KEY}...")
if s3_file_exists(s3_client, S3_BUCKET, S3_KEY):
    print("    File EXISTS — deleting old version...")
    s3_delete_file(s3_client, S3_BUCKET, S3_KEY)
else:
    print("    File NOT found — fresh upload.")

# 3. Upload new file
upload_to_s3(s3_client, data, S3_BUCKET, S3_KEY)

# ── Done ─────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  EXPORT COMPLETE — BOTH DESTINATIONS")
print("=" * 60)
print(f"\n  Google Sheets : {SHEET_NAME} → tab '{TAB_NAME}'")
print(f"  AWS S3        : s3://{S3_BUCKET}/{S3_KEY}")
print(f"  Records       : {final_count}")
print(f"\n  Columns: lname, fname, time_of_arrival, appt_scheduled_time,")
print(f"           meeting_location, info_comment,")
print(f"           visit_year, visit_month, visit_hour")

spark.stop()

# ============================================================
# task3_load_filter_save.py
# Task 3: Load raw CSV → Filter POTUS → Select cols → Save Hive
# Place at: /home/talentum/test-jupyter/Project_Wh_visits/scriptstask3_load_filter_save.py
# ============================================================

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task3 - Load Filter Save")

print("=" * 60)
print("  TASK 3: Load → Filter POTUS → Select Cols → Save Hive")
print("=" * 60)

# ── Step 1: Load Raw CSV ─────────────────────────────────────
print("\n[Step 1] Loading raw CSV from local filesystem...")

filepath = "file:///home/talentum/test-jupyter/Project_Wh_visits/whitehouse_visits.txt"

wh_visitsdf = (spark.read
               .option("header", "false")
               .option("delimiter", ",")
               .csv(filepath))

raw_count = wh_visitsdf.count()
print(f"    Raw record count : {raw_count}")
print(f"    Total columns    : {len(wh_visitsdf.columns)}")

# ── Step 2: Filter POTUS Visits ──────────────────────────────
print("\n[Step 2] Filtering rows where _c19 == 'POTUS'...")

wh_visitsdf = wh_visitsdf.filter(wh_visitsdf["_c19"] == "POTUS")
potus_count = wh_visitsdf.count()

print(f"    POTUS record count : {potus_count}")
print(f"    Filtered out       : {raw_count - potus_count} records")

# ── Step 3: Select Columns [0:26] and Rename ─────────────────
print("\n[Step 3] Selecting columns [0:26] and renaming...")

selected_cols = wh_visitsdf.columns[0:26]
wh_visitsdf   = wh_visitsdf.select(*selected_cols)

# Column index → meaningful name mapping
mapping = {
    0 : "lname",
    1 : "fname",
    6 : "time_of_arrival",
    11: "appt_scheduled_time",
    21: "meeting_location",
    25: "info_comment"
}

selected_df = wh_visitsdf.select(
    [wh_visitsdf[f"_c{i}"].alias(name) for i, name in mapping.items()]
)

print("    Sample data (top 5 rows):")
selected_df.show(5)
print(f"    Final selected record count: {selected_df.count()}")

# ── Step 4: Save to Hive Table ───────────────────────────────
print("\n[Step 4] Writing to Hive table 'whitehouse_visits_refined'...")

selected_df.write.mode("overwrite").saveAsTable("whitehouse_visits_refined")

# Verify Hive write
wh_visits_hive = spark.table("whitehouse_visits_refined")
hive_count     = wh_visits_hive.count()

print(f"    Hive table record count : {hive_count}")
print("    Schema:")
wh_visits_hive.printSchema()
print("    Sample from Hive table:")
wh_visits_hive.show(5)

print("\n✅ Task 3 Complete — Hive table 'whitehouse_visits_refined' is ready.")
print("=" * 60)

spark.stop()

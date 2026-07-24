# ============================================================
# task8_null_no_purpose.py
# Task 8: Null Records & Visitors Without Purpose of Visit
# Place at: /home/talentum/test-jupyter/Project_Wh_visits/scripts/task8_null_no_purpose.py
# ============================================================

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task8 - Null and No Purpose Analysis")

print("=" * 60)
print("  TASK 8: Null Records & No Purpose of Visit")
print("=" * 60)

# ── Load Hive Table ──────────────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")

wh_visits_hive = spark.table("whitehouse_visits_refined")
total_count    = wh_visits_hive.count()
print(f"    Total record count: {total_count}")

# ── Analysis 7: Records with at Least One Null ───────────────
print("\n" + "-" * 60)
print("  ANALYSIS 7: Records with at Least One Null Value")
print("-" * 60)

clean_count    = wh_visits_hive.dropna().count()  # rows with NO nulls
null_row_count = total_count - clean_count         # rows with at least one null

print(f"    Total records                     : {total_count}")
print(f"    Records with NO nulls             : {clean_count}")
print(f"    Records with at least one null    : {null_row_count}")
print(f"    Null record percentage            : {(null_row_count / total_count) * 100:.4f}%")

# Show which columns have nulls and how many
print("\n    Null count per column:")
wh_visits_hive.select(
    [F_col for F_col in [
        col(c).isNull().cast("int").alias(c)
        for c in wh_visits_hive.columns
    ]]
).agg(
    *[__import__('pyspark.sql.functions', fromlist=['sum']).sum(c).alias(c)
      for c in wh_visits_hive.columns]
).show(truncate=False)

# ── Analysis 8: Visitors Without Purpose of Visit ────────────
print("\n" + "-" * 60)
print("  ANALYSIS 8: Visitors Without Purpose of Visit")
print("-" * 60)

no_purpose_df = wh_visits_hive.filter(
    col("info_comment").isNull() |    # NULL values
    (col("info_comment") == "")       # Empty string values
)

missing_count = no_purpose_df.count()

print(f"    Total visitors with no purpose entered : {missing_count}")
print(f"    Percentage of total                    : {(missing_count / total_count) * 100:.4f}%")

print("\n    Sample visitors with no purpose (top 10):")
no_purpose_df.select("fname", "lname", "time_of_arrival", "meeting_location") \
             .show(10, truncate=False)

print("\n✅ Task 8 Complete.")
print("=" * 60)

spark.stop()

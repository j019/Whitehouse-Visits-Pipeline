# ============================================================
# task4_first_last_visitor.py
# Task 4: Find first and last visitor by time_of_arrival
# Place at: /home/talentum/test-jupyter/Project_Wh_visits/scripts/task4_first_last_visitor.py
# ============================================================

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col, trim, unix_timestamp

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task4 - First Last Visitor")

print("=" * 60)
print("  TASK 4: First & Last Visitor Analysis")
print("=" * 60)

# ── Load Hive Table ──────────────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")

wh_visits_hive = spark.table("whitehouse_visits_refined")
print(f"    Record count: {wh_visits_hive.count()}")

# ── Analysis 1: First Visitor ────────────────────────────────
print("\n" + "-" * 60)
print("  ANALYSIS 1: First Visitor (Earliest time_of_arrival)")
print("-" * 60)

first_visitor = (wh_visits_hive
                 .filter(trim(col("time_of_arrival")) != "")   # exclude empty
                 .orderBy(
                     unix_timestamp(                            # sort as date not string
                         col("time_of_arrival"),
                         "MM/dd/yyyy hh:mm"
                     ).asc()
                 )
                 .limit(1))

print("The FIRST visitor in the dataset is:")
first_visitor.show(truncate=False)

# ── Analysis 2: Last Visitor ─────────────────────────────────
print("\n" + "-" * 60)
print("  ANALYSIS 2: Last Visitor (Latest time_of_arrival)")
print("-" * 60)

last_visitor = (wh_visits_hive
                .filter(trim(col("time_of_arrival")) != "")    # exclude empty
                .orderBy(
                    unix_timestamp(                             # sort as date not string
                        col("time_of_arrival"),
                        "MM/dd/yyyy hh:mm"
                    ).desc()
                )
                .limit(1))

print("The LAST visitor in the dataset is:")
last_visitor.show(truncate=False)

print("\n✅ Task 4 Complete.")
print("=" * 60)

spark.stop()

# ============================================================
# task7_top_visitors.py
# Task 7: Top 20 Most Frequent POTUS Visitors
# Place at: /home/talentum/test-jupyter/Project_Wh_visits/scripts/task7_top_visitors.py
# ============================================================

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col, concat, lit
import pyspark.sql.functions as F

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task7 - Top POTUS Visitors")

print("=" * 60)
print("  TASK 7: Top 20 Most Frequent POTUS Visitors")
print("=" * 60)

# ── Load Hive Table ──────────────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")

wh_visits_hive = spark.table("whitehouse_visits_refined")
print(f"    Record count: {wh_visits_hive.count()}")

# ── Analysis 6: Top 20 Visitors ──────────────────────────────
print("\n" + "-" * 60)
print("  ANALYSIS 6: Top 20 Most Frequent POTUS Visitors")
print("-" * 60)

top_20_potus = (wh_visits_hive
                .withColumn(
                    "visitor_full_name",
                    concat(
                        col("fname"),   # First name
                        lit(" "),       # Space separator
                        col("lname")    # Last name
                    )
                )
                .groupBy("visitor_full_name")
                .count()
                .orderBy(col("count").desc())
                .limit(20))

print("Top 20 individuals who visited the POTUS most:")
top_20_potus.show(20, truncate=False)

print("\n✅ Task 7 Complete.")
print("=" * 60)

spark.stop()

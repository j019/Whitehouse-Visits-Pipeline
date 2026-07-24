# ============================================================
# task5_comments.py
# Task 5: Top 10 Most & Least Frequent Comments
# Place at: /home/talentum/test-jupyter/Project_Wh_visits/scripts/task5_comments.py
# ============================================================

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task5 - Comments Analysis")

print("=" * 60)
print("  TASK 5: Top 10 Most & Least Frequent Comments")
print("=" * 60)

# ── Load Hive Table ──────────────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")

wh_visits_hive = spark.table("whitehouse_visits_refined")
print(f"    Record count: {wh_visits_hive.count()}")

# ── Analysis 3: Top 10 Most Common Comments ──────────────────
print("\n" + "-" * 60)
print("  ANALYSIS 3: Top 10 Most Common Comments")
print("-" * 60)

top_comments = (wh_visits_hive
                .filter(col("info_comment") != "")     # exclude empty strings
                .groupBy("info_comment")
                .count()
                .orderBy(col("count").desc())
                .limit(10))

print("Top 10 Most Common Comments:")
top_comments.show(truncate=False)

# ── Analysis 4: Top 10 Least Frequent Comments ───────────────
print("\n" + "-" * 60)
print("  ANALYSIS 4: Top 10 Least Frequent Comments")
print("-" * 60)

least_frequent_comments = (wh_visits_hive
                           .filter(
                               col("info_comment").isNotNull() &  # exclude nulls
                               (col("info_comment") != "")        # exclude empty strings
                           )
                           .groupBy("info_comment")
                           .count()
                           .orderBy(col("count").asc())
                           .limit(10))

print("10 Least Frequent Comments (Rare Entries):")
least_frequent_comments.show(truncate=False)

print("\n✅ Task 5 Complete.")
print("=" * 60)

spark.stop()

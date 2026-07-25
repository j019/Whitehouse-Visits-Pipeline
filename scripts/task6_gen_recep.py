# ============================================================
# task6_gen_recep.py
# Task 6: GEN RECEP Variations — Inconsistency Check
# Place at: /home/talentum/test-jupyter/Project_Wh_visits/scripts/task6_gen_recep.py
# ============================================================

import sys
sys.path.insert(0, "/home/talentum/test-jupyter/Project_Wh_visits/scripts")

from spark_session_util import get_spark_session
from pyspark.sql.functions import col
import pyspark.sql.functions as F

# ── Spark Session ────────────────────────────────────────────
spark = get_spark_session("Task6 - GEN RECEP Variations")

print("=" * 60)
print("  TASK 6: GEN RECEP Variations — Inconsistency Check")
print("=" * 60)

# ── Load Hive Table ──────────────────────────────────────────
print("\n[Step 1] Loading Hive table 'whitehouse_visits_refined'...")

wh_visits_hive = spark.table("whitehouse_visits_refined")
total_count    = wh_visits_hive.count()
print(f"    Total record count: {total_count}")

# ── Analysis 5: GEN RECEP Variations ─────────────────────────
print("\n" + "-" * 60)
print("  ANALYSIS 5: GEN RECEP Inconsistency Check")
print("-" * 60)

# 1. Standardise the column to uppercase for easier matching
df_clean = wh_visits_hive.withColumn(
    "info_comment_upper",
    F.upper(col("info_comment"))
)

# 2. Define regex pattern for GEN RECEP variations
pattern = "GEN.*RECEP|GENERAL.*RECEPTION"

# 3. Filter for matching records
gen_recep_df = df_clean.filter(col("info_comment_upper").rlike(pattern))

# 4. Count matching records
match_count = gen_recep_df.count()

# 5. Calculate percentage
percentage = (match_count / total_count) * 100

print(f"\n    Total records in dataset              : {total_count}")
print(f"    Records matching 'GEN RECEP' variations: {match_count}")
print(f"    Percentage of total                   : {percentage:.4f}%")

# 6. Drop helper column and show distinct variations
print("\n    Distinct GEN RECEP variations (up to 30):")
gen_recep_df.drop("info_comment_upper") \
            .select("info_comment") \
            .distinct() \
            .orderBy("info_comment") \
            .show(30, truncate=False)

print("\n✅ Task 6 Complete.")
print("=" * 60)

spark.stop()

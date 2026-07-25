# ============================================================
# spark_session_util.py
# Shared utility — imported by all task scripts
# Place at: /home/talentum/scripts/spark_session_util.py
# NOTE: Environment setup is handled by spark-submit itself.
#       This file only creates and returns the SparkSession.
# ============================================================

from pyspark.sql import SparkSession

def get_spark_session(app_name="Whitehouse Visits"):
    """
    Creates and returns a Spark session with Hive support enabled.
    Environment variables are handled externally by spark-submit.
    """

    spark = (SparkSession.builder
             .appName(app_name)
             .enableHiveSupport()
             .getOrCreate())

    # Suppress logs — only show ERROR level
    spark.sparkContext.setLogLevel("ERROR")

    return spark
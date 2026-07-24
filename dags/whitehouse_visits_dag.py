# whitehouse_visits_dag.py
# Place this file in: $AIRFLOW_HOME/dags/
# POTUS = President Of The United States
# Dataset = Whitehouse Visits

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ── Boilerplate: Scripts directory path ──────────────────────
SCRIPTS_DIR = "/path/to/your/scripts"  #PYTHON37 = "/path/to/python3.7"

# ── Default Arguments ────────────────────────────────────────
default_args = {
    "owner"           : "airflow",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────────
dag = DAG(
    dag_id            = "whitehouse_visits_pipeline",
    default_args      = default_args,
    description       = "Whitehouse Visits ETL + Analysis Pipeline",
    schedule_interval = "@daily",
    start_date        = days_ago(1),
    catchup           = False,
    tags              = ["whitehouse", "potus", "spark", "hdfs"],
)

# ============================================================
# TASK 1 — Check & Create HDFS Directory
# ============================================================
check_create_hdfs_dir = BashOperator(
    task_id      = "check_and_create_hdfs_dir",
    bash_command = """
        if ! hdfs dfs -test -d whitehouse; then
            echo "Creating HDFS directory..."
            hdfs dfs -mkdir whitehouse
        else
            echo "HDFS directory already exists. Skipping."
        fi
    """,
    dag = dag,
)

# ============================================================
# TASK 2 — Upload File to HDFS (only if not exists)
# ============================================================
upload_file_to_hdfs = BashOperator(
    task_id      = "upload_file_to_hdfs",
    bash_command = """
        if ! hdfs dfs -test -f whitehouse/visits.txt; then
            echo "Uploading file to HDFS..."
            hdfs dfs -put ~/shared/whitehouse_visits.txt whitehouse/visits.txt
            echo "Upload successful."
        else
            echo "File already exists in HDFS. Skipping upload."
        fi
    """,
    dag = dag,
)

# ============================================================
# TASK 3 — Load Whitehouse Visits, Filter President Visits,
#           Select Columns, Save to Hive
# ============================================================
load_filter_save = BashOperator(
    task_id      = "load_filter_president_visits_save_hive",
    bash_command = f"""
        source ~/unset-jupyter.sh
        spark-submit {SCRIPTS_DIR}/task3_load_filter_save.py 2>/dev/null
    """,
    dag = dag,
)

# ============================================================
# TASK 4 — First & Last Visitor to the White House
# ============================================================
analysis_first_last = BashOperator(
    task_id      = "analysis_first_last_visitor",
    bash_command = f"""
        source ~/unset-jupyter.sh
        spark-submit {SCRIPTS_DIR}/task4_first_last_visitor.py 2>/dev/null
    """,
    dag = dag,
)

# ============================================================
# TASK 5 — Top 10 Most & Least Common Visit Comments
# ============================================================
analysis_comments = BashOperator(
    task_id      = "analysis_top_least_comments",
    bash_command = f"""
        source ~/unset-jupyter.sh
        spark-submit {SCRIPTS_DIR}/task5_comments.py 2>/dev/null
    """,
    dag = dag,
)

# ============================================================
# TASK 6 — GEN RECEP Inconsistency Analysis
# ============================================================
analysis_gen_recep = BashOperator(
    task_id      = "analysis_gen_recep_variations",
    bash_command = f"""
        source ~/unset-jupyter.sh
        spark-submit {SCRIPTS_DIR}/task6_gen_recep.py 2>/dev/null
    """,
    dag = dag,
)

# ============================================================
# TASK 7 — Top 20 Most Frequent President (POTUS) Visitors
# ============================================================
analysis_top_visitors = BashOperator(
    task_id      = "analysis_top_20_president_visitors",
    bash_command = f"""
        source ~/unset-jupyter.sh
        spark-submit {SCRIPTS_DIR}/task7_top_visitors.py 2>/dev/null
    """,
    dag = dag,
)

# ============================================================
# TASK 8 — Null Records & Visitors with No Purpose Entered
# ============================================================
analysis_null_no_purpose = BashOperator(
    task_id      = "analysis_null_no_purpose",
    bash_command = f"""
        source ~/unset-jupyter.sh
        spark-submit {SCRIPTS_DIR}/task8_null_no_purpose.py 2>/dev/null
    """,
    dag = dag,
)

# ============================================================
# TASK 9 — Push All Analysis Results to Google Sheets
# Pushes 9 separate tabs to Google Sheets
# Each tab = one analysis result
# ============================================================
export_to_gsheet = BashOperator(
    task_id      = "export_all_results_to_google_sheets",
    bash_command = f"""
	export PYSPARK_PYTHON=/home/talentum/miniconda3/envs/airflow-tutorial/bin/python
        spark-submit {SCRIPTS_DIR}/task9_push_to_gsheet.py
    """,
    dag = dag,
)

# ============================================================
# TASK DEPENDENCIES — Execution Order
# ============================================================

# Step 1 → Step 2 → Step 3 (sequential — must be in order)
check_create_hdfs_dir >> upload_file_to_hdfs >> load_filter_save

# Step 3 → Step 4 & 5 (parallel — both only need the Hive table)
load_filter_save >> [analysis_first_last, analysis_comments]

# Step 4 & 5 → Step 6 & 7 (parallel — independent analyses)
analysis_first_last >> [analysis_gen_recep, analysis_top_visitors]
analysis_comments   >> [analysis_gen_recep, analysis_top_visitors]

# Step 6 & 7 → Step 8 (final analysis)
[analysis_gen_recep, analysis_top_visitors] >> analysis_null_no_purpose

# Step 8 → Step 9 (push to Google Sheets after all analyses done)
analysis_null_no_purpose >> export_to_gsheet

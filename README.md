# 🏛️ Whitehouse Visits Pipeline

> End-to-end Big Data pipeline: Spark → Hive → Airflow → Google Sheets + AWS S3 → Tableau

An end-to-end data engineering pipeline that extracts, transforms, and analyses publicly available White House visitor records — focusing exclusively on visits to the **President of the United States (POTUS)**.

---

## 📌 What This Project Does

The White House receives thousands of visitors every day, but only a few meet the President. This pipeline:

- Filters **21,819 POTUS-only visits** from 400,000+ raw records
- Cleans and stores data in **Apache Hive** (ORC format)
- Runs **8 analytical queries** on the curated dataset
- Exports cleaned results to **Google Sheets** and **AWS S3** simultaneously
- Orchestrates the entire workflow daily with **Apache Airflow**
- Connects to **Tableau** for interactive dashboards

---

## 🏗️ Architecture

```
Raw Data (whitehouse_visits.txt)
        │
        ▼
  [HDFS Storage]
  whitehouse/visits.txt
        │
        ▼
  [Task 3 — PySpark ETL]
  Load CSV → Filter _c19==POTUS → Select 6 cols → Save Hive (ORC)
        │
        ├──────────────────────────┐
        ▼                          ▼
[Task 4]                      [Task 5]           ← PARALLEL
First & Last Visitor          Top/Least Comments
        │                          │
        └──────────┬───────────────┘
                   │
        ┌──────────┴───────────────┐
        ▼                          ▼
[Task 6]                      [Task 7]           ← PARALLEL
GEN RECEP Variations          Top 20 Visitors
        │                          │
        └──────────┬───────────────┘
                   ▼
              [Task 8]
      Null & No Purpose Analysis
                   │
                   ▼
              [Task 9]
     Clean + Add Derived Columns
                   │
        ┌──────────┴───────────────┐
        ▼                          ▼
 [Google Sheets]             [AWS S3]
  WhiteHouse tab           potus_visits.csv
        │
        ▼
    [Tableau]
   Dashboards
```

---

## 📁 Project Structure

```
Whitehouse-Visits-Pipeline/
│
├── dags/
│   └── whitehouse_visits_dag.py          # Airflow DAG — 9 tasks, daily schedule
│
├── scripts/
│   ├── spark_session_util.py             # Shared SparkSession utility
│   ├── task3_load_filter_save.py         # Load CSV → Filter POTUS → Save Hive
│   ├── task4_first_last_visitor.py       # First & last visitor by timestamp
│   ├── task5_comments.py                 # Top 10 most & least common comments
│   ├── task6_gen_recep.py                # GEN RECEP inconsistency analysis
│   ├── task7_top_visitors.py             # Top 20 most frequent POTUS visitors
│   ├── task8_null_no_purpose.py          # Null records & no purpose analysis
│   └── task9_push_to_gsheet.py          # Clean → Google Sheets + AWS S3
│
├── .gitignore
└── README.md
```

---

## 🔧 Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Apache Spark (PySpark) | 2.4.5 | Distributed data processing |
| Apache Hive | 2.x | Data warehouse (ORC format) |
| HDFS | 2.x | Distributed file system |
| Apache Airflow | 1.x | Pipeline orchestration |
| Python | 3.7 | Scripts and DAG |
| Bash | Shell | HDFS operations |
| gspread | 4.0.1 | Google Sheets API client |
| boto3 | Latest | AWS S3 client |
| Google Sheets | — | Tableau-ready data output |
| AWS S3 | — | Cloud data storage |
| Tableau | — | Dashboard and visualisation |

---

## 📊 Dataset

**Source:** Publicly available White House visitor records
**Raw records:** 400,000+
**POTUS records:** 21,819
**Cleaned & exported:** 1,133 (with complete time data)

**Columns exported:**

| Column | Description |
|---|---|
| `lname` | Visitor last name |
| `fname` | Visitor first name |
| `time_of_arrival` | Arrival timestamp (MM/dd/yyyy hh:mm) |
| `appt_scheduled_time` | Scheduled appointment time |
| `meeting_location` | Meeting location (WH / OEOB) |
| `info_comment` | Purpose of visit |
| `visit_year` | Extracted year (derived) |
| `visit_month` | Extracted month (derived) |
| `visit_hour` | Extracted hour (derived) |

---

## ⚙️ Configuration

Before running, update these values in the files:

### `scripts/task9_push_to_gsheet.py`
```python
KEY_FILE   = "/path/to/your/gsheet_key.json"   # Google service account key
SHEET_ID   = "YOUR_GOOGLE_SHEET_ID_HERE"        # From your Google Sheet URL
S3_BUCKET  = "YOUR_S3_BUCKET_NAME"              # Your AWS S3 bucket
AWS_REGION = "YOUR_AWS_REGION"                  # e.g. us-east-1
```

### `dags/whitehouse_visits_dag.py`
```python
SCRIPTS_DIR = "/path/to/your/scripts"           # Absolute path to scripts/
```

---

## 🚀 Setup & Installation

### 1. Clone the repo

```bash
git clone https://github.com/j019/Whitehouse-Visits-Pipeline.git
cd Whitehouse-Visits-Pipeline
```

### 2. Install Python dependencies

```bash
# Packages are auto-installed at runtime by task9 via subprocess
# But you can also install manually:
/path/to/python3.7 -m pip install gspread==4.0.1 oauth2client boto3
```

### 3. Google Sheets setup (one-time)

```
1. Go to https://console.cloud.google.com
2. Create project → Enable Google Sheets API + Google Drive API
3. Create Service Account → Download JSON key file
4. Save as: gsheet_key.json  (DO NOT commit this file)
5. Share your Google Sheet with the service account email
6. Copy your Sheet ID from the URL:
   https://docs.google.com/spreadsheets/d/SHEET_ID_IS_HERE/edit
```

### 4. AWS S3 setup (one-time)

```bash
# Install AWS CLI
sudo apt install awscli -y

# Configure credentials
aws configure
# Enter: Access Key, Secret Key, Region, output format

# Create S3 bucket
aws s3 mb s3://your-bucket-name --region us-east-1
```

### 5. Start HDFS and Airflow

```bash
# Start HDFS
start-dfs.sh && start-yarn.sh

# Start Airflow
airflow webserver -p 8080 &
airflow scheduler &
```

### 6. Deploy the DAG

```bash
cp dags/whitehouse_visits_dag.py $AIRFLOW_HOME/dags/
cp scripts/*.py /path/to/your/scripts/
```

### 7. Trigger the pipeline

```
1. Open http://localhost:8080
2. Find: whitehouse_visits_pipeline
3. Toggle ON → Click ▶ Trigger
```

---

## 📈 Analyses Performed

| # | Analysis | Method |
|---|---|---|
| 1 | First visitor to the White House | `unix_timestamp` sort ASC |
| 2 | Last visitor to the White House | `unix_timestamp` sort DESC |
| 3 | Top 10 most common visit purposes | `groupBy → count → DESC` |
| 4 | Top 10 least frequent visit purposes | `groupBy → count → ASC` |
| 5 | GEN RECEP data inconsistency | `rlike` regex + percentage |
| 6 | Top 20 most frequent POTUS visitors | `concat(fname, lname) → count` |
| 7 | Records with at least one null | `total - dropna().count()` |
| 8 | Visitors with no purpose entered | `isNull() OR == ""` filter |

---

## 📤 Task 9 — Dual Export

Task 9 exports cleaned data to **two destinations simultaneously**:

### Google Sheets
- Tab: `WhiteHouse`
- Writes in batches of 1,000 rows
- Auto-clears and rewrites on every run

### AWS S3
- Path: `s3://your-bucket/whitehouse/potus_visits.csv`
- **Auto-replace logic:** checks if file exists → deletes old → uploads new
- Packages installed at runtime via `subprocess`

---

## 📊 Tableau Dashboard Ideas

| Chart | Fields | Insight |
|---|---|---|
| Line chart | `visit_month`, COUNT | Monthly visit trends |
| Heatmap | `visit_hour` × `visit_month` | Peak visit times |
| Horizontal bar | `fname+lname`, COUNT | Top 20 visitors |
| Donut | `meeting_location` | WH 70% vs OEOB 30% |
| Histogram | arrival gap (minutes) | Early vs late arrivals |
| Bar chart | `info_comment`, COUNT | Top 10 visit purposes |
| KPI cards | Total, Peak month, Avg gap | Dashboard header |

---

## 🔒 Security — What NOT to Commit

| File | Reason |
|---|---|
| `gsheet_key.json` | Google service account credentials |
| `whitehouse_visits.txt` | Large raw data file |
| `*.json` | All credential files |
| `logs/` | Airflow task logs |
| `__pycache__/` | Python cache |
| `.env` | Environment variables |
| `csv_exports/` | Generated output files |

All blocked by `.gitignore`.

---

## ⚠️ Common Issues & Fixes

| Error | Cause | Fix |
|---|---|---|
| `POTUS filter returns 0 rows` | Datetime spaces shifting columns | Set `delimiter=","` explicitly |
| `Wrong date sort order` | String sort on dates | Use `unix_timestamp()` before sorting |
| `ModuleNotFoundError: gspread` | Wrong Python environment | Use `sys.executable` with subprocess |
| `NoSuchBucket` | Placeholder bucket name in code | Set actual `S3_BUCKET` value |
| `HDFS connection refused` | Hadoop not running | Run `start-dfs.sh && start-yarn.sh` |
| `unset-jupyter.sh not found` | Wrong path in DAG | `find /home -name unset-jupyter.sh` |
| `SyntaxError: pip install` | Shell command in Python file | Use `subprocess.check_call` instead |

---

## 📄 License

Educational purposes. All data is publicly available White House visitor records.

---

## 👨‍💻 Author

**Jatin Valecha** — Data Engineer
🔗 [GitHub](https://github.com/j019) | 🌐 [Portfolio](https://jatinvalecha.netlify.app)

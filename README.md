# MTS Lab ETL Script

This is a Python script that automates the process of reading `.dat` files from test machines, extracting the data and metadata, and uploading it into a PostgreSQL database (and also into a TimescaleDB version of the same database).

It's designed for a specific lab setup where multiple machines export data into folders, and each data file contains a mix of test results and machine metadata.

---

## How It Works

- Watches specific folders for modified `.dat` files
- Keeps track of the last line read in each file, so it only processes new data
- Figures out which database table to write to based on the file headers
- Logs everything to both a text file and a database table
- Creates tables and sequences automatically if they don’t exist yet
- Also copies the data to a TimescaleDB version of the same table

---

## Setup

1. Create a `.env` file with your database login and folder paths:


2. Install dependencies:
    pip install -r requirements.txt

3. Run the script:
    python etl_script.py

---

## Notes

- This script is not meant to be generalized or reused outside of its current setup.
- All logs go to a `system_data/` folder as `.txt` files, and also into two tables: `etl_log` and `error_log`.
- Table names (`table_top`, `rotary`, `mts_810`) are auto-detected based on headers in the file.
- The TimescaleDB database is assumed to be named `mts771_ts`.

---

Built for internal use in a lab setting.

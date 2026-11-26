<<<<<<< HEAD
# Hospital Data Engineering — PySpark + PostgreSQL Medallion Pipeline

This project is built for the uploaded dataset (patients: 1,000 rows; services_weekly: 208; staff: 110; staff_schedule: 6,552). It reads the existing PostgreSQL `bronze` schema, cleans/transforms data with PySpark, and writes Silver and Gold tables back to PostgreSQL.

## Architecture

`CSV files (data-lake/landing) -> PostgreSQL bronze -> PySpark -> PostgreSQL silver -> PostgreSQL gold`

The four source table names and columns are:
- `bronze.patients`: patient_id, name, age, arrival_date, departure_date, service, satisfaction
- `bronze.services_weekly`: week, month, service, available_beds, patients_request, patients_admitted, patients_refused, patient_satisfaction, staff_morale, event
- `bronze.staff`: staff_id, staff_name, role, service
- `bronze.staff_schedule`: week, staff_id, staff_name, role, service, present

## Requirements (Windows)

1. PostgreSQL 18 and pgAdmin are already installed/configured in this project.
2. Install **JDK 17** and set `JAVA_HOME` to its installation directory. Verify `java -version`.
3. Install Python 3.10 or 3.11 (recommended). Verify `python --version`.
4. Install VS Code and its Python extension.

## Setup in VS Code

Open this project folder (`hospital-pyspark-pipeline`) in VS Code. Open Terminal (PowerShell) and run:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

Open `.env` and set the actual PostgreSQL password for the `postgres` user. Do not share or commit `.env`. If your PostgreSQL user is not `postgres`, update `PG_USER` too.

Set `DATA_LAKE_LANDING` to your real landing path. Current expected path is `D:/hospital-data-engineering/data-lake/landing`.

## Prepare PostgreSQL

In pgAdmin, select database `hospital_dw`, open Query Tool and run `sql/00_create_schemas.sql` (or execute):

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

The Bronze tables must already contain the four CSV datasets. This project intentionally does not reload/overwrite Bronze; it uses your successfully loaded Bronze tables as its source.

## Run pipeline

From the project root, with `.venv` activated:

```powershell
python src/pipeline.py
```

The first run may take longer because Spark downloads the PostgreSQL JDBC driver package. Ensure internet access is available for this first run and PostgreSQL is running on localhost:5432.

## Output tables

**Silver**
- `silver.silver_patients` — typed dates/numbers, valid ages (0–120), satisfaction (0–100), departure >= arrival, unique patient_id, length_of_stay_days.
- `silver.silver_services_weekly` — typed numeric metrics, valid week/month and score ranges, unique week+month+service, admission_rate_pct.
- `silver.silver_staff` — trimmed/non-empty fields and unique staff_id.
- `silver.silver_staff_schedule` — valid week and present flag (0/1), unique week+staff_id.

**Gold**
- `gold.service_performance` — service-level requests, admissions, refusals, satisfaction, morale and admission rate.
- `gold.patient_service_summary` — patient count, average age/satisfaction and length of stay by service.
- `gold.staff_attendance_summary` — scheduled/present shifts and attendance rate by service and role.

## Validate output

Run in pgAdmin Query Tool:

```sql
SELECT 'bronze.patients' AS table_name, COUNT(*) FROM bronze.patients
UNION ALL SELECT 'silver.silver_patients', COUNT(*) FROM silver.silver_patients
UNION ALL SELECT 'silver.silver_services_weekly', COUNT(*) FROM silver.silver_services_weekly
UNION ALL SELECT 'silver.silver_staff', COUNT(*) FROM silver.silver_staff
UNION ALL SELECT 'silver.silver_staff_schedule', COUNT(*) FROM silver.silver_staff_schedule;

SELECT * FROM gold.service_performance ORDER BY service;
SELECT * FROM gold.patient_service_summary ORDER BY service;
SELECT * FROM gold.staff_attendance_summary ORDER BY service, role;
```

## Important behavior

- The pipeline reads Bronze and writes Silver/Gold. It does not alter Bronze.
- Silver and Gold target tables are rebuilt on each run (`overwrite`), making runs repeatable. Do not manually add data to these output tables because the next run replaces them.
- Invalid records are currently filtered out by the explicit validation rules in `src/pipeline.py`. For production, extend the project to persist rejected records to quarantine tables and add logging/alerting.
- The `DATA_LAKE_LANDING` variable documents the local raw-file location; this first pipeline version uses PostgreSQL Bronze as the Spark input. Landing-to-Bronze automated ingestion can be added as a separate stage after validating your local environment.
=======
# HospitalBedsManagement
>>>>>>> 00d4795999b6674697b1200f5bcbbd2e7b6efc3c

"""Hospital Medallion pipeline: PostgreSQL Bronze -> PySpark Silver -> Gold."""
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

HOST = os.getenv("PG_HOST", "localhost")
PORT = os.getenv("PG_PORT", "5432")
DB = os.getenv("PG_DATABASE", "hospital_dw")
USER = os.getenv("PG_USER", "postgres")
PASSWORD = os.getenv("PG_PASSWORD", "")
JDBC_PACKAGE = os.getenv("POSTGRES_JDBC_PACKAGE", "org.postgresql:postgresql:42.7.7")
JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"

# Explicit source schemas mirror the actual CSV headers and PostgreSQL Bronze tables.
SCHEMAS = {
    "patients": StructType([
        StructField("patient_id", StringType()), StructField("name", StringType()),
        StructField("age", StringType()), StructField("arrival_date", StringType()),
        StructField("departure_date", StringType()), StructField("service", StringType()),
        StructField("satisfaction", StringType())]),
    "services_weekly": StructType([
        StructField("week", StringType()), StructField("month", StringType()),
        StructField("service", StringType()), StructField("available_beds", StringType()),
        StructField("patients_request", StringType()), StructField("patients_admitted", StringType()),
        StructField("patients_refused", StringType()), StructField("patient_satisfaction", StringType()),
        StructField("staff_morale", StringType()), StructField("event", StringType())]),
    "staff": StructType([
        StructField("staff_id", StringType()), StructField("staff_name", StringType()),
        StructField("role", StringType()), StructField("service", StringType())]),
    "staff_schedule": StructType([
        StructField("week", StringType()), StructField("staff_id", StringType()),
        StructField("staff_name", StringType()), StructField("role", StringType()),
        StructField("service", StringType()), StructField("present", StringType())])
}


def spark_session():
    return (SparkSession.builder.appName("Hospital-Medallion-Pipeline")
            .config("spark.jars.packages", JDBC_PACKAGE)
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())


def jdbc_read(spark, table):
    return (spark.read.format("jdbc").option("url", JDBC_URL)
            .option("dbtable", f"bronze.{table}")
            .option("user", USER).option("password", PASSWORD)
            .option("driver", "org.postgresql.Driver").load())


def jdbc_write(df, table):
    # overwrite makes this pipeline repeatable; Spark recreates the target table.
    (df.write.format("jdbc").option("url", JDBC_URL)
     .option("dbtable", f"silver.{table}" if table.startswith("silver_") else f"gold.{table}")
     .option("user", USER).option("password", PASSWORD)
     .option("driver", "org.postgresql.Driver").mode("overwrite").save())


def clean_text(df, columns):
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
        df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))
    return df


def deduplicate(df, keys):
    return df.dropDuplicates(keys)


def main():
    if not PASSWORD:
        raise ValueError("PG_PASSWORD missing. Copy .env.example to .env and set your PostgreSQL password.")
    spark = spark_session()
    try:
        # Read existing Bronze tables loaded from the landing CSVs.
        p = clean_text(jdbc_read(spark, "patients"), ["patient_id", "name", "service"])
        p = (p.withColumn("age", F.col("age").cast("int"))
             .withColumn("arrival_date", F.to_date("arrival_date", "yyyy-MM-dd"))
             .withColumn("departure_date", F.to_date("departure_date", "yyyy-MM-dd"))
             .withColumn("satisfaction", F.col("satisfaction").cast("int")))
        p = p.filter(F.col("patient_id").isNotNull() & F.col("name").isNotNull()
                     & F.col("arrival_date").isNotNull() & F.col("departure_date").isNotNull()
                     & (F.col("age").between(0, 120)) & (F.col("satisfaction").between(0, 100))
                     & (F.col("departure_date") >= F.col("arrival_date")))
        p = deduplicate(p, ["patient_id"])
        p = p.withColumn("length_of_stay_days", F.datediff("departure_date", "arrival_date"))
        jdbc_write(p, "silver_patients")

        sw = clean_text(jdbc_read(spark, "services_weekly"), ["service", "event"])
        for c in ["week", "month", "available_beds", "patients_request", "patients_admitted",
                  "patients_refused", "patient_satisfaction", "staff_morale"]:
            sw = sw.withColumn(c, F.col(c).cast("int"))
        sw = sw.filter(F.col("week").between(1, 53) & F.col("month").between(1, 12)
                       & F.col("service").isNotNull() & (F.col("available_beds") >= 0)
                       & (F.col("patients_request") >= 0) & (F.col("patients_admitted") >= 0)
                       & (F.col("patients_refused") >= 0)
                       & F.col("patient_satisfaction").between(0, 100)
                       & F.col("staff_morale").between(0, 100))
        sw = deduplicate(sw, ["week", "month", "service"])
        sw = sw.withColumn("admission_rate_pct", F.round(F.when(F.col("patients_request") > 0,
                    F.col("patients_admitted") / F.col("patients_request") * 100), 2))
        jdbc_write(sw, "silver_services_weekly")

        st = clean_text(jdbc_read(spark, "staff"), ["staff_id", "staff_name", "role", "service"])
        st = st.filter(F.col("staff_id").isNotNull() & F.col("staff_name").isNotNull()
                       & F.col("role").isNotNull() & F.col("service").isNotNull())
        st = deduplicate(st, ["staff_id"])
        jdbc_write(st, "silver_staff")

        ss = clean_text(jdbc_read(spark, "staff_schedule"),
                        ["staff_id", "staff_name", "role", "service"])
        ss = ss.withColumn("week", F.col("week").cast("int")).withColumn("present", F.col("present").cast("int"))
        ss = ss.filter(F.col("week").between(1, 53) & F.col("staff_id").isNotNull()
                       & F.col("present").isin(0, 1))
        ss = deduplicate(ss, ["week", "staff_id"])
        jdbc_write(ss, "silver_staff_schedule")

        # Gold: service performance, patient service outcomes, staff attendance.
        gold_service = sw.groupBy("service").agg(
            F.sum("patients_request").alias("total_requests"),
            F.sum("patients_admitted").alias("total_admitted"),
            F.sum("patients_refused").alias("total_refused"),
            F.avg("patient_satisfaction").cast("decimal(10,2)").alias("avg_patient_satisfaction"),
            F.avg("staff_morale").cast("decimal(10,2)").alias("avg_staff_morale"),
            F.round(F.sum("patients_admitted") / F.when(F.sum("patients_request") > 0,
                     F.sum("patients_request")) * 100, 2).alias("admission_rate_pct"))
        jdbc_write(gold_service, "service_performance")

        gold_patient = p.groupBy("service").agg(
            F.count("*").alias("patient_count"),
            F.avg("age").cast("decimal(10,2)").alias("avg_patient_age"),
            F.avg("satisfaction").cast("decimal(10,2)").alias("avg_satisfaction"),
            F.avg("length_of_stay_days").cast("decimal(10,2)").alias("avg_length_of_stay_days"))
        jdbc_write(gold_patient, "patient_service_summary")

        gold_staff = ss.groupBy("service", "role").agg(
            F.count("*").alias("scheduled_shifts"),
            F.sum(F.when(F.col("present") == 1, 1).otherwise(0)).alias("present_shifts"),
            F.round(F.avg("present") * 100, 2).alias("attendance_rate_pct"))
        jdbc_write(gold_staff, "staff_attendance_summary")

        print("\nPIPELINE COMPLETED")
        for name, df in [("silver_patients", p), ("silver_services_weekly", sw),
                         ("silver_staff", st), ("silver_staff_schedule", ss),
                         ("gold_service_performance", gold_service),
                         ("gold_patient_service_summary", gold_patient),
                         ("gold_staff_attendance_summary", gold_staff)]:
            print(f"{name}: {df.count()} rows")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Nhận tham số truyền vào từ dòng lệnh (Ví dụ: python job_analytics.py 2026 09 06)
if len(sys.argv) >= 4:
    target_year = sys.argv[1]
    target_month = sys.argv[2]
    target_day = sys.argv[3]
else:
    # Giá trị mặc định nếu không truyền tham số
    target_year = "2026"
    target_month = "09"
    target_day = "06"

print(f"Running analytics for Date: {target_year}-{target_month}-{target_day}")

# ============================================================
# 1. Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("hagent_callrec_analytics_pipeline")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.convertTimestampNano", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 2. Schema
# ============================================================

call_rec_schema = """
    callid long,
    anslogin string,
    duration long,
    year string,
    month string,
    day string
"""

hagent_schema = """
    agentname string,
    logid string,
    split integer,
    acdcalls integer,
    acdtime integer,
    year string,
    month string,
    day string
"""

# ============================================================
# 3. Read CALL_REC
# ============================================================

call_rec = (
    spark.read
    .schema(call_rec_schema)
    .parquet("s3a://datalake-raw/call_rec/")
    .filter(
        (F.col("year") == target_year) &
        (F.col("month") == target_month) &
        (F.col("day") == target_day)
    )
)

# ============================================================
# 4. Read HAGENT
# ============================================================

hagent = (
    spark.read
    .schema(hagent_schema)
    .parquet("s3a://datalake-raw/hagent/")
    .filter(
        (F.col("year") == target_year) &
        (F.col("month") == target_month) &
        (F.col("day") == target_day)
    )
)

# ============================================================
# 5. Base Join
# ============================================================

joined = (
    hagent.alias("h")
    .join(
        call_rec.alias("c"),
        (
            (F.col("h.logid") == F.col("c.anslogin")) &
            (F.col("h.year") == F.col("c.year")) &
            (F.col("h.month") == F.col("c.month")) &
            (F.col("h.day") == F.col("c.day"))
        ),
        how="left"
    )
)

# ============================================================
# 6. REPORT 1
# ============================================================

q1 = (
    joined
    .groupBy(
        "h.agentname",
        "h.logid",
        "h.split",
        "h.year",
        "h.month",
        "h.day"
    )
    .agg(
        F.count("c.callid").alias("total_calls_in_call_rec"),
        F.sum("c.duration").alias("total_duration_seconds"),
        F.sum("h.acdcalls").alias("total_acd_calls_from_hagent"),
        F.sum("h.acdtime").alias("total_acd_time_from_hagent")
    )
)

q1.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://datalake-curated/agent_split_summary/")

# ============================================================
# 7. REPORT 2
# ============================================================

q2 = (
    joined
    .groupBy(
        "h.agentname",
        "h.logid",
        "h.year",
        "h.month",
        "h.day"
    )
    .agg(
        F.sum("h.acdtime").alias("total_acd_time_hagent"),
        F.sum("c.duration").alias("total_call_duration_rec")
    )
    .withColumn(
        "duration_vs_acd_ratio_percent",
        F.round(
            F.when(
                F.col("total_acd_time_hagent") > 0,
                (
                    F.col("total_call_duration_rec").cast("double")
                    /
                    F.col("total_acd_time_hagent")
                ) * 100
            ).otherwise(0),
            2
        )
    )
)

q2.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://datalake-curated/agent_duration_ratio/")

# ============================================================
# 8. REPORT 3
# ============================================================

q3 = (
    joined
    .groupBy(
        "h.agentname",
        "h.logid",
        "h.split",
        "h.year",
        "h.month",
        "h.day"
    )
    .agg(
        F.sum("h.acdcalls").alias("acd_calls_hagent"),
        F.count("c.callid").alias("actual_calls_call_rec")
    )
    .withColumn(
        "call_discrepancy",
        F.abs(
            F.col("acd_calls_hagent")
            -
            F.col("actual_calls_call_rec")
        )
    )
    .filter(F.col("call_discrepancy") > 0)
)

q3.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://datalake-curated/call_discrepancy/")

# ============================================================
# 9. REPORT 4
# ============================================================

q4 = (
    joined
    .groupBy(
        "h.split",
        "h.year",
        "h.month",
        "h.day"
    )
    .agg(
        F.countDistinct("h.logid").alias("total_agents"),
        F.sum("h.acdcalls").alias("total_split_acd_calls"),
        F.sum("h.acdtime").alias("total_split_acd_time"),
        F.count("c.callid").alias("total_split_call_rec_count"),
        F.sum("c.duration").alias("total_split_call_duration")
    )
)

q4.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://datalake-curated/split_summary/")

# ============================================================
# 10. REPORT 5
# ============================================================

q5 = (
    call_rec.alias("c")
    .join(
        hagent.alias("h"),
        (
            (F.col("c.anslogin") == F.col("h.logid")) &
            (F.col("c.year") == F.col("h.year")) &
            (F.col("c.month") == F.col("h.month")) &
            (F.col("c.day") == F.col("h.day"))
        ),
        how="left"
    )
    .filter(F.col("h.logid").isNull())
    .select(
        "c.callid",
        "c.anslogin",
        "c.duration",
        "c.year",
        "c.month",
        "c.day"
    )
)

q5.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .parquet("s3a://datalake-curated/orphan_calls/")

print("DONE: All 5 reports successfully processed for date:", f"{target_year}-{target_month}-{target_day}")
spark.stop()
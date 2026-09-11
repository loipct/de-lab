import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Nhận tham số ngày chạy
if len(sys.argv) >= 4:
    target_year = str(sys.argv[1])
    target_month = str(sys.argv[2])
    target_day = str(sys.argv[3])
else:
    target_year = "2026"
    target_month = "09"
    target_day = "06"

print(f"Running analytics for Date: {target_year}-{target_month}-{target_day}")

# ============================================================
# 1. Spark Session với Hive Metastore & Dynamic Overwrite
# ============================================================
spark = (
    SparkSession.builder
    .appName("hagent_callrec_hive_curated_pipeline")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    # QUAN TRỌNG: Đảm bảo khi ghi đè chỉ ghi đè đúng partition ngày chạy (year/month/day)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.convertTimestampNano", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Chọn database đích
spark.sql("USE datalake_curated")

# ============================================================
# 2. Đọc dữ liệu từ tầng Raw
# ============================================================
# (Nếu raw nằm ở datalake_raw thì gọi datalake_raw.call_rec)
call_rec = (
    spark.table("datalake_raw.call_rec")
    .filter(
        (F.col("year") == target_year) &
        (F.col("month") == target_month) &
        (F.col("day") == target_day)
    )
)

hagent = (
    spark.table("datalake_raw.hagent")
    .filter(
        (F.col("year") == target_year) &
        (F.col("month") == target_month) &
        (F.col("day") == target_day)
    )
)

# ============================================================
# 3. Pre-aggregate CALL_REC (Khắc phục lỗi x2/xN dữ liệu)
# ============================================================
call_rec_agg = (
    call_rec
    .groupBy("anslogin", "year", "month", "day")
    .agg(
        F.count("callid").alias("total_calls_in_call_rec"),
        F.sum("duration").alias("total_duration_seconds")
    )
)

# ============================================================
# REPORT 1: datalake_curated.agent_split_summary
# ============================================================
q1 = (
    hagent.alias("h")
    .join(
        call_rec_agg.alias("c"),
        (
            (F.col("h.logid") == F.col("c.anslogin")) &
            (F.col("h.year") == F.col("c.year")) &
            (F.col("h.month") == F.col("c.month")) &
            (F.col("h.day") == F.col("c.day"))
        ),
        how="left"
    )
    .groupBy(
        "h.agentname",
        "h.logid",
        "h.split",
        "h.year",
        "h.month",
        "h.day"
    )
    .agg(
        F.coalesce(F.first("c.total_calls_in_call_rec"), F.lit(0)).alias("total_calls_in_call_rec"),
        F.coalesce(F.first("c.total_duration_seconds"), F.lit(0)).alias("total_duration_seconds"),
        F.sum("h.acdcalls").alias("total_acd_calls_from_hagent"),
        F.sum("h.acdtime").alias("total_acd_time_from_hagent")
    )
)

(
    q1.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("datalake_curated.agent_split_summary")
)

# ============================================================
# REPORT 2: datalake_curated.agent_duration_ratio
# ============================================================
q2 = (
    hagent.groupBy("logid", "agentname", "year", "month", "day")
    .agg(F.sum("acdtime").alias("total_acd_time_hagent"))
    .join(
        call_rec_agg,
        (
            (F.col("logid") == F.col("anslogin")) &
            (F.col("year") == F.col("year")) &
            (F.col("month") == F.col("month")) &
            (F.col("day") == F.col("day"))
        ),
        how="left"
    )
    .select(
        "agentname",
        "logid",
        "total_acd_time_hagent",
        F.coalesce(F.col("total_duration_seconds"), F.lit(0)).alias("total_call_duration_rec"),
        "year",
        "month",
        "day"
    )
    .withColumn(
        "duration_vs_acd_ratio_percent",
        F.round(
            F.when(
                F.col("total_acd_time_hagent") > 0,
                (F.col("total_call_duration_rec").cast("double") / F.col("total_acd_time_hagent")) * 100
            ).otherwise(0),
            2
        )
    )
)

(
    q2.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("datalake_curated.agent_duration_ratio")
)

# ============================================================
# REPORT 3: datalake_curated.call_discrepancy
# ============================================================
q3 = (
    hagent.groupBy("agentname", "logid", "split", "year", "month", "day")
    .agg(F.sum("acdcalls").alias("acd_calls_hagent"))
    .join(
        call_rec_agg,
        (
            (F.col("logid") == F.col("anslogin")) &
            (F.col("year") == F.col("year")) &
            (F.col("month") == F.col("month")) &
            (F.col("day") == F.col("day"))
        ),
        how="left"
    )
    .withColumn(
        "actual_calls_call_rec",
        F.coalesce(F.col("total_calls_in_call_rec"), F.lit(0))
    )
    .withColumn(
        "call_discrepancy",
        F.abs(F.col("acd_calls_hagent") - F.col("actual_calls_call_rec"))
    )
    .filter(F.col("call_discrepancy") > 0)
    .select(
        "agentname", "logid", "split",
        "acd_calls_hagent", "actual_calls_call_rec", "call_discrepancy",
        "year", "month", "day"
    )
)

(
    q3.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("datalake_curated.call_discrepancy")
)

# ============================================================
# REPORT 4: datalake_curated.split_summary
# ============================================================
hagent_split = (
    hagent.groupBy("split", "year", "month", "day")
    .agg(
        F.countDistinct("logid").alias("total_agents"),
        F.sum("acdcalls").alias("total_split_acd_calls"),
        F.sum("acdtime").alias("total_split_acd_time")
    )
)

call_with_split = (
    call_rec.alias("c")
    .join(
        hagent.select("logid", "split", "year", "month", "day").distinct().alias("h"),
        (
            (F.col("c.anslogin") == F.col("h.logid")) &
            (F.col("c.year") == F.col("h.year")) &
            (F.col("c.month") == F.col("h.month")) &
            (F.col("c.day") == F.col("h.day"))
        ),
        how="inner"
    )
    .groupBy("h.split", "c.year", "c.month", "c.day")
    .agg(
        F.count("c.callid").alias("total_split_call_rec_count"),
        F.sum("c.duration").alias("total_split_call_duration")
    )
)

q4 = (
    hagent_split.alias("hs")
    .join(
        call_with_split.alias("cs"),
        (
            (F.col("hs.split") == F.col("cs.split")) &
            (F.col("hs.year") == F.col("cs.year")) &
            (F.col("hs.month") == F.col("cs.month")) &
            (F.col("hs.day") == F.col("cs.day"))
        ),
        how="left"
    )
    .select(
        F.col("hs.split"),
        F.col("hs.total_agents"),
        F.col("hs.total_split_acd_calls"),
        F.col("hs.total_split_acd_time"),
        F.coalesce(F.col("cs.total_split_call_rec_count"), F.lit(0)).alias("total_split_call_rec_count"),
        F.coalesce(F.col("cs.total_split_call_duration"), F.lit(0)).alias("total_split_call_duration"),
        F.col("hs.year"),
        F.col("hs.month"),
        F.col("hs.day")
    )
)

(
    q4.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("datalake_curated.split_summary")
)

# ============================================================
# REPORT 5: datalake_curated.orphan_calls
# ============================================================
q5 = (
    call_rec.alias("c")
    .join(
        hagent.select("logid", "year", "month", "day").distinct().alias("h"),
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

(
    q5.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("datalake_curated.orphan_calls")
)

print(f"DONE: 5 reports written to datalake_curated for date: {target_year}-{target_month}-{target_day}")
spark.stop()
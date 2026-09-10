from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("hagent_callrec_analytics_pipeline")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# Lọc dữ liệu theo ngày cần xử lý (ví dụ: 2026-09-06)
call_rec = spark.read.parquet("s3a://datalake-raw/call_rec/").filter(
    (F.col("year") == "2026") & (F.col("month") == "09") & (F.col("day") == "06")
)
hagent = spark.read.parquet("s3a://datalake-raw/hagent/").filter(
    (F.col("year") == "2026") & (F.col("month") == "09") & (F.col("day") == "06")
)

# Base Join chung
joined = hagent.alias("h").join(
    call_rec.alias("c"),
    (F.col("h.logid") == F.col("c.anslogin")) & 
    (F.col("h.year") == F.col("c.year")) & 
    (F.col("h.month") == F.col("c.month")) & 
    (F.col("h.day") == F.col("c.day")),
    how="left"
)

# Báo cáo 1: Summary theo Agent & Split
q1 = joined.groupBy("h.agentname", "h.logid", "h.split", "h.year", "h.month", "h.day").agg(
    F.count("c.callid").alias("total_calls_in_call_rec"),
    F.sum("c.duration").alias("total_duration_seconds"),
    F.sum("h.acdcalls").alias("total_acd_calls_from_hagent"),
    F.sum("h.acdtime").alias("total_acd_time_from_hagent")
)
q1.write.mode("overwrite").partitionBy("year", "month", "day").parquet("s3a://datalake-curated/agent_split_summary/")

# Báo cáo 2: Tỷ lệ Duration vs ACD Ratio (%)
q2 = joined.groupBy("h.agentname", "h.logid", "h.year", "h.month", "h.day").agg(
    F.sum("h.acdtime").alias("total_acd_time_hagent"),
    F.sum("c.duration").alias("total_call_duration_rec")
).withColumn(
    "duration_vs_acd_ratio_percent",
    F.round(F.when(F.col("total_acd_time_hagent") > 0, (F.col("total_call_duration_rec").cast("double") / F.col("total_acd_time_hagent")) * 100).otherwise(0), 2)
)
q2.write.mode("overwrite").partitionBy("year", "month", "day").parquet("s3a://datalake-curated/agent_duration_ratio/")

# Báo cáo 3: Phát hiện sự chênh lệch cuộc gọi (Call Discrepancy)
q3 = joined.groupBy("h.agentname", "h.logid", "h.split", "h.year", "h.month", "h.day").agg(
    F.sum("h.acdcalls").alias("acd_calls_hagent"),
    F.count("c.callid").alias("actual_calls_call_rec")
).withColumn(
    "call_discrepancy", F.abs(F.col("acd_calls_hagent") - F.col("actual_calls_call_rec"))
).filter(F.col("call_discrepancy") > 0).orderBy(F.desc("call_discrepancy"))
q3.write.mode("overwrite").partitionBy("year", "month", "day").parquet("s3a://datalake-curated/call_discrepancy/")

# Báo cáo 4: Tổng hợp theo Split (Nhóm tổng đài)
q4 = joined.groupBy("h.split", "h.year", "h.month", "h.day").agg(
    F.countDistinct("h.logid").alias("total_agents"),
    F.sum("h.acdcalls").alias("total_split_acd_calls"),
    F.sum("h.acdtime").alias("total_split_acd_time"),
    F.count("c.callid").alias("total_split_call_rec_count"),
    F.sum("c.duration").alias("total_split_call_duration")
).orderBy(F.desc("total_split_acd_calls"))
q4.write.mode("overwrite").partitionBy("year", "month", "day").parquet("s3a://datalake-curated/split_summary/")

# Báo cáo 5: Tìm cuộc gọi mồ côi (Orphan Calls)
q5 = call_rec.alias("c").join(
    hagent.alias("h"),
    (F.col("c.anslogin") == F.col("h.logid")) & 
    (F.col("c.year") == F.col("h.year")) & 
    (F.col("c.month") == F.col("h.month")) & 
    (F.col("c.day") == F.col("h.day")),
    how="left"
).filter(F.col("h.logid").isNull()).select("c.callid", "c.anslogin", "c.duration", "c.year", "c.month", "c.day")
q5.write.mode("overwrite").partitionBy("year", "month", "day").parquet("s3a://datalake-curated/orphan_calls/")

print("Done: All 5 analytical reports successfully processed and written to MinIO.")
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

# ============================================================
# 2. Schema đồng bộ chính xác với DDL Hive/Trino
# ============================================================

call_rec_schema = """
    seqnum long,
    acd integer,
    row_date date,
    row_time integer,
    acwtime long,
    ansholdtime long,
    anslogin string,
    assist integer,
    audio integer,
    callid long,
    calling_pty string,
    conference integer,
    consulttime long,
    da_queued integer,
    dialed_num string,
    dispivector integer,
    disposition integer,
    disppriority integer,
    dispsplit integer,
    disptime long,
    dispvdn string,
    duration long,
    eqloc string,
    event1 integer,
    event2 integer,
    event3 integer,
    event4 integer,
    event5 integer,
    event6 integer,
    event7 integer,
    event8 integer,
    event9 integer,
    firstivector integer,
    firstvdn string,
    vdn2 string,
    vdn3 string,
    vdn4 string,
    vdn5 string,
    vdn6 string,
    vdn7 string,
    vdn8 string,
    vdn9 string,
    held integer,
    holdabn integer,
    lastcwc string,
    lastdigits string,
    lastobserver string,
    malicious integer,
    observingcall integer,
    origlogin string,
    segment integer,
    segstart long,
    segstart_utc long,
    segstop long,
    segstop_utc long,
    split1 integer,
    split2 integer,
    split3 integer,
    talktime long,
    tkgrp integer,
    transferred integer,
    agt_released integer,
    ansreason integer,
    calling_ii string,
    dispsklevel integer,
    origreason integer,
    netintime long,
    origholdtime long,
    ucid string,
    anslocid integer,
    eqlocid integer,
    obslocid integer,
    origlocid integer,
    cwc1 string,
    cwc2 string,
    cwc3 string,
    cwc4 string,
    cwc5 string,
    queuetime long,
    ringtime long,
    uui_len integer,
    asai_uui string,
    interruptdel integer,
    agentsurplus integer,
    agentskilllevel integer,
    prefskilllevel integer,
    icrresent integer,
    icrpullreason integer,
    orig_attrib_id string,
    ans_attrib_id string,
    obs_attrib_id string,
    tenant long,
    ecd_num long,
    ecd_control integer,
    ecd_info integer,
    ecd_str string,
    year string,
    month string,
    day string
"""

hagent_schema = """
    id string,
    abncalls integer,
    abntime integer,
    acceptedintrs integer,
    acd integer,
    acd_release long,
    acdauxoutcalls integer,
    acdcalls integer,
    acdcalls_r1 integer,
    acdcalls_r2 integer,
    acdtime integer,
    acwincalls integer,
    acwintime integer,
    acwoutadjcalls integer,
    acwoutcalls integer,
    acwoutoffcalls integer,
    acwoutofftime integer,
    acwouttime integer,
    acwtime integer,
    ansringtime integer,
    assists integer,
    attrib_id string,
    auxincalls integer,
    auxintime integer,
    auxoutadjcalls integer,
    auxoutcalls integer,
    auxoutoffcalls integer,
    auxoutofftime integer,
    auxouttime integer,
    conference integer,
    da_abncalls integer,
    da_abntime integer,
    da_acdcalls integer,
    da_acdtime integer,
    da_acwincalls integer,
    da_acwintime integer,
    da_acwoadjcalls integer,
    da_acwocalls integer,
    da_acwooffcalls integer,
    da_acwoofftime integer,
    da_acwotime integer,
    da_acwtime integer,
    da_anstime integer,
    da_icrpullcalls long,
    da_icrpulltime long,
    da_othercalls integer,
    da_othertime integer,
    da_release long,
    event1 integer,
    event2 integer,
    event3 integer,
    event4 integer,
    event5 integer,
    event6 integer,
    event7 integer,
    event8 integer,
    event9 integer,
    extension string,
    holdabncalls integer,
    holdacdtime long,
    holdcalls integer,
    holdtime integer,
    i_acdaux_outtime integer,
    i_acdauxintime integer,
    i_acdothertime integer,
    i_acdtime integer,
    i_acwintime integer,
    i_acwouttime integer,
    i_acwtime integer,
    i_auxintime integer,
    i_auxouttime integer,
    i_auxstbytime integer,
    i_auxtime long,
    i_availtime integer,
    i_da_acdtime integer,
    i_da_acwtime integer,
    i_otherstbytime integer,
    i_othertime integer,
    i_ringtime integer,
    i_stafftime integer,
    icrpullcalls long,
    icrpulltime long,
    incomplete integer,
    intrdeliveries integer,
    intrnotifies integer,
    intrvl integer,
    loc_id integer,
    logid string,
    noansredir integer,
    o_acdcalls integer,
    o_acdtime integer,
    o_acwtime integer,
    partial_archive integer,
    phantomabns integer,
    rejectedintrs integer,
    ringcalls integer,
    ringtime integer,
    row_date timestamp,
    rsv_level integer,
    split integer,
    starttime integer,
    starttime_utc long,
    tenant integer,
    ti_auxtime integer,
    ti_auxtime0 long,
    ti_auxtime1 long,
    ti_auxtime2 long,
    ti_auxtime3 long,
    ti_auxtime4 long,
    ti_auxtime5 long,
    ti_auxtime6 long,
    ti_auxtime7 long,
    ti_auxtime8 long,
    ti_auxtime9 long,
    ti_availtime integer,
    ti_othertime long,
    ti_stafftime integer,
    transferred integer,
    importsessionid string,
    ignorereport boolean,
    agentname string,
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
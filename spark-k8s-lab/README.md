Cài đặt Spark Operator qua Helm (hoặc file manifest), sau đó sử dụng lệnh `kubectl edit` để cấu hình chuyển đổi namespace sang `de-lab` (hoặc điều chỉnh trường watchedNamespaces/operatorNamespace phù hợp):

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace de-lab \
  --set webhook.enable=true \
  --set watchedNamespaces={de-lab}

# Nếu cần chỉnh sửa trực tiếp cấu hình deployment của operator:
kubectl edit deployment spark-operator -n de-lab

```

---

Tạo Phân quyền RBAC cho Spark Job trong `de-lab`:

```bash
kubectl apply -f spark-rbac.yaml -n de-lab

```

---

Triển khai Spark Job (`spark-application.yaml`):

```bash
kubectl apply -f spark-application.yaml -n de-lab

```

---

Kiểm tra mọi thứ trong `de-lab`:

* Kiểm tra xem Spark Operator và các thành phần đã chạy chưa:

```bash
kubectl get all -n de-lab

```

* Kiểm tra trạng thái chạy của Spark Job:

```bash
kubectl get sparkapplications -n de-lab

```

* Xem log của Driver Job:

```bash
kubectl logs -f hagent-analytics-job-driver -n de-lab

```

---

Truy vấn dữ liệu Curated trên Trino:

Dưới đây là script SQL hoàn chỉnh để tạo schema, khai báo bảng đồng bộ với kiểu dữ liệu `BIGINT`/`VARCHAR`, đồng bộ phân vùng và kiểm tra kết quả:

```sql
-- ============================================================
-- 1. TẠO SCHEMA & CÁC BẢNG CURATED
-- ============================================================

-- Tạo schema datalake_curated nếu chưa tồn tại
CREATE SCHEMA IF NOT EXISTS hive.datalake_curated
WITH (location = 's3a://datalake-curated/');

-- Drop các bảng cũ để tạo lại đúng schema khớp với Parquet
DROP TABLE IF EXISTS hive.datalake_curated.agent_split_summary;
DROP TABLE IF EXISTS hive.datalake_curated.agent_duration_ratio;
DROP TABLE IF EXISTS hive.datalake_curated.split_summary;
DROP TABLE IF EXISTS hive.datalake_curated.call_discrepancy;
DROP TABLE IF EXISTS hive.datalake_curated.orphan_calls;

-- 1. Agent Split Summary
CREATE TABLE hive.datalake_curated.agent_split_summary (
    agentname VARCHAR,
    logid VARCHAR,
    split BIGINT,
    total_calls_in_call_rec BIGINT,
    total_duration_seconds BIGINT,
    total_acd_calls_from_hagent BIGINT,
    total_acd_time_from_hagent BIGINT,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-curated/agent_split_summary/',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- 2. Agent Duration Ratio
CREATE TABLE hive.datalake_curated.agent_duration_ratio (
    agentname VARCHAR,
    logid VARCHAR,
    total_acd_time_hagent BIGINT,
    total_call_duration_rec BIGINT,
    duration_vs_acd_ratio_percent DOUBLE,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-curated/agent_duration_ratio/',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- 3. Split Summary
CREATE TABLE hive.datalake_curated.split_summary (
    split BIGINT,
    total_agents BIGINT,
    total_split_acd_calls BIGINT,
    total_split_acd_time BIGINT,
    total_split_call_rec_count BIGINT,
    total_split_call_duration BIGINT,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-curated/split_summary/',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- 4. Call Discrepancy
CREATE TABLE hive.datalake_curated.call_discrepancy (
    agentname VARCHAR,
    logid VARCHAR,
    split BIGINT,
    acd_calls_hagent BIGINT,
    actual_calls_call_rec BIGINT,
    call_discrepancy BIGINT,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-curated/call_discrepancy/',
    partitioned_by = ARRAY['year', 'month', 'day']
);

-- 5. Orphan Calls
CREATE TABLE hive.datalake_curated.orphan_calls (
    callid BIGINT,
    anslogin VARCHAR,
    duration BIGINT,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-curated/orphan_calls/',
    partitioned_by = ARRAY['year', 'month', 'day']
);


-- ============================================================
-- 2. ĐỒNG BỘ PHÂN VÙNG METADATA TỪ MINIO
-- ============================================================

CALL hive.system.sync_partition_metadata('datalake_curated', 'agent_split_summary', 'ADD');
CALL hive.system.sync_partition_metadata('datalake_curated', 'agent_duration_ratio', 'ADD');
CALL hive.system.sync_partition_metadata('datalake_curated', 'split_summary', 'ADD');
CALL hive.system.sync_partition_metadata('datalake_curated', 'call_discrepancy', 'ADD');
CALL hive.system.sync_partition_metadata('datalake_curated', 'orphan_calls', 'ADD');


-- ============================================================
-- 3. CÂU LỆNH KIỂM TRA DỮ LIỆU (SELECT)
-- ============================================================

-- Kiểm tra dữ liệu tổng hợp Agent Split Summary
SELECT * 
FROM hive.datalake_curated.agent_split_summary 
WHERE year = '2026' AND month = '09' AND day = '06'
ORDER BY total_acd_calls_from_hagent DESC 
LIMIT 20;

-- Kiểm tra dữ liệu tỷ lệ Duration vs ACD Ratio
SELECT * 
FROM hive.datalake_curated.agent_duration_ratio 
WHERE year = '2026' AND month = '09' AND day = '06'
LIMIT 20;

-- Kiểm tra dữ liệu chênh lệch cuộc gọi (Call Discrepancy)
SELECT * 
FROM hive.datalake_curated.call_discrepancy 
WHERE year = '2026' AND month = '09' AND day = '06'
LIMIT 20;

-- Kiểm tra dữ liệu tổng hợp Split Summary
SELECT * 
FROM hive.datalake_curated.split_summary 
WHERE year = '2026' AND month = '09' AND day = '06';

-- Kiểm tra dữ liệu cuộc gọi mồ côi (Orphan Calls)
SELECT * 
FROM hive.datalake_curated.orphan_calls 
WHERE year = '2026' AND month = '09' AND day = '06'
LIMIT 20;

```
## Cài đặt và kiểm tra Spark Operator

Trước tiên, thêm repo Helm của Spark Operator và cập nhật danh sách chart:

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
```

Trên cluster hiện tại, Spark Operator đã chạy ổn định dưới dạng các pod trong namespace `de-lab`, cụ thể là `spark-operator-controller` và `spark-operator-webhook`. Nếu cần chỉnh cấu hình của controller, phải edit đúng namespace `de-lab`, ví dụ:

```bash
kubectl edit deployment spark-operator-controller -n de-lab
```

Không được edit ở namespace mặc định. Sau khi operator sẵn sàng, tiến hành tạo quyền truy cập cho Spark job trong namespace `de-lab`:

```bash
kubectl apply -f spark-k8s-lab/spark-rbac.yaml -n de-lab

```

---

Tạo file YAML từ biến môi trường local rồi apply. Đây là cách đúng với SparkApplication CRD, vì `envVars` phải là map chuỗi và không hỗ trợ `valueFrom` ở định dạng Kubernetes SecretRef trong schema này. Nói ngắn gọn: phải `source ~/de-lab/.env.local` rồi render YAML bằng heredoc trước khi `kubectl apply`.

```bash
source ~/de-lab/.env.local

cat <<EOF | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: hagent-analytics-job
  namespace: de-lab
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: apache/spark:3.5.0
  imagePullPolicy: IfNotPresent
  sparkVersion: "3.5.0"
  mainApplicationFile: "https://raw.githubusercontent.com/loipct/de-lab/main/spark-k8s-lab/job/job_analytics.py"
  deps:
    jars:
      - "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar"
      - "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar"
  sparkConf:
    spark.hadoop.fs.s3a.endpoint: "http://minio:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
  restartPolicy:
    type: OnFailure
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1024m"
    serviceAccount: spark-operator-sa
    envVars:
      AWS_ACCESS_KEY_ID: "${MINIO_ACCESS_KEY}"
      AWS_SECRET_ACCESS_KEY: "${MINIO_SECRET_KEY}"
  executor:
    cores: 2
    instances: 3
    memory: "2048m"
    envVars:
      AWS_ACCESS_KEY_ID: "${MINIO_ACCESS_KEY}"
      AWS_SECRET_ACCESS_KEY: "${MINIO_SECRET_KEY}"
EOF
```

> `sparkConf` chỉ giữ endpoint và cấu hình Hadoop không nhạy cảm. Access key và secret key phải được render từ `~/de-lab/.env.local` trước khi apply; nếu để nguyên file YAML raw, Kubernetes sẽ không expand `${VAR}` và Spark sẽ không nhận được AWS credential.

---

Kiểm tra mọi thứ trong `de-lab`:

* Kiểm tra resource cluster và các pod đang chạy:

```bash
kubectl get all -n de-lab
```

* Kiểm tra trạng thái chạy của Spark job:

```bash
kubectl get sparkapplications -n de-lab
```

* Xem log của driver job:

```bash
kubectl logs -f hagent-analytics-job-driver -n de-lab
```

> Nếu resource `spark-operator` không tồn tại, nghĩa là bạn đang cố thao tác vào deployment không được tạo trong lab này. Hướng dẫn đúng là dùng `spark-rbac.yaml` + `spark-application.yaml` như trên.

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
# Dự án Data Lake cho dữ liệu CSKH và phân tích tương tác khách hàng

Dự án này xây dựng một nền tảng dữ liệu theo hướng lakehouse để thu thập, lưu trữ và phân tích dữ liệu từ các nguồn liên quan đến chăm sóc khách hàng (CSKH) và hoạt động tương tác với khách hàng. Mục tiêu là tạo một kiến trúc dữ liệu có thể mở rộng theo thời gian, cho phép tích hợp nhiều nguồn dữ liệu khác nhau như call log, agent activity, ticket, CRM, phản hồi khách hàng, chat, email, và các dữ liệu vận hành liên quan.

Hiện tại, hệ thống đang tập trung vào việc đồng bộ dữ liệu từ các nguồn MSSQL/warehouse vào MinIO theo dạng raw data có phân vùng theo ngày, sau đó có thể truy vấn qua Hive/Trino hoặc xử lý tiếp bằng Spark. Cấu trúc này giúp dễ dàng mở rộng sang nhiều nguồn dữ liệu khác trong tương lai mà không cần phá cấu trúc hiện có.

Các thành phần chính của dự án:
- MinIO: lưu trữ raw data dạng Parquet, theo từng bảng và phân vùng `year/month/day`
- Airflow: điều phối và tự động hóa quá trình ingest dữ liệu
- Hive metastore: quản lý metadata, schema và external tables
- Trino: truy vấn nhanh trên dữ liệu data lake
- Spark: xử lý ETL, join dữ liệu, tổng hợp KPI và xây dựng curated layer

Mục tiêu kinh doanh:
- kết nối dữ liệu từ nhiều nguồn liên quan đến CSKH
- chuẩn hóa dữ liệu để phục vụ phân tích hiệu suất đội ngũ chăm sóc khách hàng
- hỗ trợ báo cáo về cuộc gọi, thời gian xử lý, agent, SLA, chất lượng dịch vụ
- tạo nền tảng mở cho các mô hình phân tích và BI trong tương lai

Hướng phát triển tiếp theo:
- tích hợp thêm nguồn dữ liệu CSKH như CRM, ticketing, chat, email, survey, feedback, call recording, IVR
- xây dựng các layer raw → curated → analytics
- mở rộng mô hình ETL/ELT để phục vụ báo cáo và dashboard cho bộ phận CSKH, vận hành và quản lý

Yêu cầu:
- `kubectl` đã cấu hình tới cluster.
- Namespace mặc định trong các manifest là `de-lab`.

## Triển khai MinIO

```bash
kubectl apply -f minio-k8s-lab/minio-lab.yaml

# Kiểm tra
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep minio

# Port-forward console (tùy chọn)
kubectl port-forward svc/minio -n de-lab 9001:9001
# Console: http://localhost:9001
```

Mặc định (trong manifest): `MINIO_ROOT_USER=minioadmin`, `MINIO_ROOT_PASSWORD=minioadmin123`.


---------------------------------


Tạo Access/Secret MinIO (chỉ MinIO)

1) Tạo service account MinIO và lấy access/secret (dùng `mc` - MinIO Client):

```bash
# Thêm alias tới MinIO (chạy trên máy nơi MinIO có thể truy cập)
mc alias set myminio http://localhost:9000 minioadmin minioadmin123

# Tạo service account (ví dụ tên svcacct: minioadmin)
# Kết quả sẽ hiển thị accessKey và secretKey
mc admin user svcacct add myminio minioadmin

# Nếu mc hỗ trợ JSON output, bạn có thể trích xuất bằng jq:
# CREDS_JSON=$(mc admin user svcacct add myminio minioadmin --json)
# ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.credentials.accessKey')
# SECRET_KEY=$(echo "$CREDS_JSON" | jq -r '.credentials.secretKey')
```

## Triển khai Hive metastore

```bash
kubectl apply -f hive-k8s-lab/hive-lab.yaml

# Kiểm tra
kubectl get pods -n de-lab
kubectl logs -f <hive-metastore-pod> -n de-lab
```

Ghi chú: Hive metastore thường dùng Thrift trên cổng `9083`.

## Triển khai Postgres + Trino

```bash
# Postgres
kubectl apply -f trino-k8s-lab/postgres.yaml

# Trino coordinator
kubectl apply -f trino-k8s-lab/trino-coordinator.yaml

# Trino workers
kubectl apply -f trino-k8s-lab/trino-workers.yaml

# Kiểm tra
kubectl get pods -n de-lab
# Hướng dẫn triển khai (thứ tự: MinIO → PostgreSQL → Hive → Trino)

Tóm tắt: hướng dẫn ngắn để triển khai MinIO, tạo access/secret, cấu hình PostgreSQL (tạo user và cấp quyền), sau đó triển khai Hive và Trino.

Yêu cầu:
- `kubectl` đã cấu hình tới cluster.
- Namespace mặc định trong các manifest là `de-lab`.
```
1) Triển khai MinIO & tạo access/secret

```bash
# Triển khai MinIO
kubectl apply -f minio-k8s-lab/minio-lab.yaml

# Kiểm tra
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep minio

# Port-forward console (tùy chọn)
kubectl port-forward svc/minio -n de-lab 9001:9001
# Mở: http://localhost:9001
```

Tài khoản quản trị mặc định (trong manifest): `MINIO_ROOT_USER=minioadmin`, `MINIO_ROOT_PASSWORD=minioadmin123`.

Tạo service account và lấy access/secret bằng `mc` (MinIO Client):

```bash
# Trên máy có thể kết nối tới MinIO
mc alias set myminio http://localhost:9000 minioadmin minioadmin123

# Tạo service account (ví dụ tên: svcacct-minio)
mc admin user svcacct add myminio svcacct-minio

# mc hiển thị accessKey và secretKey; nếu mc hỗ trợ JSON, có thể trích xuất bằng jq
# CREDS_JSON=$(mc admin user svcacct add myminio svcacct-minio --json)
# ACCESS_KEY=$(echo "$CREDS_JSON" | jq -r '.credentials.accessKey')
# SECRET_KEY=$(echo "$CREDS_JSON" | jq -r '.credentials.secretKey')
```

Lưu ý: lưu trữ `ACCESS_KEY`/`SECRET_KEY` an toàn (Kubernetes Secret hoặc vault). Không commit vào repo.

2) Triển khai PostgreSQL + tạo user, database và cấp quyền

```bash
# Triển khai Postgres
kubectl apply -f trino-k8s-lab/postgres.yaml

# Exec vào pod Postgres (thay <postgres-pod> bằng tên pod thực tế)
kubectl exec -it <postgres-pod> -n de-lab -- psql -U postgres
```

Lệnh SQL mẫu (chạy trong psql):

```sql
-- Tạo users
CREATE USER labuser WITH PASSWORD 'labpassword';
CREATE USER hive WITH PASSWORD 'hive';

-- Tạo databases
CREATE DATABASE labdb;
CREATE DATABASE metastore_db OWNER hive;

-- Quyền cho Trino user (labuser) trên labdb
\c labdb
GRANT CONNECT ON DATABASE labdb TO labuser;
GRANT USAGE ON SCHEMA public TO labuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO labuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO labuser;

-- Quyền cho Hive user (hive) trên metastore_db
\c metastore_db
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
GRANT USAGE ON SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO hive;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO hive;
```

Ghi chú: thay mật khẩu mẫu bằng mật khẩu mạnh và kiểm tra tồn tại user/db trước khi chạy để tránh lỗi.

3) Triển khai Hive

```bash
kubectl apply -f hive-k8s-lab/hive-lab.yaml

# Kiểm tra pod/service
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep hive
```

Ghi chú: Hive metastore mặc định dùng Thrift trên cổng `9083`. Đảm bảo `core-site.xml` (hive config) tham chiếu đúng MinIO endpoint và keys nếu dùng S3.

## Triển khai Trino

```bash
kubectl apply -f trino-k8s-lab/trino-coordinator.yaml
kubectl apply -f trino-k8s-lab/trino-workers.yaml

# Kiểm tra
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep trino
```

## Cập nhật `hive.properties` cho Trino (sau khi MinIO + Hive sẵn sàng)

Mở `trino-k8s-lab/trino-coordinator.yaml` và cập nhật `hive.properties` catalog với các giá trị:

```
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=<MINIO_ACCESS_KEY>
hive.s3.aws-secret-key=<MINIO_SECRET_KEY>
hive.s3.path-style-access=true
```

## Triển khai Spark trên Kubernetes (namespace `de-lab` + RBAC)

Spark được deploy theo namespace `de-lab` để chạy các job phân tích trên MinIO. Trước khi chạy job, cần tạo ServiceAccount và quyền RBAC cho Spark operator / driver.

### 1) Tạo RBAC cho namespace `de-lab`

```bash
kubectl apply -f spark-k8s-lab/spark-rbac.yaml
```

### 2) Deploy SparkApplication

```bash
kubectl apply -f spark-k8s-lab/spark-application.yaml
```

File `spark-k8s-lab/spark-application.yaml` dùng namespace `de-lab` và chạy job Python trên cluster Spark

### 3) Kiểm tra job Spark

```bash
kubectl get sparkapplication -n de-lab
kubectl get pods -n de-lab | grep hagent-analytics-job
kubectl logs -n de-lab <driver-pod-name> --tail=100
```

Nếu job chạy thành công, Spark sẽ đọc raw data từ MinIO, xử lý `hagent` + `call_rec`, rồi ghi kết quả vào bucket curated như:

```text
s3a://datalake-curated/agent_call_summary/
```

### 4) Lưu ý quan trọng

- Tất cả resource Spark phải chạy trong cùng namespace `de-lab`.
- `ServiceAccount` và `RoleBinding` phải khớp với namespace `de-lab` để driver/executor có quyền tạo pod, configmap và service.
- `MINIO_ACCESS_KEY` và `MINIO_SECRET_KEY` nên được lưu dưới dạng Secret thay vì hardcode trực tiếp trong file YAML.
- Nếu dùng MinIO trên cluster nội bộ, nên đảm bảo `minio` service có thể resolve từ pod Spark trong namespace `de-lab`.

### 5) Gỡ bỏ Spark khỏi namespace `de-lab`

Nếu muốn xóa hoàn toàn Spark khỏi namespace, chạy các lệnh sau:

```bash
kubectl delete sparkapplication hagent-analytics-job -n de-lab --ignore-not-found
kubectl delete serviceaccount spark-operator-sa -n de-lab --ignore-not-found
kubectl delete role spark-operator-role -n de-lab --ignore-not-found
kubectl delete rolebinding spark-operator-role-binding -n de-lab --ignore-not-found
kubectl delete role spark-operator-controller-role -n de-lab --ignore-not-found
kubectl delete rolebinding spark-operator-controller-rolebinding -n de-lab --ignore-not-found
```

Kiểm tra lại:

```bash
kubectl get all -n de-lab | grep -i spark
kubectl get sa,role,rolebinding -n de-lab | grep -i spark
```

Nếu không còn output, nghĩa là Spark đã được gỡ bỏ hoàn toàn khỏi namespace `de-lab`.

## Kiểm tra Trino

```bash
# Port-forward coordinator
kubectl port-forward svc/trino-coordinator -n de-lab 8080:8080

# Kiểm tra catalogs
curl -sS -X POST "http://localhost:8080/v1/statement" -d "SHOW CATALOGS;"
curl -sS -X POST "http://localhost:8080/v1/statement" -d "SHOW SCHEMAS FROM hive;"
```

## DAG đồng bộ dữ liệu từ MSSQL sang MinIO
Điều kiện : tạo trước các bucket : datalake-processed, datalake-raw

File DAG đã tạo: `ingestion/airflow/hagent_callrec.py`

Mục tiêu:
- Đọc dữ liệu từ 2 bảng `call_rec` và `hagent` trong MSSQL.
- Lọc theo khoảng ngày truyền từ Trigger DAG (`start_date`, `end_date`).
- Chuyển đổi dữ liệu thành Parquet.
- Tạo cấu trúc partition theo kiểu Hive:
  - `s3://datalake-raw/call_rec/year=YYYY/month=MM/day=DD/data.parquet`
  - `s3://datalake-raw/hagent/year=YYYY/month=MM/day=DD/data.parquet`

DAG chính:

```python
TABLES = ["call_rec", "hagent"]
DATE_COLUMN = "row_date"

with DAG(
    dag_id="mssql_to_minio_manual_sync",
    description="Sync batch data from MSSQL DWH to MinIO with dynamic date parameters and Hive partitioning",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
) as dag:
    sync_task = PythonOperator(
        task_id="extract_mssql_load_minio",
        python_callable=extract_and_load_to_minio,
    )
```

Hàm chính xử lý extract + upload:

```python
for table_name in TABLES:
    sql = f"""
        SELECT * FROM {table_name}
        WHERE {DATE_COLUMN} >= %s AND {DATE_COLUMN} <= %s
    """

    df = pd.read_sql(sql, conn, params=(start_date, end_date))
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN]).dt.strftime('%Y-%m-%d')

    for date_str in df[DATE_COLUMN].unique():
        dt_obj = pendulum.parse(date_str)
        year = dt_obj.year
        month = f"{dt_obj.month:02d}"
        day = f"{dt_obj.day:02d}"

        file_path = f"{table_name}/year={year}/month={month}/day={day}/data.parquet"
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=file_path,
            Body=df_day.to_parquet(index=False)
        )
```

Trigger DAG:

```bash
airflow dags trigger mssql_to_minio_manual_sync   --conf '{"start_date":"2026-09-01","end_date":"2026-09-07"}'
```

Kiểm tra dữ liệu trên MinIO:

```bash
mc alias set myminio http://localhost:9000 minioadmin minioadmin123
mc ls --recursive myminio/datalake-raw
```

Mẫu cấu trúc thư mục sau khi load:

```text
datalake-raw/
├── call_rec/
│   └── year=2026/
│       └── month=09/
│           └── day=06/
│               └── data.parquet
├── hagent/
│   └── year=2026/
│       └── month=09/
│           └── day=06/
│               └── data.parquet
```

## Tạo schema và external table trên Hive / Trino

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM hive;

DROP SCHEMA IF EXISTS hive.datalake_raw CASCADE;
CREATE SCHEMA hive.datalake_raw;
```

Mẫu tạo external table cho `call_rec`:

```sql
CREATE TABLE IF NOT EXISTS hive.datalake_raw.call_rec (
    seqnum INTEGER,
    acd SMALLINT,
    row_date DATE,
    row_time SMALLINT,
    acwtime INTEGER,
    ansholdtime INTEGER,
    anslogin VARCHAR,
    assist SMALLINT,
    audio SMALLINT,
    callid INTEGER,
    calling_pty VARCHAR,
    conference SMALLINT,
    consulttime INTEGER,
    da_queued SMALLINT,
    dialed_num VARCHAR,
    dispivector SMALLINT,
    disposition SMALLINT,
    disppriority SMALLINT,
    dispsplit SMALLINT,
    disptime INTEGER,
    dispvdn VARCHAR,
    duration INTEGER,
    eqloc VARCHAR,
    event1 SMALLINT,
    event2 SMALLINT,
    event3 SMALLINT,
    event4 SMALLINT,
    event5 SMALLINT,
    event6 SMALLINT,
    event7 SMALLINT,
    event8 SMALLINT,
    event9 SMALLINT,
    firstivector SMALLINT,
    firstvdn VARCHAR,
    vdn2 VARCHAR,
    vdn3 VARCHAR,
    vdn4 VARCHAR,
    vdn5 VARCHAR,
    vdn6 VARCHAR,
    vdn7 VARCHAR,
    vdn8 VARCHAR,
    vdn9 VARCHAR,
    held SMALLINT,
    holdabn SMALLINT,
    lastcwc VARCHAR,
    lastdigits VARCHAR,
    lastobserver VARCHAR,
    malicious SMALLINT,
    observingcall SMALLINT,
    origlogin VARCHAR,
    segment SMALLINT,
    segstart INTEGER,
    segstart_utc INTEGER,
    segstop INTEGER,
    segstop_utc INTEGER,
    split1 SMALLINT,
    split2 SMALLINT,
    split3 SMALLINT,
    talktime INTEGER,
    tkgrp SMALLINT,
    transferred SMALLINT,
    agt_released SMALLINT,
    ansreason SMALLINT,
    calling_ii VARCHAR,
    dispsklevel SMALLINT,
    origreason SMALLINT,
    netintime INTEGER,
    origholdtime INTEGER,
    ucid VARCHAR,
    anslocid SMALLINT,
    eqlocid SMALLINT,
    obslocid SMALLINT,
    origlocid SMALLINT,
    cwc1 VARCHAR,
    cwc2 VARCHAR,
    cwc3 VARCHAR,
    cwc4 VARCHAR,
    cwc5 VARCHAR,
    queuetime INTEGER,
    ringtime INTEGER,
    uui_len SMALLINT,
    asai_uui VARCHAR,
    interruptdel SMALLINT,
    agentsurplus SMALLINT,
    agentskilllevel SMALLINT,
    prefskilllevel SMALLINT,
    icrresent SMALLINT,
    icrpullreason SMALLINT,
    orig_attrib_id VARCHAR,
    ans_attrib_id VARCHAR,
    obs_attrib_id VARCHAR,
    tenant INTEGER,
    ecd_num INTEGER,
    ecd_control SMALLINT,
    ecd_info SMALLINT,
    ecd_str VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-raw/call_rec/',
    partitioned_by = ARRAY['year', 'month', 'day']
);

CALL hive.system.sync_partition_metadata('datalake_raw', 'call_rec', 'ADD');
SELECT COUNT(*) FROM hive.datalake_raw.call_rec;
```

Mẫu tạo external table cho `hagent`:

```sql
CREATE TABLE IF NOT EXISTS hive.datalake_raw.hagent (
    id VARCHAR,
    abncalls SMALLINT,
    abntime SMALLINT,
    acceptedintrs SMALLINT,
    acd SMALLINT,
    acd_release INTEGER,
    acdauxoutcalls SMALLINT,
    acdcalls SMALLINT,
    acdcalls_r1 SMALLINT,
    acdcalls_r2 SMALLINT,
    acdtime SMALLINT,
    acwincalls SMALLINT,
    acwintime SMALLINT,
    acwoutadjcalls SMALLINT,
    acwoutcalls SMALLINT,
    acwoutoffcalls SMALLINT,
    acwoutofftime SMALLINT,
    acwouttime SMALLINT,
    acwtime SMALLINT,
    ansringtime SMALLINT,
    assists SMALLINT,
    attrib_id VARCHAR,
    auxincalls SMALLINT,
    auxintime SMALLINT,
    auxoutadjcalls SMALLINT,
    auxoutcalls SMALLINT,
    auxoutoffcalls SMALLINT,
    auxoutofftime SMALLINT,
    auxouttime SMALLINT,
    conference SMALLINT,
    da_abncalls SMALLINT,
    da_abntime SMALLINT,
    da_acdcalls SMALLINT,
    da_acdtime SMALLINT,
    da_acwincalls SMALLINT,
    da_acwintime SMALLINT,
    da_acwoadjcalls SMALLINT,
    da_acwocalls SMALLINT,
    da_acwooffcalls SMALLINT,
    da_acwoofftime SMALLINT,
    da_acwotime SMALLINT,
    da_acwtime SMALLINT,
    da_anstime SMALLINT,
    da_icrpullcalls INTEGER,
    da_icrpulltime INTEGER,
    da_othercalls SMALLINT,
    da_othertime SMALLINT,
    da_release INTEGER,
    event1 SMALLINT,
    event2 SMALLINT,
    event3 SMALLINT,
    event4 SMALLINT,
    event5 SMALLINT,
    event6 SMALLINT,
    event7 SMALLINT,
    event8 SMALLINT,
    event9 SMALLINT,
    extension VARCHAR,
    holdabncalls SMALLINT,
    holdacdtime INTEGER,
    holdcalls SMALLINT,
    holdtime SMALLINT,
    i_acdaux_outtime SMALLINT,
    i_acdauxintime SMALLINT,
    i_acdothertime SMALLINT,
    i_acdtime SMALLINT,
    i_acwintime SMALLINT,
    i_acwouttime SMALLINT,
    i_acwtime SMALLINT,
    i_auxintime SMALLINT,
    i_auxouttime SMALLINT,
    i_auxstbytime SMALLINT,
    i_auxtime INTEGER,
    i_availtime SMALLINT,
    i_da_acdtime SMALLINT,
    i_da_acwtime SMALLINT,
    i_otherstbytime SMALLINT,
    i_othertime SMALLINT,
    i_ringtime SMALLINT,
    i_stafftime SMALLINT,
    icrpullcalls INTEGER,
    icrpulltime INTEGER,
    incomplete SMALLINT,
    intrdeliveries SMALLINT,
    intrnotifies SMALLINT,
    intrvl SMALLINT,
    loc_id SMALLINT,
    logid VARCHAR,
    noansredir SMALLINT,
    o_acdcalls SMALLINT,
    o_acdtime SMALLINT,
    o_acwtime SMALLINT,
    partial_archive SMALLINT,
    phantomabns SMALLINT,
    rejectedintrs SMALLINT,
    ringcalls SMALLINT,
    ringtime SMALLINT,
    row_date TIMESTAMP,
    rsv_level SMALLINT,
    split SMALLINT,
    starttime SMALLINT,
    starttime_utc INTEGER,
    tenant INTEGER,
    ti_auxtime SMALLINT,
    ti_auxtime0 INTEGER,
    ti_auxtime1 INTEGER,
    ti_auxtime10 INTEGER,
    ti_auxtime11 INTEGER,
    ti_auxtime12 INTEGER,
    ti_auxtime13 INTEGER,
    ti_auxtime14 INTEGER,
    ti_auxtime15 INTEGER,
    ti_auxtime16 INTEGER,
    ti_auxtime17 INTEGER,
    ti_auxtime18 INTEGER,
    ti_auxtime19 INTEGER,
    ti_auxtime2 INTEGER,
    ti_auxtime20 INTEGER,
    ti_auxtime21 INTEGER,
    ti_auxtime22 INTEGER,
    ti_auxtime23 INTEGER,
    ti_auxtime24 INTEGER,
    ti_auxtime25 INTEGER,
    ti_auxtime26 INTEGER,
    ti_auxtime27 INTEGER,
    ti_auxtime28 INTEGER,
    ti_auxtime29 INTEGER,
    ti_auxtime3 INTEGER,
    ti_auxtime30 INTEGER,
    ti_auxtime31 INTEGER,
    ti_auxtime32 INTEGER,
    ti_auxtime33 INTEGER,
    ti_auxtime34 INTEGER,
    ti_auxtime35 INTEGER,
    ti_auxtime36 INTEGER,
    ti_auxtime37 INTEGER,
    ti_auxtime38 INTEGER,
    ti_auxtime39 INTEGER,
    ti_auxtime4 INTEGER,
    ti_auxtime40 INTEGER,
    ti_auxtime41 INTEGER,
    ti_auxtime42 INTEGER,
    ti_auxtime43 INTEGER,
    ti_auxtime44 INTEGER,
    ti_auxtime45 INTEGER,
    ti_auxtime46 INTEGER,
    ti_auxtime47 INTEGER,
    ti_auxtime48 INTEGER,
    ti_auxtime49 INTEGER,
    ti_auxtime5 INTEGER,
    ti_auxtime50 INTEGER,
    ti_auxtime51 INTEGER,
    ti_auxtime52 INTEGER,
    ti_auxtime53 INTEGER,
    ti_auxtime54 INTEGER,
    ti_auxtime55 INTEGER,
    ti_auxtime56 INTEGER,
    ti_auxtime57 INTEGER,
    ti_auxtime58 INTEGER,
    ti_auxtime59 INTEGER,
    ti_auxtime6 INTEGER,
    ti_auxtime60 INTEGER,
    ti_auxtime61 INTEGER,
    ti_auxtime62 INTEGER,
    ti_auxtime63 INTEGER,
    ti_auxtime64 INTEGER,
    ti_auxtime65 INTEGER,
    ti_auxtime66 INTEGER,
    ti_auxtime67 INTEGER,
    ti_auxtime68 INTEGER,
    ti_auxtime69 INTEGER,
    ti_auxtime7 INTEGER,
    ti_auxtime70 INTEGER,
    ti_auxtime71 INTEGER,
    ti_auxtime72 INTEGER,
    ti_auxtime73 INTEGER,
    ti_auxtime74 INTEGER,
    ti_auxtime75 INTEGER,
    ti_auxtime76 INTEGER,
    ti_auxtime77 INTEGER,
    ti_auxtime78 INTEGER,
    ti_auxtime79 INTEGER,
    ti_auxtime8 INTEGER,
    ti_auxtime80 INTEGER,
    ti_auxtime81 INTEGER,
    ti_auxtime82 INTEGER,
    ti_auxtime83 INTEGER,
    ti_auxtime84 INTEGER,
    ti_auxtime85 INTEGER,
    ti_auxtime86 INTEGER,
    ti_auxtime87 INTEGER,
    ti_auxtime88 INTEGER,
    ti_auxtime89 INTEGER,
    ti_auxtime9 INTEGER,
    ti_auxtime90 INTEGER,
    ti_auxtime91 INTEGER,
    ti_auxtime92 INTEGER,
    ti_auxtime93 INTEGER,
    ti_auxtime94 INTEGER,
    ti_auxtime95 INTEGER,
    ti_auxtime96 INTEGER,
    ti_auxtime97 INTEGER,
    ti_auxtime98 INTEGER,
    ti_auxtime99 INTEGER,
    ti_availtime SMALLINT,
    ti_othertime INTEGER,
    ti_stafftime SMALLINT,
    transferred SMALLINT,
    importsessionid VARCHAR,
    ignorereport BOOLEAN,
    agentname VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://datalake-raw/hagent/',
    partitioned_by = ARRAY['year', 'month', 'day']
);

CALL hive.system.sync_partition_metadata('datalake_raw', 'hagent', 'ADD');
SELECT COUNT(*) FROM hive.datalake_raw.hagent;
```

## Ví dụ truy vấn kết hợp dữ liệu

```sql
SELECT 
    h.agentname,
    h.logid,
    h.split,
    COUNT(c.callid) AS total_calls_in_call_rec,
    SUM(c.duration) AS total_duration_seconds,
    SUM(h.acdcalls) AS total_acd_calls_from_hagent,
    SUM(h.acdtime) AS total_acd_time_from_hagent
FROM hive.datalake_raw.hagent h
LEFT JOIN hive.datalake_raw.call_rec c
    ON h.logid = c.anslogin
   AND h.year = c.year
   AND h.month = c.month
   AND h.day = c.day
WHERE h.year = '2026'
  AND h.month = '09'
  AND h.day = '06'
GROUP BY h.agentname, h.logid, h.split;

```

* **Chú thích:** Truy vấn tổng hợp thông tin chi tiết từng agent, kết hợp số liệu thống kê tổng đài từ bảng `hagent` và dữ liệu đo lường cuộc gọi thực tế từ bảng `call_rec`.

```sql
SELECT 
    h.agentname,
    h.logid,
    SUM(h.acdtime) AS total_acd_time_hagent,
    SUM(c.duration) AS total_call_duration_rec,
    ROUND(
        CASE 
            WHEN SUM(h.acdtime) > 0 THEN (CAST(SUM(c.duration) AS DOUBLE) / SUM(h.acdtime)) * 100 
            ELSE 0 
        END, 2
    ) AS duration_vs_acd_ratio_percent
FROM hive.datalake_raw.hagent h
LEFT JOIN hive.datalake_raw.call_rec c 
    ON h.logid = c.anslogin 
   AND h.year = c.year 
   AND h.month = c.month 
   AND h.day = c.day
WHERE h.year = '2026' 
  AND h.month = '09' 
  AND h.day = '06'
GROUP BY h.agentname, h.logid;

```

* **Chú thích:** Tính tỷ lệ phần trăm giữa thời gian đàm thoại thực tế (`duration`) so với tổng thời gian ACD (`acdtime`) của từng agent để đánh giá hiệu suất làm việc.

```sql
SELECT 
    h.agentname,
    h.logid,
    h.split,
    SUM(h.acdcalls) AS acd_calls_hagent,
    COUNT(c.callid) AS actual_calls_call_rec,
    ABS(SUM(h.acdcalls) - COUNT(c.callid)) AS call_discrepancy
FROM hive.datalake_raw.hagent h
LEFT JOIN hive.datalake_raw.call_rec c 
    ON h.logid = c.anslogin 
   AND h.year = c.year 
   AND h.month = c.month 
   AND h.day = c.day
WHERE h.year = '2026' 
  AND h.month = '09' 
  AND h.day = '06'
GROUP BY h.agentname, h.logid, h.split
HAVING ABS(SUM(h.acdcalls) - COUNT(c.callid)) > 0
ORDER BY call_discrepancy DESC;

```

* **Chú thích:** Phát hiện và liệt kê các agent có sự chênh lệch (lệch số lượng cuộc gọi) giữa thống kê tổng đài (`acdcalls`) và bảng chi tiết cuộc gọi (`call_rec`), sắp xếp theo mức độ chênh lệch giảm dần.

```sql
SELECT 
    h.split,
    COUNT(DISTINCT h.logid) AS total_agents,
    SUM(h.acdcalls) AS total_split_acd_calls,
    SUM(h.acdtime) AS total_split_acd_time,
    COUNT(c.callid) AS total_split_call_rec_count,
    SUM(c.duration) AS total_split_call_duration
FROM hive.datalake_raw.hagent h
LEFT JOIN hive.datalake_raw.call_rec c 
    ON h.logid = c.anslogin 
   AND h.year = c.year 
   AND h.month = c.month 
   AND h.day = c.day
WHERE h.year = '2026' 
  AND h.month = '09' 
  AND h.day = '06'
GROUP BY h.split
ORDER BY total_split_acd_calls DESC;

```

* **Chú thích:** Tổng hợp các chỉ số hoạt động theo từng nhóm kỹ năng/tổng đài (`split`), giúp nhà quản lý theo dõi tải công việc ở cấp độ nhóm thay vì từng cá nhân.

```sql
SELECT 
    c.callid,
    c.anslogin,
    c.duration
FROM hive.datalake_raw.call_rec c
LEFT JOIN hive.datalake_raw.hagent h 
    ON c.anslogin = h.logid 
   AND c.year = h.year 
   AND c.month = h.month 
   AND c.day = h.day
WHERE c.year = '2026' 
  AND c.month = '09' 
  AND h.logid IS NULL;

```

* **Chú thích:** Truy vấn tìm các cuộc gọi "mồ côi" — nghĩa là có bản ghi chi tiết cuộc gọi trong `call_rec` nhưng mã nhân viên xử lý (`anslogin`) không khớp với bất kỳ agent nào ghi nhận trong bảng `hagent` vào ngày hôm đó.

Kết luận:

* Các câu query không gọi trực tiếp cột `row_date` trong phần `SELECT` hoạt động bình thường vì Trino chỉ lọc qua các phân vùng `year`, `month`, `day` ở tầng metadata.
* Lỗi kiểu dữ liệu `Unsupported Trino column type` chỉ xảy ra khi Trino phải quét vào nội dung file Parquet để đọc cột có kiểu định nghĩa bị lệch so với thực tế tệp.
* Việc loại bỏ cột `row_date` khỏi câu lệnh `SELECT` giúp tránh được xung đột kiểu dữ liệu giữa Parquet `STRING` và Metastore `DATE`.
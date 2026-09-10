# Trino Lab

## Mục tiêu

Bộ này triển khai Trino trong namespace `de-lab` để truy vấn dữ liệu từ PostgreSQL, Hive metastore và MinIO/S3.

Kiến trúc chuẩn trong project:

* Trino coordinator: xử lý query planning và metadata
* Trino worker: thực thi query và scan dữ liệu
* Catalog `postgresql`: truy cập dữ liệu PostgreSQL
* Catalog `hive`: truy cập dữ liệu trên MinIO/S3 thông qua Hive metastore
* Catalog `tpch`: dữ liệu mẫu chuẩn để kiểm tra Trino

---

## Chuẩn bị

Trước khi triển khai, đảm bảo file biến môi trường local đã có sẵn:

```bash
source ~/de-lab/.env.local

```

Các biến cần thiết thường gồm:

* `MINIO_ACCESS_KEY`
* `MINIO_SECRET_KEY`
* `POSTGRES_USER`
* `POSTGRES_PASSWORD`

---

## Tạo Secret cho Trino / MinIO

Nếu cần lưu credential ở dạng Secret, dùng pattern tương tự MinIO:

```bash
source ~/de-lab/.env.local

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: trino-credentials
  namespace: de-lab
type: Opaque
stringData:
  MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}
  MINIO_SECRET_KEY: ${MINIO_SECRET_KEY}
  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
EOF

```

> Dùng file local `~/de-lab/.env.local` để tránh hardcode secret vào Git.

---

## Tạo ConfigMap cho Trino

Theo chuẩn project, không để placeholder `${POSTGRES_USER}` hoặc `${MINIO_ACCESS_KEY}` trực tiếp trong manifest. Dùng file local `.env.local` và render bằng heredoc trước khi apply.

### 1) Tạo `trino-coordinator-catalog-config`

```bash
source ~/de-lab/.env.local

kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-coordinator-catalog-config
  namespace: de-lab
data:
  tpch.properties: |
    connector.name=tpch
    tpch.splits-per-node=4

  postgres.properties: |
    connector.name=postgresql
    connection-url=jdbc:postgresql://postgres-db:5432/labdb
    connection-user=${POSTGRES_USER}
    connection-password=${POSTGRES_PASSWORD}

  hive.properties: |
    connector.name=hive
    hive.metastore.uri=thrift://hive-metastore:9083
    hive.s3.endpoint=http://minio:9000
    hive.s3.aws-access-key=${MINIO_ACCESS_KEY}
    hive.s3.aws-secret-key=${MINIO_SECRET_KEY}
    hive.s3.path-style-access=true
    hive.non-managed-table-writes-enabled=true
    hive.allow-drop-table=true
EOF

```

### 2) Tạo `trino-worker-config`

```bash
source ~/de-lab/.env.local

kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-worker-config
  namespace: de-lab
data:
  node.properties: |
    node.environment=docker
    node.data-dir=/data/trino

  jvm.config: |
    -server
    -Xmx2G
    -XX:InitialRAMPercentage=80
    -XX:MaxRAMPercentage=80
    -XX:G1HeapRegionSize=32M
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+ExitOnOutOfMemoryError

  config.properties: |
    coordinator=false
    http-server.http.port=8080
    discovery.uri=http://trino-coordinator:8080
    internal-communication.shared-secret=chuoibimatdungchungcuaquy

  tpch.properties: |
    connector.name=tpch
    tpch.splits-per-node=4

  postgres.properties: |
    connector.name=postgresql
    connection-url=jdbc:postgresql://postgres-db:5432/labdb
    connection-user=${POSTGRES_USER}
    connection-password=${POSTGRES_PASSWORD}
EOF

```

> Sau khi tạo ConfigMap theo heredoc, nên xóa phần `data:` tương ứng khỏi manifest YAML nếu còn định nghĩa trong file `trino-coordinator.yaml` và `trino-workers.yaml` để tránh duplicate và placeholder. Manifest nên giữ tối đa các cấu hình nền tảng, còn secret/value thực tế nên inject bằng `kubectl apply -f -` như trên.

---

## Triển khai Trino

### 1) Triển khai coordinator

```bash
kubectl apply -f trino-k8s-lab/trino-coordinator.yaml

```

### 2) Triển khai workers

```bash
kubectl apply -f trino-k8s-lab/trino-workers.yaml

```

### 3) Kiểm tra

```bash
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep trino

```

---

## Truy cập UI

Trino coordinator exposes port `30088` theo kiểu `NodePort`:

```text
http://<IP_NODE>:30088

```

Nếu đang chạy trên máy local, có thể thay `<IP_NODE>` bằng `localhost` hoặc IP của node.

---

## Cấu hình catalog Hive

Cấu hình Hive trong `trino-coordinator.yaml` nên dùng các biến đã có trong file môi trường:

```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=${MINIO_ACCESS_KEY}
hive.s3.aws-secret-key=${MINIO_SECRET_KEY}
hive.s3.path-style-access=true
hive.non-managed-table-writes-enabled=true
hive.allow-drop-table=true

```

Nếu file config đang lưu dạng placeholder, có thể render trước khi apply:

```bash
source ~/de-lab/.env.local
envsubst < trino-k8s-lab/trino-coordinator.yaml | kubectl apply -f -

```

> Lưu ý: `envsubst` chỉ hoạt động khi file YAML chứa placeholder dạng `${VAR}`.

---

## Cấu hình catalog PostgreSQL

```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres-db:5432/labdb
connection-user=labuser
connection-password=labpassword

```

Trong môi trường thực tế, nên dùng Secret hoặc biến môi trường thay vì ghi password trực tiếp.

---

## Kiểm tra hoạt động

Sau khi các pod đã Ready, truy cập Trino UI và chạy query ví dụ:

```sql
SHOW CATALOGS;
SHOW TABLES FROM postgresql.public;
SHOW TABLES FROM hive.default;

```

Hoặc thử query trên PostgreSQL catalog:

```sql
SELECT * FROM postgresql.public.employees LIMIT 10;

```

---

## Khởi tạo Schema và Bảng dữ liệu

### 1) Tạo Schema và Bảng `call_rec`

```sql
CREATE SCHEMA IF NOT EXISTS hive.datalake_raw;

-- Tạo lại bảng call_rec
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

-- Đồng bộ phân vùng từ MinIO
CALL hive.system.sync_partition_metadata('datalake_raw', 'call_rec', 'ADD');

-- Kiểm tra dữ liệu
SELECT COUNT(*) FROM hive.datalake_raw.call_rec;

```

---

### 2) Tạo Bảng `hagent`

```sql
DROP TABLE IF EXISTS hive.datalake_raw.hagent;

CREATE TABLE hive.datalake_raw.hagent (
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
    intervl SMALLINT,
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

-- Đồng bộ phân vùng từ MinIO
CALL hive.system.sync_partition_metadata('datalake_raw', 'hagent', 'ADD');

-- Truy vấn kiểm tra
SELECT COUNT(*) FROM hive.datalake_raw.hagent;

```

---

## Lưu ý

* Không hardcode secret vào Git.
* Dùng file local `~/de-lab/.env.local` và Secret theo pattern `kubectl apply -f -`.
* Nếu Hive chưa sẵn sàng, `hive.properties` có thể để trống trước khi metastore đã chạy.
* Trino chỉ hoạt động ổn định khi MinIO, PostgreSQL và Hive metastore đã sẵn sàng.
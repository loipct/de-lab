# Trino lab

## Mục tiêu

Bộ này triển khai Trino trong namespace `de-lab` để truy vấn dữ liệu từ PostgreSQL, Hive metastore và MinIO/S3.

Kiến trúc chuẩn trong project:

- Trino coordinator: xử lý query planning và metadata
- Trino worker: thực thi query và scan dữ liệu
- Catalog `postgresql`: truy cập dữ liệu PostgreSQL
- Catalog `hive`: truy cập dữ liệu trên MinIO/S3 thông qua Hive metastore
- Catalog `tpch`: dữ liệu mẫu chuẩn để kiểm tra Trino

## Chuẩn bị

Trước khi triển khai, đảm bảo file biến môi trường local đã có sẵn:

```bash
source ~/de-lab/.env.local
```

Các biến cần thiết thường gồm:

- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

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

## Truy cập UI

Trino coordinator exposes port `30088` theo kiểu `NodePort`:

```bash
http://<IP_NODE>:30088
```

Nếu đang chạy trên máy local, có thể thay `<IP_NODE>` bằng `localhost` hoặc IP của node.

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

## Cấu hình catalog PostgreSQL

```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres-db:5432/labdb
connection-user=labuser
connection-password=labpassword
```

Trong môi trường thực tế, nên dùng Secret hoặc biến môi trường thay vì ghi password trực tiếp.

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

## Lưu ý

- Không hardcode secret vào Git.
- Dùng file local `~/de-lab/.env.local` và Secret theo pattern `kubectl apply -f -`.
- Nếu Hive chưa sẵn sàng, `hive.properties` có thể để trống trước khi metastore đã chạy.
- Trino chỉ hoạt động ổn định khi MinIO, PostgreSQL và Hive metastore đã sẵn sàng.

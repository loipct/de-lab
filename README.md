# Hướng dẫn triển khai (MinIO → Hive → Trino)

Tóm tắt: file này hướng dẫn triển khai theo thứ tự MinIO, Hive metastore, rồi Trino (Postgres + coordinator + workers).

Yêu cầu:
- `kubectl` đã cấu hình tới cluster.
- Namespace mặc định trong các manifest là `de-lab`.

1) Triển khai MinIO

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

2) Triển khai Hive metastore

```bash
kubectl apply -f hive-k8s-lab/hive-lab.yaml

# Kiểm tra
kubectl get pods -n de-lab
kubectl logs -f <hive-metastore-pod> -n de-lab
```

Ghi chú: Hive metastore thường dùng Thrift trên cổng `9083`.

3) Triển khai Postgres + Trino

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

4) Triển khai Trino

```bash
kubectl apply -f trino-k8s-lab/trino-coordinator.yaml
kubectl apply -f trino-k8s-lab/trino-workers.yaml

# Kiểm tra
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep trino
```

5) Cập nhật `hive.properties` cho Trino (sau khi MinIO + Hive sẵn sàng)

Mở `trino-k8s-lab/trino-coordinator.yaml` và cập nhật `hive.properties` catalog với các giá trị:

```
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=<MINIO_ACCESS_KEY>
hive.s3.aws-secret-key=<MINIO_SECRET_KEY>
hive.s3.path-style-access=true
```

6) Kiểm tra Trino

```bash
# Port-forward coordinator
kubectl port-forward svc/trino-coordinator -n de-lab 8080:8080

# Kiểm tra catalogs
curl -sS -X POST "http://localhost:8080/v1/statement" -d "SHOW CATALOGS;"
```


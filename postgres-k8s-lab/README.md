# PostgreSQL lab

## Mục tiêu

Bộ này triển khai một instance PostgreSQL trong namespace `de-lab` để làm nguồn dữ liệu quan hệ cho hệ thống data lake / lakehouse. PostgreSQL ở đây phục vụ hai mục đích chính:

- lưu trữ dữ liệu nghiệp vụ cho các ứng dụng ETL / analytics
- làm database cho Hive metastore và các schema metadata của hệ thống dữ liệu

Cấu trúc thường dùng trong dự án:

- `labdb`: database nghiệp vụ, dùng cho các dataset mẫu và truy vấn phân tích
- `metastore_db`: database dành cho Hive metastore
- `labuser`: user đọc dữ liệu từ `labdb`
- `hive`: user quản trị metastore, có quyền tạo schema và cấu trúc dữ liệu trong `metastore_db`

## Chuẩn bị

Trước khi triển khai, đảm bảo cluster Kubernetes đã sẵn sàng và namespace `de-lab` tồn tại:

```bash
kubectl create namespace de-lab
```

Nếu namespace chưa có, có thể deploy manifest luôn; file `postgres.yaml` đã có khai báo `Namespace` và `StorageClass` bên trong.

## Chuẩn bị Secret PostgreSQL

Giữ password ở local file `~/de-lab/.env.local`, không commit vào Git. Thao tác tương tự MinIO:

```bash
source ~/de-lab/.env.local

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: de-lab
type: Opaque
stringData:
  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  POSTGRES_HIVE_USER: ${POSTGRES_HIVE_USER}
  POSTGRES_HIVE_PASSWORD: ${POSTGRES_HIVE_PASSWORD}
EOF
```

Nếu file `.env.local` chưa có các biến này, hãy thêm vào trước khi chạy:

```bash
cat <<EOF >> ~/de-lab/.env.local
POSTGRES_USER=labuser
POSTGRES_PASSWORD=labpassword
POSTGRES_HIVE_USER=hive
POSTGRES_HIVE_PASSWORD=hive
EOF
```

## Triển khai PostgreSQL

```bash
kubectl apply -f postgres-k8s-lab/postgres.yaml
```

Kiểm tra trạng thái:

```bash
kubectl get pods -n de-lab
kubectl get svc -n de-lab
```

Nếu cần truy cập trực tiếp từ local:

```bash
kubectl port-forward svc/postgres-db -n de-lab 5432:5432
```

Sau đó connect bằng client local:

```bash
psql -h localhost -U labuser -d labdb
```

## Vai trò và chức năng của các user

### 1) `postgres`
- Là superuser admin mặc định của PostgreSQL
- Dùng để tạo user, database, schema và cấp quyền
- Không nên dùng cho ứng dụng nghiệp vụ

### 2) `labuser`
- Là user dùng cho truy vấn dữ liệu phân tích
- Thường chỉ cần quyền `CONNECT`, `USAGE`, và `SELECT`
- Dùng cho Trino / reporting / BI / notebook đọc dữ liệu
- Không nên cấp full admin trên database production

### 3) `hive`
- Là user quản trị metastore
- Dùng cho Hive metastore và ghi metadata vào `metastore_db`
- Thường cần quyền tạo schema, bảng, sequence, và chỉnh sửa đối tượng metadata

## Tạo user, database, schema và quyền

Vào shell trong pod trước:

```bash
kubectl exec -it deploy/postgres-db -n de-lab -- sh
```

Sau đó chạy các lệnh cần thiết:

```bash
export PGPASSWORD="$POSTGRES_PASSWORD"
psql -U "$POSTGRES_USER" -d postgres
```

Trong shell `psql`, gõ trực tiếp:

```sql
CREATE ROLE hive LOGIN PASSWORD 'hive';
CREATE DATABASE metastore_db OWNER hive;
```

Nếu cần tạo schema và quyền thêm:

```sql
\c labdb
CREATE SCHEMA IF NOT EXISTS raw;
GRANT CONNECT ON DATABASE labdb TO labuser;
GRANT USAGE ON SCHEMA public TO labuser;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO labuser;

\c metastore_db
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
GRANT USAGE ON SCHEMA public TO hive;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO hive;
```

## Kết luận

PostgreSQL trong project này đóng vai trò là tầng metadata và dữ liệu quan hệ nền tảng. `labuser` phù hợp cho truy vấn analytics, còn `hive` phù hợp cho metastore và các schema metadata. Việc phân chia schema rõ ràng (`raw`, `curated`, `analytics`) giúp hệ thống data lake dễ mở rộng, dễ quản lý và dễ cấp quyền theo từng nhóm người dùng.

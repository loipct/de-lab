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
kubectl get svc -n de-lab | grep trino
```

4) Cập nhật `hive.properties` cho Trino (sau khi MinIO + Hive sẵn sàng)

Mở `trino-k8s-lab/trino-coordinator.yaml` và điền các giá trị Hive/MinIO:

```
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=<MINIO_ACCESS_KEY>
hive.s3.aws-secret-key=<MINIO_SECRET_KEY>
hive.s3.path-style-access=true
```

5) Truy cập Trino và thử truy vấn

```bash
# Port-forward coordinator
kubectl port-forward svc/trino-coordinator -n de-lab 8080:8080

# Kiểm tra catalog
curl -sS -X POST "http://localhost:8080/v1/statement" -d "SHOW CATALOGS;"
```

# Hive lab

Thư mục này chứa manifest để triển khai Hive Metastore và HiveServer2 trong namespace `de-lab`.

Mục tiêu của lab là:
- quản lý metadata của data lake qua Hive Metastore
- lưu trữ dữ liệu warehouse và dữ liệu raw trên MinIO
- phục vụ truy vấn và ETL bằng Trino / Spark
- tách riêng secret và biến môi trường ra khỏi YAML để tránh commit password vào repo

## Kiến trúc

- MinIO: lưu trữ dữ liệu S3-compatible
- PostgreSQL: lưu metadata metastore Hive
- Hive Metastore: quản lý schema và bảng
- HiveServer2: cung cấp giao diện SQL cho Hive
- Trino: đọc dữ liệu từ Hive / PostgreSQL / MinIO

## Yêu cầu

- Cluster Kubernetes đã sẵn sàng
- `kubectl` đã cấu hình đúng tới cluster
- Namespace `de-lab` đã tồn tại
- File local `~/de-lab/.env.local` đã được tạo và chứa các biến cần thiết

```bash
source ~/de-lab/.env.local
```

Các biến quan trọng thường gồm:

```bash
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
POSTGRES_HIVE_USER=
POSTGRES_HIVE_PASSWORD=
```

## Tạo ConfigMap `hive-core-site`

Theo chuẩn project, không để placeholder `${MINIO_ACCESS_KEY}` hoặc `${MINIO_SECRET_KEY}` trực tiếp trong file YAML. Dùng `source ~/de-lab/.env.local` rồi tạo `ConfigMap` bằng heredoc với giá trị thực.

```bash
source ~/de-lab/.env.local

kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: hive-core-site
  namespace: de-lab
data:
  core-site.xml: |
    <configuration>
        <property>
            <name>fs.s3a.endpoint</name>
            <value>http://minio:9000</value>
        </property>
        <property>
            <name>fs.s3a.access.key</name>
            <value>${MINIO_ACCESS_KEY}</value>
        </property>
        <property>
            <name>fs.s3a.secret.key</name>
            <value>${MINIO_SECRET_KEY}</value>
        </property>
        <property>
            <name>fs.s3a.path.style.access</name>
            <value>true</value>
        </property>
        <property>
            <name>fs.s3a.connection.ssl.enabled</name>
            <value>false</value>
        </property>
        <property>
            <name>fs.s3a.impl</name>
            <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
        </property>
    </configuration>
EOF
```

> Sau khi tạo bằng heredoc, nên xóa phần `ConfigMap` tương ứng khỏi file manifest nếu còn tồn tại, để tránh duplicate hoặc placeholder trong YAML.

## Chuẩn secret trong project

Không hardcode password trong YAML. Mỗi service nên lấy giá trị từ Secret hoặc từ biến môi trường đã được source từ file local.

Ví dụ tạo Secret cho Hive:

```bash
source ~/de-lab/.env.local

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: hive-credentials
  namespace: de-lab
type: Opaque
stringData:
  MINIO_ACCESS_KEY: "${MINIO_ACCESS_KEY}"
  MINIO_SECRET_KEY: "${MINIO_SECRET_KEY}"
  POSTGRES_HIVE_USER: "${POSTGRES_HIVE_USER}"
  POSTGRES_HIVE_PASSWORD: "${POSTGRES_HIVE_PASSWORD}"
EOF
```

Nếu manifest đang đọc từ các biến môi trường trong pod, hãy đảm bảo pod đã có env value tương ứng.

## Triển khai Hive

```bash
kubectl apply -f hive-k8s-lab/hive-lab.yaml
```

Kiểm tra:

```bash
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep hive
kubectl logs -f deploy/hive-metastore -n de-lab
```

Các service chính:
- `hive-metastore`: port `9083`
- `hiveserver2`: port `10000`, `10002`

## Kiểm tra Hive metastore

Có thể test bằng cách exec vào pod và kiểm tra trạng thái metastore:

```bash
kubectl exec -it deploy/hive-metastore -n de-lab -- sh
```

Trong shell, có thể dùng `beeline` hoặc kiểm tra log nếu cần troubleshoot.

## Ghi chú quan trọng

- Hive metastore cần thông tin PostgreSQL đúng username/password.
- MinIO credentials phải khớp với giá trị đã tạo trong MinIO.
- Nếu dữ liệu hoặc phiên bản cũ còn tồn tại trên PVC, có thể phải xoá volume hoặc recreate deployment để tránh conflict schema / metadata cũ.
- Không commit file `.env.local` vào Git. Chỉ lưu ở local máy.

## Xóa lab

```bash
kubectl delete -f hive-k8s-lab/hive-lab.yaml -n de-lab
```

Nếu cần xoá cả Secret:

```bash
kubectl delete secret hive-credentials -n de-lab
```

## Tài liệu liên quan

- [README.md](../README.md)
- [minio-k8s-lab/README.md](../minio-k8s-lab/README.md)
- [postgres-k8s-lab/README.md](../postgres-k8s-lab/README.md)
- [trino-k8s-lab/README.md](../trino-k8s-lab/README.md)

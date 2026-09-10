# Dự án Data Lake CSKH

Dự án này xây dựng một kiến trúc lakehouse cho dữ liệu chăm sóc khách hàng (CSKH), tập trung vào lưu trữ, đồng bộ hóa, truy vấn và xử lý dữ liệu từ nhiều nguồn khác nhau như call log, agent activity, ticket, CRM và dữ liệu tương tác khách hàng.

## Kiến trúc tổng quan

- MinIO: kho dữ liệu raw / object storage dạng S3-compatible
- Airflow: điều phối ingest dữ liệu
- PostgreSQL: metadata và dữ liệu nghiệp vụ / metastore
- Hive Metastore: quản lý metadata schema và external tables
- Trino: truy vấn nhanh trên data lake
- Spark: xử lý ETL / chuẩn hóa / tạo curated layer

## Luồng cài đặt theo thứ tự

Dưới đây là trình tự triển khai hợp lý cho lab này. Mỗi bước chỉ nên chạy khi điều kiện ở bước trước đã đạt yêu cầu.

1. Chuẩn bị môi trường
   - Điều kiện bắt buộc:
     - Kubernetes cluster đã sẵn sàng
     - `kubectl` đã được cấu hình đúng với cluster
     - namespace `de-lab` tồn tại
     - file local `~/de-lab/.env.local` đã có giá trị cho MinIO, PostgreSQL, Hive, Trino
   - Thực hiện:
     - `source ~/de-lab/.env.local`
     - `kubectl get ns de-lab`

2. Cài đặt MinIO
   - Điều kiện bắt buộc:
     - namespace `de-lab` sẵn sàng
     - biến `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` đã được load từ `.env.local`
   - Mục tiêu:
     - khởi tạo object storage S3-compatible
     - tạo bucket / tài khoản truy cập cho Hive và Trino

3. Cài đặt PostgreSQL
   - Điều kiện bắt buộc:
     - MinIO đang chạy để dùng làm backend dữ liệu / object storage nếu cần test kết nối
     - biến `POSTGRES_USER`, `POSTGRES_PASSWORD` đã có trong `.env.local`
   - Mục tiêu:
     - tạo database metadata / dữ liệu nghiệp vụ
     - tạo user cho ứng dụng như `labuser`, `hive`

4. Cài đặt Hive Metastore
   - Điều kiện bắt buộc:
     - PostgreSQL đã sẵn sàng và user/database cho metastore đã được tạo
     - MinIO đã chạy và có credentials hợp lệ
     - file cấu hình `core-site.xml` được render từ `.env.local`
   - Mục tiêu:
     - khởi tạo metastore
     - đăng ký schema, external table, định nghĩa truy cập S3

5. Cài đặt Trino
   - Điều kiện bắt buộc:
     - Hive metastore đã ready
     - MinIO đang hoạt động
     - catalog `hive` và `postgresql` đã được render từ file env local
   - Mục tiêu:
     - truy vấn dữ liệu từ Hive, PostgreSQL
     - tạo catalog `hive`, `postgresql`, `tpch`

6. Cài đặt Spark
   - Điều kiện bắt buộc:
     - namespace `de-lab` đang hoạt động
     - Spark Operator / webhook đã được cài đặt đúng namespace
     - credentials S3 đã sẵn sàng trong biến môi trường local
   - Mục tiêu:
     - chạy ETL / chuẩn hóa / xử lý dữ liệu theo SparkApplication

7. Tạo schema / bảng dữ liệu
   - Điều kiện bắt buộc:
     - Hive metastore đã disponível
     - MinIO bucket đã được tạo và có dữ liệu raw
     - Trino đã có catalog `hive` và `postgresql`
   - Mục tiêu:
     - tạo schema `hive.datalake_raw`
     - tạo các bảng dữ liệu như `call_rec`, `hagent`
     - sync metadata và kiểm tra query

8. Chạy ingest / validation
   - Điều kiện bắt buộc:
     - dữ liệu raw đã có trong MinIO hoặc nguồn bên ngoài đã được tích hợp
     - Airflow / pipeline đã được cấu hình đúng
   - Mục tiêu:
     - ingest dữ liệu từ nguồn CSKH về lakehouse
     - kiểm tra khả năng query, ETL và dashboard/reporting

## Tài liệu chi tiết theo thành phần

- [minio-k8s-lab/README.md](minio-k8s-lab/README.md)
- [postgres-k8s-lab/README.md](postgres-k8s-lab/README.md)
- [hive-k8s-lab/README.md](hive-k8s-lab/README.md)
- [trino-k8s-lab/README.md](trino-k8s-lab/README.md)
- [spark-k8s-lab/README.md](spark-k8s-lab/README.md)
- [ingestion/airflow/hagent_callrec.py](ingestion/airflow/hagent_callrec.py)

## Mục tiêu của project

- tích hợp dữ liệu CSKH từ nhiều nguồn
- lưu trữ raw data an toàn và mở rộng dễ dàng
- chuẩn hóa dữ liệu để hỗ trợ báo cáo và analytics
- cho phép truy vấn nhanh qua Trino / Hive
- tạo nền tảng cho các layer curated và BI trong tương lai

## Yêu cầu chung

- Kubernetes cluster đã sẵn sàng
- `kubectl` đã được cấu hình đúng
- namespace `de-lab` được sử dụng cho lab hiện tại
- file local `~/de-lab/.env.local` chứa các biến môi trường cần thiết cho MinIO / PostgreSQL / Hive / Trino

## Ghi chú

README tổng này chỉ cung cấp overview và điều hướng. Chi tiết triển khai, cấu hình và troubleshooting đã được tách vào các README theo từng thành phần để dễ quản lý và bảo trì hơn.
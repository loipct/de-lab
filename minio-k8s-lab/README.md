# MinIO lab

## Mục tiêu
Bộ này khởi tạo MinIO trong namespace `de-lab` để phục vụ làm object storage cho data lake.

## Chuẩn bị

Đầu tiên hãy tạo namespace `de-lab`:

```bash
kubectl create ns de-lab
```

Sau đó load biến môi trường riêng cho project:

```bash
source ~/de-lab/.env.local
```

> File `~/de-lab/.env.local` là file local của Linux, không commit vào Git. Nó chứa các biến như `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` cho project này.

## Triển khai

1. Source biến môi trường project và tạo Secret bằng heredoc:

```bash
source ~/de-lab/.env.local

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: minio-credentials
  namespace: de-lab
type: Opaque
stringData:
  MINIO_ROOT_USER: ${MINIO_ROOT_USER}
  MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
EOF
```

2. Triển khai MinIO:

```bash
kubectl apply -f minio-k8s-lab/minio-lab.yaml
```

3. Tạo Access/Secret MinIO (chỉ MinIO)

```bash
source ~/de-lab/.env.local

kubectl exec -it deploy/minio -n de-lab -- sh -lc "mc alias set myminio http://localhost:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} && mc admin user svcacct add myminio minioadmin"
```

> Lệnh trên chạy trực tiếp bên trong container MinIO, vừa đăng ký alias vừa tạo service account `minioadmin` cho hệ thống.

4. Lưu access/secret vào file `~/de-lab/.env.local` nếu bạn muốn dùng cho các job khác:

```bash
cat <<EOF >> ~/de-lab/.env.local
MINIO_ACCESS_KEY=<access_key_từ_mc>
MINIO_SECRET_KEY=<secret_key_từ_mc>
EOF
```

5. Kiểm tra:

```bash
kubectl get pods -n de-lab
kubectl get svc -n de-lab | grep minio
```

## Truy cập MinIO
Manifest hiện tại đã cấu hình `Service` kiểu `NodePort`, nên MinIO có thể truy cập qua các port sau:

- S3 API: `http://<IP_NODE>:30900`
- Console: `http://<IP_NODE>:30901`

Nếu đang chạy trên máy local, có thể lấy IP của node tự động và test bằng curl như sau:

```bash
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')

echo "Node IP: $NODE_IP"
curl -I "http://$NODE_IP:30900"
curl -I "http://$NODE_IP:30901"
```

Nếu cần truy cập bằng địa chỉ cố định local:

```bash
# S3 API
http://localhost:30900

# Console
http://localhost:30901
```

## Thông tin đăng nhập
- User: `${MINIO_ROOT_USER}`
- Password: `${MINIO_ROOT_PASSWORD}`

> Secret đã được lưu trong `minio-k8s-lab/secrets/minio-secret.yaml`, và file này không commit vào Git.

## Lưu ý quan trọng
- Biến môi trường được giữ ở file local `~/de-lab/.env.local`.
- Manifest chính không chứa password.
- Chỉ cần `source` biến rồi `kubectl apply -f minio-k8s-lab/secrets/minio-secret.yaml` là đủ.

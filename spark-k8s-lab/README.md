
### Bước 3: Cài đặt Spark Operator trực tiếp vào namespace `de-lab`

Sử dụng Helm để cài đặt Spark Operator và cấu hình để nó chỉ quản lý/lắng nghe sự kiện trong namespace `de-lab` (bằng tham số `--set operatorNamespace=de-lab` hoặc `--set watchedNamespaces=de-lab` tùy phiên bản chart):

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace de-lab \
  --set webhook.enable=true \
  --set watchedNamespaces={de-lab}

```

---

### Bước 4: Tạo Phân quyền RBAC cho Spark Job trong `de-lab`

Tạo file `spark-rbac.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark-operator-sa
  namespace: de-lab
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-operator-role
  namespace: de-lab
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps", "secrets"]
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-operator-role-binding
  namespace: de-lab
subjects:
- kind: ServiceAccount
  name: spark-operator-sa
  namespace: de-lab
roleRef:
  kind: Role
  name: spark-operator-role
  apiGroup: rbac.authorization.k8s.io

```

Apply file RBAC vào namespace `de-lab`:

```bash
kubectl apply -f spark-rbac.yaml -n de-lab

```

---

### Bước 5: Triển khai Spark Job (`spark-application.yaml`)

Tạo file `spark-application.yaml`:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: hagent-analytics-job
  namespace: de-lab
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "your-registry/spark-analytics:v1"
  imagePullPolicy: IfNotPresent
  mainApplicationFile: "local:///app/job_analytics.py"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: OnFailure
    maxRetries: 2
    retryInterval: 10
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1024m"
    serviceAccount: spark-operator-sa
  executor:
    cores: 2
    instances: 3
    memory: "2048m"

```

Apply job lên namespace `de-lab`:

```bash
kubectl apply -f spark-application.yaml -n de-lab

```

---

### Bước 6: Kiểm tra mọi thứ trong `de-lab`

* Kiểm tra xem Spark Operator và các thành phần đã chạy chưa:
```bash
kubectl get all -n de-lab

```


* Kiểm tra trạng thái chạy của Spark Job:
```bash
kubectl get sparkapplications -n de-lab

```


* Xem log của Driver Job:
```bash
kubectl logs -f hagent-analytics-job-driver -n de-lab

```
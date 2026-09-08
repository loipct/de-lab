import logging
import io
from datetime import timedelta
import pendulum
import pandas as pd
import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# ==============================================================================
# CONFIG
# ==============================================================================

MSSQL_CONN_ID = "mssql_dwh_conn"
TABLES = ["call_rec", "hagent"]  # Danh sách các bảng cần đồng bộ
DATE_COLUMN = "row_date"

MINIO_ENDPOINT = "http://IP_NODE:30900"
MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY"
MINIO_SECRET_KEY = "MINIO_SECRET_KEY"
MINIO_BUCKET = "datalake-raw"

# ==============================================================================
# TASK FUNCTIONS
# ==============================================================================

def extract_and_load_to_minio(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    
    # Lấy start_date và end_date từ giao diện Trigger DAG w/ config, nếu không có mặc định lấy hôm qua
    default_date = pendulum.yesterday("Asia/Ho_Chi_Minh").date()
    start_date = conf.get("start_date", str(default_date))
    end_date = conf.get("end_date", str(default_date))
    
    logging.info(f"Extracting data from MSSQL for range: {start_date} to {end_date}")
    
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    for table_name in TABLES:
        logging.info(f"Processing table: {table_name}")
        sql = f"""
            SELECT * FROM {table_name}
            WHERE {DATE_COLUMN} >= %s AND {DATE_COLUMN} <= %s
        """
        
        with mssql_hook.get_conn() as conn:
            df = pd.read_sql(sql, conn, params=(start_date, end_date))
            
        if df.empty:
            logging.info(f"No data found for table {table_name} in the given date range.")
            continue
            
        # Chuẩn hóa cột ngày tháng về chuỗi YYYY-MM-DD để tách partition chuẩn Hive-style
        df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN]).dt.strftime('%Y-%m-%d')
        unique_dates = df[DATE_COLUMN].unique()
        
        logging.info(f"Fetched {len(df)} rows for {table_name}. Uploading partitioned files to MinIO...")
        
        for date_str in unique_dates:
            df_day = df[df[DATE_COLUMN] == date_str]
            if df_day.empty:
                continue
                
            # Tách năm, tháng, ngày từ chuỗi YYYY-MM-DD
            dt_obj = pendulum.parse(date_str)
            year = dt_obj.year
            month = f"{dt_obj.month:02d}"  # Format 2 chữ số: 01, 02...
            day = f"{dt_obj.day:02d}"      # Format 2 chữ số: 01, 02...
                
            parquet_buffer = io.BytesIO()
            df_day.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Cấu trúc đường dẫn phân cấp chuẩn Data Lake lớn
            file_path = f"{table_name}/year={year}/month={month}/day={day}/data.parquet"
            
            s3_client.put_object(
                Bucket=MINIO_BUCKET,
                Key=file_path,
                Body=parquet_buffer.getvalue()
            )
            logging.info(f"Successfully uploaded {len(df_day)} rows to s3://{MINIO_BUCKET}/{file_path}")
# ==============================================================================
# DAG DEFINITION
# ==============================================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mssql_to_minio_manual_sync",
    description="Sync batch data from MSSQL DWH to MinIO with dynamic date parameters and Hive partitioning",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["mssql", "minio", "batch"],
) as dag:

    sync_task = PythonOperator(
        task_id="extract_mssql_load_minio",
        python_callable=extract_and_load_to_minio,
    )
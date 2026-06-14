"""
test_api_pipeline.py
====================
DAG test: Lấy data từ S3 /feature-store và gọi API Gateway.

Flow:
  1. load_feature_data  → Lấy file parquet mới nhất từ S3 /feature-store/
  2. call_fraud_api     → Gọi API POST với request body đúng format model
  3. verify_response    → Kiểm tra response trả về thành công

Request body format:
  {
    "request_data": [
      {
        "Time": <float>,   ← dùng hour_of_day vì đây là proxy cho Time trong Kaggle
        "Amount": <float>,
        "V1": ..., "V2": ..., ..., "V28": ...
      },
      ...
    ]
  }

Lưu ý:
  - is_fraud_label bị DROP trước khi gửi
  - Các feature engineering (amount_zscore, tx_count_1h...) bị DROP
    vì model Kaggle chỉ nhận đúng Time + Amount + V1..V28
  - Kết quả API KHÔNG lưu vào S3 (chỉ test)
"""

import os
import io
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from alert_utils import send_alert, airflow_failure_callback

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET       = os.environ.get("S3_BUCKET", "")
S3_FEATURE      = os.environ.get("S3_FEATURE_PREFIX", "feature-store")
AWS_REGION      = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
API_GATEWAY_URL = os.environ.get("API_GATEWAY_URL", "")
API_USERNAME    = os.environ.get("API_GATEWAY_USERNAME", "admin")
API_PASSWORD    = os.environ.get("API_GATEWAY_PASSWORD", "")

# Các cột model Kaggle nhận vào — đúng theo request body format
MODEL_FEATURE_COLS = ["Time", "Amount"] + [f"V{i}" for i in range(1, 29)]

# Các cột cần DROP trước khi gửi API
# (label + feature engineering không có trong model gốc)
COLS_TO_DROP = [
    "is_fraud_label",           # label — tuyệt đối không gửi
    "transaction_id",           # metadata
    "user_id", "card_id",       # metadata
    "timestamp",                # metadata
    "merchant_id",              # metadata
    "merchant_category",        # metadata
    "amount_normalized",        # feature engineering
    "amount_log1p",             # feature engineering
    "amount_zscore",            # feature engineering
    "tx_count_1h",              # feature engineering
    "is_night_hour",            # feature engineering
    "is_high_amount",           # feature engineering
    "is_international",         # không có trong model Kaggle
    "hour_of_day",              # sẽ được map sang "Time" riêng
]

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "mlops-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": airflow_failure_callback,
}

dag = DAG(
    dag_id="test_api_pipeline",
    description="Test DAG: Load feature-store → gọi API Gateway fraud detection",
    schedule=None,          # Chỉ trigger thủ công
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fraud-detection", "test", "api"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def get_latest_feature_file() -> tuple[str, str]:
    """
    Tìm file features.parquet mới nhất trong S3 /feature-store/.
    Returns: (s3_key, batch_id)
    """
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_FEATURE}/")
    objects = resp.get("Contents", [])

    if not objects:
        raise RuntimeError(f"No objects found in s3://{S3_BUCKET}/{S3_FEATURE}/")

    # Lọc chỉ file .parquet
    parquet_objects = [o for o in objects if o["Key"].endswith(".parquet")]
    if not parquet_objects:
        raise RuntimeError(f"No parquet files in s3://{S3_BUCKET}/{S3_FEATURE}/")

    # Lấy file mới nhất theo LastModified
    latest = max(parquet_objects, key=lambda o: o["LastModified"])
    s3_key = latest["Key"]

    # Trích batch_id từ path: feature-store/{batch_id}/features.parquet
    parts = s3_key.split("/")
    batch_id = parts[1] if len(parts) >= 2 else "unknown"

    logger.info(f"Latest feature file: s3://{S3_BUCKET}/{s3_key} (batch: {batch_id})")
    return s3_key, batch_id


def build_request_body(df: pd.DataFrame, limit: int = None) -> dict:
    """
    Chuyển DataFrame thành request body đúng format model.

    Mapping:
      hour_of_day → Time   (proxy cho Time trong Kaggle dataset)
      amount      → Amount
      V1..V28     → V1..V28

    Args:
        df   : DataFrame từ feature-store
        limit: Giới hạn số rows gửi (None = gửi tất cả)

    Returns:
        {"request_data": [...]}
    """
    if limit:
        df = df.head(limit)

    records = []
    for _, row in df.iterrows():
        record = {
            # hour_of_day dùng làm proxy cho "Time" của Kaggle
            # (Kaggle Time = seconds from first transaction, ở đây dùng giờ trong ngày)
            "Time": float(row.get("hour_of_day", 0)),
            "Amount": float(row["amount"]),
        }
        # Thêm V1..V28
        for i in range(1, 29):
            col = f"V{i}"
            record[col] = float(row[col]) if col in row.index else 0.0

        records.append(record)

    return {"request_data": records}


# ── Task Functions ────────────────────────────────────────────────────────────

def load_feature_data(**context):
    """
    Task 1: Lấy feature file mới nhất từ S3 /feature-store/.
    Lưu thông tin vào XCom để task sau dùng.
    """
    s3 = get_s3_client()

    s3_key, batch_id = get_latest_feature_file()

    # Đọc parquet từ S3
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    logger.info(f"Loaded {len(df)} rows from {s3_key}")
    logger.info(f"Columns: {list(df.columns)}")

    # Kiểm tra các cột model cần có
    missing_v_cols = [c for c in [f"V{i}" for i in range(1, 29)] if c not in df.columns]
    if missing_v_cols:
        raise ValueError(f"Missing required model columns: {missing_v_cols}")

    if "amount" not in df.columns:
        raise ValueError("Missing required column: 'amount'")

    # Thống kê
    fraud_count  = int(df["is_fraud_label"].sum()) if "is_fraud_label" in df.columns else "N/A"
    logger.info(f"Batch: {batch_id} | rows: {len(df)} | fraud_label: {fraud_count}")

    # Push XCom
    ti = context["ti"]
    ti.xcom_push(key="s3_key",   value=s3_key)
    ti.xcom_push(key="batch_id", value=batch_id)
    ti.xcom_push(key="row_count", value=len(df))


def call_fraud_api(**context):
    """
    Task 2: Gọi API Gateway với feature data.

    - Đọc lại file từ S3 (không dùng XCom để truyền DataFrame lớn)
    - Build request body đúng format
    - Gọi POST với Basic Auth
    - Gửi theo batch nhỏ 100 rows/request để tránh timeout
    - Chỉ kiểm tra success — không lưu kết quả vào S3
    """
    ti = context["ti"]
    s3_key   = ti.xcom_pull(key="s3_key",   task_ids="load_feature_data")
    batch_id = ti.xcom_pull(key="batch_id", task_ids="load_feature_data")

    if not API_GATEWAY_URL:
        raise ValueError("API_GATEWAY_URL is not set — check your .env file")

    # Đọc lại từ S3
    s3  = get_s3_client()
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    logger.info(f"Sending {len(df)} rows to API: {API_GATEWAY_URL}")

    # ── Gọi API theo batch 100 rows ───────────────────────────────────────
    api_url    = f"{API_GATEWAY_URL.rstrip('/')}/predict"
    batch_size = 100
    total_rows = len(df)
    total_sent = 0
    failed_chunks = []

    for chunk_start in range(0, total_rows, batch_size):
        chunk_df    = df.iloc[chunk_start : chunk_start + batch_size]
        chunk_num   = chunk_start // batch_size + 1
        total_chunks = (total_rows + batch_size - 1) // batch_size

        # Build request body
        body = build_request_body(chunk_df)

        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: "
            f"{len(body['request_data'])} rows → POST {api_url}"
        )

        try:
            resp = requests.post(
                api_url,
                json=body,
                auth=(API_USERNAME, API_PASSWORD),
                headers={"Content-Type": "application/json"},
                timeout=60,
            )
            resp.raise_for_status()

            # Log response
            try:
                resp_data = resp.json()
                logger.info(f"  ✅ Chunk {chunk_num} OK | status: {resp.status_code} | response: {str(resp_data)[:200]}")
            except Exception:
                logger.info(f"  ✅ Chunk {chunk_num} OK | status: {resp.status_code}")

            total_sent += len(chunk_df)

        except requests.exceptions.HTTPError as e:
            logger.error(f"  ❌ Chunk {chunk_num} HTTP error: {e} | body: {resp.text[:300]}")
            failed_chunks.append(chunk_num)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"  ❌ Chunk {chunk_num} Connection error: {e}")
            failed_chunks.append(chunk_num)
        except requests.exceptions.Timeout:
            logger.error(f"  ❌ Chunk {chunk_num} Timeout after 60s")
            failed_chunks.append(chunk_num)
        except Exception as e:
            logger.error(f"  ❌ Chunk {chunk_num} Unexpected error: {e}")
            failed_chunks.append(chunk_num)

    # ── Tổng kết ──────────────────────────────────────────────────────────
    total_chunks = (total_rows + batch_size - 1) // batch_size
    logger.info(
        f"API call complete: {total_sent}/{total_rows} rows sent | "
        f"failed chunks: {len(failed_chunks)}/{total_chunks}"
    )

    ti.xcom_push(key="total_sent",    value=total_sent)
    ti.xcom_push(key="failed_chunks", value=len(failed_chunks))

    # Nếu TẤT CẢ chunks đều fail → raise để Airflow mark task failed
    if len(failed_chunks) == total_chunks:
        raise RuntimeError(
            f"All {total_chunks} API chunks failed. "
            f"Check API_GATEWAY_URL ({API_GATEWAY_URL}) and credentials."
        )

    # Nếu một phần fail → warning, không dừng
    if failed_chunks:
        logger.warning(f"Partial failure: chunks {failed_chunks} failed")
        send_alert(
            subject=f"Test API: Partial failure — batch {batch_id}",
            message=f"{len(failed_chunks)}/{total_chunks} chunks failed.",
            level="warning",
            context={"batch_id": batch_id, "failed_chunks": str(failed_chunks)},
        )


def verify_response(**context):
    """
    Task 3: Kiểm tra kết quả gọi API có thành công không.
    Chỉ đọc XCom từ task trước và log kết quả tổng hợp.
    """
    ti = context["ti"]
    batch_id     = ti.xcom_pull(key="batch_id",      task_ids="load_feature_data")
    row_count    = ti.xcom_pull(key="row_count",      task_ids="load_feature_data")
    total_sent   = ti.xcom_pull(key="total_sent",     task_ids="call_fraud_api")
    failed_chunks = ti.xcom_pull(key="failed_chunks", task_ids="call_fraud_api")

    success_rate = (total_sent / row_count * 100) if row_count else 0

    summary = (
        f"\n{'='*50}\n"
        f"  TEST API PIPELINE — KẾT QUẢ\n"
        f"{'='*50}\n"
        f"  Batch ID      : {batch_id}\n"
        f"  Rows loaded   : {row_count}\n"
        f"  Rows sent     : {total_sent}\n"
        f"  Failed chunks : {failed_chunks}\n"
        f"  Success rate  : {success_rate:.1f}%\n"
        f"  API URL       : {API_GATEWAY_URL}\n"
        f"{'='*50}"
    )
    logger.info(summary)

    if failed_chunks == 0:
        logger.info("✅ ALL API CALLS SUCCEEDED")
        send_alert(
            subject=f"Test API: SUCCESS — batch {batch_id}",
            message=f"All {row_count} rows sent successfully.",
            level="success",
            context={"batch_id": batch_id, "rows_sent": total_sent},
        )
    else:
        logger.warning(f"⚠️ PARTIAL SUCCESS: {failed_chunks} chunks failed")


# ── Task Definitions ──────────────────────────────────────────────────────────
with dag:
    t1 = PythonOperator(
        task_id="load_feature_data",
        python_callable=load_feature_data,
    )

    t2 = PythonOperator(
        task_id="call_fraud_api",
        python_callable=call_fraud_api,
        execution_timeout=timedelta(minutes=15),
    )

    t3 = PythonOperator(
        task_id="verify_response",
        python_callable=verify_response,
    )

    t1 >> t2 >> t3
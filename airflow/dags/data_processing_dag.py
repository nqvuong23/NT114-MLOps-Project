"""
fraud_detection_pipeline.py
============================
Airflow DAG chính: Batch Preprocessing Pipeline cho Fraud Detection MLOps.

Schedule: Chạy mỗi 5 phút
Flow:
  1. extract_from_rds       → Lấy batch data từ RDS PostgreSQL
  2. save_raw_to_s3         → Lưu raw data vào S3 /raw-data/
  3. validate_raw           → Great Expectations validate raw
  4. run_spark_cleaning     → Spark cleaning & transformation
  5. validate_processed     → Great Expectations validate processed
  6. run_spark_features     → Spark feature engineering
  7. validate_features      → Great Expectations validate features
  8. call_fraud_api         → Gọi API Gateway → Lambda → ECS Model
"""

import os
import json
import logging
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

# Import từ plugins
import sys
sys.path.insert(0, "/opt/airflow/plugins")

from alert_utils import send_alert, airflow_failure_callback

logger = logging.getLogger(__name__)

# ── Config từ Environment ────────────────────────────────────────────────────
S3_BUCKET        = os.environ.get("S3_BUCKET_NAME", "")
S3_RAW           = os.environ.get("S3_PREFIX_RAW", "raw-data")
S3_PROCESSED     = os.environ.get("S3_PREFIX_PROCESSED", "processed-data")
S3_FEATURE       = os.environ.get("S3_PREFIX_FEATURE", "feature-store")
S3_PREDICTION    = os.environ.get("S3_PREFIX_PREDICTION", "prediction")
S3_DEAD_LETTER   = os.environ.get("S3_PREFIX_DEAD_LETTER", "dead-letter")
SPARK_JOBS_DIR   = os.environ.get("SPARK_JOBS_DIR")
API_GATEWAY_URL  = os.environ.get("API_GATEWAY_URL", "")
API_USERNAME     = os.environ.get("API_GATEWAY_USERNAME", "admin")
API_PASSWORD     = os.environ.get("API_GATEWAY_PASSWORD", "")
AWS_REGION       = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")

if SPARK_JOBS_DIR not in sys.path:
    sys.path.insert(0, SPARK_JOBS_DIR)

# ── Default Args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "mlops-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": airflow_failure_callback,
    # FIX Airflow 3.x: email_on_failure và email bị deprecated trong default_args
    # → Dùng SmtpNotifier hoặc on_failure_callback để gửi email thay thế
}

# ── DAG Definition ────────────────────────────────────────────────────────────
dag = DAG(
    dag_id="fraud_detection_preprocessing",
    description="Batch preprocessing pipeline: RDS → S3 → Spark → GE → API → DVC",
    schedule="*/5 * * * *",                           # Mỗi 5 phút
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,                                    # Không chạy bù các lần bỏ lỡ
    max_active_runs=1,                                # Chỉ 1 run tại một thời điểm
    default_args=default_args,
    tags=["fraud-detection", "preprocessing", "mlops"],
)


# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_batch_id(logical_date) -> str:
    """Tạo batch_id từ logical_date (Airflow 3.x — thay thế execution_date)."""
    return logical_date.strftime("%Y%m%d_%H%M%S")


def get_rds_conn():
    """Tạo kết nối tới RDS PostgreSQL."""
    return psycopg2.connect(
        host=os.environ["RDS_HOST"],
        port=int(os.environ.get("RDS_PORT", 5432)),
        database=os.environ["RDS_TRANSACTIONS_DB"],
        user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"],
        connect_timeout=10,
    )


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str) -> str:
    """Upload DataFrame dưới dạng Parquet lên S3."""
    import io
    s3 = get_s3_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    logger.info(f"Uploaded {len(df)} rows to {s3_path}")
    return s3_path


def write_dead_letter(df: pd.DataFrame, batch_id: str, step: str, reason: str):
    """Ghi bad data vào dead-letter prefix trên S3."""
    import io
    key = f"{S3_DEAD_LETTER}/{step}/{batch_id}/dead.parquet"
    reason_key = f"{S3_DEAD_LETTER}/{step}/{batch_id}/reason.txt"
    s3 = get_s3_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    s3.put_object(Bucket=S3_BUCKET, Key=reason_key, Body=reason.encode())
    logger.warning(f"Dead letter written: s3://{S3_BUCKET}/{key}")

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

# ─────────────────────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_rds(**context):
    """
    Task 1: Query batch data từ RDS dựa vào last_processed_timestamp.
    Lưu timestamp mới vào Airflow Variable.
    """
    logical_date = context["logical_date"]
    batch_id = get_batch_id(logical_date)
    batch_end = logical_date
    batch_start = batch_end - timedelta(minutes=5)

    # Lấy last timestamp từ Variable (fallback: 5 phút trước)
    last_ts = Variable.get(
        "fraud_pipeline_last_timestamp",
        default_var=batch_start.isoformat()
    )
    logger.info(f"Batch [{batch_id}]: {last_ts} → {batch_end.isoformat()}")

    conn = get_rds_conn()
    try:
        v_cols = ", ".join([f'"{v}"' for v in [f"V{i}" for i in range(1, 29)]])
        query = f"""
            SELECT
                transaction_id, user_id, card_id, timestamp, amount,
                merchant_id, merchant_category, is_international, hour_of_day,
                {v_cols},
                is_fraud_label
            FROM transactions
            WHERE timestamp > %s AND timestamp <= %s
            ORDER BY timestamp ASC;
        """
        df = pd.read_sql_query(query, conn, params=[last_ts, batch_end.isoformat()])
    finally:
        conn.close()

    logger.info(f"Extracted {len(df)} transactions from RDS")

    if len(df) == 0:
        logger.info("No new transactions in this batch — skipping pipeline")
        # Update timestamp dù không có data
        Variable.set("fraud_pipeline_last_timestamp", batch_end.isoformat())
        context["ti"].xcom_push(key="batch_id", value=batch_id)
        context["ti"].xcom_push(key="row_count", value=0)
        context["ti"].xcom_push(key="skip_pipeline", value=True)
        return

    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="row_count", value=len(df))
    context["ti"].xcom_push(key="skip_pipeline", value=False)

    # Lưu raw DataFrame dạng JSON string vào XCom (nếu nhỏ, < 10k rows)
    # Với batch lớn hơn nên dùng temp file, nhưng 5 phút data thường nhỏ
    context["ti"].xcom_push(
        key="raw_df_json", 
        value=df.to_json(orient="records", date_format="iso")
    )

    # Cập nhật last timestamp
    Variable.set("fraud_pipeline_last_timestamp", batch_end.isoformat())


def save_raw_to_s3(**context):
    """Task 2: Lưu raw data lên S3 /raw-data/{batch_id}/"""
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        logger.info("Skipping: no data in this batch")
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    raw_json = ti.xcom_pull(key="raw_df_json", task_ids="extract_from_rds")
    df = pd.read_json(raw_json, orient="records")

    s3_key = f"{S3_RAW}/{batch_id}/raw.parquet"
    s3_path = upload_parquet_to_s3(df, s3_key)
    ti.xcom_push(key="raw_s3_path", value=s3_path)
    logger.info(f"Raw data saved: {s3_path}")


def validate_raw(**context):
    """Task 3: Great Expectations validate raw data."""
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    import io
    s3_path = f"s3://{S3_BUCKET}/{S3_RAW}/{batch_id}/raw.parquet"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read raw data from S3: {e}")
    
    from ge_validations import validate_raw_data, log_validation_result
    passed, result = validate_raw_data(df)
    log_validation_result(result, "raw_data")

    if not passed:
        write_dead_letter(df, batch_id, "raw_validation", json.dumps(result))
        send_alert(
            subject=f"Raw Validation FAILED — batch {batch_id}",
            message=f"Great Expectations failed for raw data.\n{json.dumps(result, indent=2)}",
            level="error",
            context={"batch_id": batch_id, "failed": result["failed_count"]}
        )
        # Raise exception để Airflow mark task failed và retry
        raise ValueError(f"Raw data validation failed: {result['failed_count']} expectations failed")


def run_spark_cleaning(**context):
    """Task 4: Gọi Spark cleaning & transformation job."""
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return
    
    import json
    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    input_path  = f"s3a://{S3_BUCKET}/{S3_RAW}/{batch_id}/raw.parquet"
    output_path = f"s3a://{S3_BUCKET}/{S3_PROCESSED}/{batch_id}"

    from spark_cleaning import clean_and_transform, get_spark_session

    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        row_count, fraud_count = clean_and_transform(
            input_path=input_path,
            output_path=output_path,
            spark=spark,     # truyền session vào để tái sử dụng
        )
        result = {
            "batch_id": batch_id,
            "output_path": output_path,
            "row_count": row_count,
            "fraud_count": fraud_count,
            "status": "success",
        }
        logger.info(f"Spark cleaning result: {result}")
        ti.xcom_push(key="cleaning_result", value=json.dumps(result))
        # Lưu spark session id để task sau tái sử dụng (thông qua getOrCreate)
    except Exception as e:
        logger.error(f"Spark cleaning failed: {e}", exc_info=True)
        spark.stop()
        raise


def validate_processed(**context):
    """Task 5: Great Expectations validate processed data."""
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    import io
    s3_path = f"s3://{S3_BUCKET}/{S3_PROCESSED}/{batch_id}/"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read processed data from S3: {e}")

    from ge_validations import validate_processed_data, log_validation_result
    passed, result = validate_processed_data(df)
    log_validation_result(result, "processed_data")

    if not passed:
        write_dead_letter(df, batch_id, "processed_validation", json.dumps(result))
        send_alert(
            subject=f"Processed Validation FAILED — batch {batch_id}",
            message=json.dumps(result, indent=2),
            level="error",
            context={"batch_id": batch_id}
        )
        raise ValueError(f"Processed data validation failed")


def run_spark_features(**context):
    """Task 6: Gọi Spark feature engineering job."""
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    import json
    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
 
    input_path  = f"s3a://{S3_BUCKET}/{S3_PROCESSED}/{batch_id}/"
    output_path = f"s3a://{S3_BUCKET}/{S3_FEATURE}/{batch_id}"
 
    from spark_feature_engineering import feature_engineering, get_spark_session
 
    # getOrCreate sẽ lấy lại session đang chạy nếu còn tồn tại
    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        row_count, fraud_count = feature_engineering(
            input_path=input_path,
            output_path=output_path,
            spark=spark,
        )
        result = {
            "batch_id": batch_id,
            "output_path": output_path,
            "row_count": row_count,
            "fraud_count": fraud_count,
            "status": "success",
        }
        logger.info(f"Spark feature result: {result}")
        ti.xcom_push(key="feature_result", value=json.dumps(result))
        # Stop session sau khi cả 2 spark tasks hoàn thành
        spark.stop()
    except Exception as e:
        logger.error(f"Spark feature engineering failed: {e}", exc_info=True)
        spark.stop()
        raise


def validate_features(**context):
    """Task 7: Great Expectations validate feature dataset."""
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    import io
    s3_path = f"s3://{S3_BUCKET}/{S3_FEATURE}/{batch_id}/"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read feature data from S3: {e}")

    from ge_validations import validate_feature_store, log_validation_result
    passed, result = validate_feature_store(df)
    log_validation_result(result, "features")

    if not passed:
        write_dead_letter(df, batch_id, "feature_validation", json.dumps(result))
        send_alert(
            subject=f"Feature Validation FAILED — batch {batch_id}",
            message=json.dumps(result, indent=2),
            level="warning",   # warning, không phải error — pipeline vẫn tiếp tục
            context={"batch_id": batch_id}
        )
        # Feature validation fail → alert nhưng KHÔNG dừng pipeline
        # (theo yêu cầu: chỉ alert, không stop)
        logger.warning("Feature validation failed but pipeline continues (non-blocking)")


def call_fraud_api(**context):
    """
    Task 8: Gọi API Gateway → Lambda → ECS ML Model.
    - Đọc feature data từ S3
    - DROP cột is_fraud_label trước khi gửi (chỉ dùng cho training)
    - Gửi từng batch nhỏ tới API
    - Lưu predictions vào XCom
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    import io
    s3_path = f"s3://{S3_BUCKET}/{S3_FEATURE}/{batch_id}/"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read feature data from S3: {e}")
    logger.info(f"Loaded {len(df)} rows from {s3_path}")

    # ── QUAN TRỌNG: Drop is_fraud_label trước khi gửi đến model ─────────
    inference_df = df.drop(columns=["is_fraud_label"], errors="ignore")
    # Drop các cột metadata không cần thiết cho inference
    meta_cols = ["user_id", "card_id", "timestamp", "merchant_id", "merchant_category"]
    inference_df = inference_df.drop(columns=meta_cols, errors="ignore")

    logger.info(f"Calling fraud detection API for {len(inference_df)} transactions")
    logger.info(f"Inference features: {list(inference_df.columns)}")

    # Gọi API theo batch nhỏ 100 records/request
    api_url    = f"{API_GATEWAY_URL.rstrip('/')}/predict"
    batch_size = 100
    total_rows = len(inference_df)
    total_sent = 0
    failed_chunks = []

    for i in range(0, len(inference_df), batch_size):
        chunk = inference_df.iloc[i:i + batch_size]
        chunk_num   = i // batch_size + 1
        total_chunks = (total_rows + batch_size - 1) // batch_size

        payload = build_request_body(chunk)

        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: "
            f"{len(payload['request_data'])} rows → POST {api_url}"
        )

        try:
            resp = requests.post(
                api_url,
                json=payload,
                auth=(API_USERNAME, API_PASSWORD),
                timeout=60,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()

            logger.info(f"  API chunk {i//batch_size + 1}: {len(preds)} predictions")
            # Log response
            try:
                resp_data = resp.json()
                logger.info(f"Chunk {chunk_num} OK | status: {resp.status_code} | response: {str(resp_data)[:200]}")
            except Exception:
                logger.info(f"Chunk {chunk_num} OK | status: {resp.status_code}")

            total_sent += len(chunk)
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"Chunk {chunk_num} HTTP error: {e} | body: {resp.text[:300]}")
            failed_chunks.append(chunk_num)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Chunk {chunk_num} Connection error: {e}")
            failed_chunks.append(chunk_num)
        except requests.exceptions.Timeout:
            logger.error(f"Chunk {chunk_num} Timeout after 60s")
            failed_chunks.append(chunk_num)
        except Exception as e:
            logger.error(f"Chunk {chunk_num} Unexpected error: {e}")
            failed_chunks.append(chunk_num)

    # ── Tổng kết ──────────────────────────────────────────────────────────
    total_chunks = (total_rows + batch_size - 1) // batch_size
    logger.info(
        f"API call complete: {total_sent}/{total_rows} rows sent | "
        f"failed chunks: {len(failed_chunks)}/{total_chunks}"
    )

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

# ─────────────────────────────────────────────────────────────────────────────
# TASK DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────

with dag:
    t1_extract = PythonOperator(
        task_id="extract_from_rds",
        python_callable=extract_from_rds,
    )

    t2_save_raw = PythonOperator(
        task_id="save_raw_to_s3",
        python_callable=save_raw_to_s3,
    )

    t3_validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw,
    )

    t4_spark_clean = PythonOperator(
        task_id="run_spark_cleaning",
        python_callable=run_spark_cleaning,
        execution_timeout=timedelta(minutes=25),
    )

    t5_validate_processed = PythonOperator(
        task_id="validate_processed",
        python_callable=validate_processed,
    )

    t6_spark_features = PythonOperator(
        task_id="run_spark_features",
        python_callable=run_spark_features,
        execution_timeout=timedelta(minutes=25),
    )

    t7_validate_features = PythonOperator(
        task_id="validate_features",
        python_callable=validate_features,
    )

    t8_call_api = PythonOperator(
        task_id="call_fraud_api",
        python_callable=call_fraud_api,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Pipeline Flow ─────────────────────────────────────────────────────
    (
        t1_extract
        >> t2_save_raw
        >> t3_validate_raw
        >> t4_spark_clean
        >> t5_validate_processed
        >> t6_spark_features
        >> t7_validate_features
        >> t8_call_api
    )
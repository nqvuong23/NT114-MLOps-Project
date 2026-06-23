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
MODEL_ECS_ENDPOINT  = os.environ.get("MODEL_ECS_ENDPOINT", "")
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


def clear_s3_prefix(prefix: str):
    """
    Xoá toàn bộ object nằm dưới một S3 prefix (xử lý output dạng thư mục
    của PySpark như _SUCCESS, part-00000.parquet, part-00001.parquet, ...).

    Args:
        prefix: S3 key prefix không có s3://bucket/, ví dụ:
                "processed-data/20250101_120000/"
    """
    s3 = get_s3_client()
    # Đảm bảo prefix kết thúc bằng '/' để tránh xoá nhầm key khác
    if not prefix.endswith("/"):
        prefix = prefix + "/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    keys_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            keys_to_delete.append({"Key": obj["Key"]})

    if not keys_to_delete:
        logger.info(f"clear_s3_prefix: no objects found under s3://{S3_BUCKET}/{prefix}")
        return

    # S3 delete_objects hỗ trợ tối đa 1000 key mỗi lần
    for i in range(0, len(keys_to_delete), 1000):
        batch = keys_to_delete[i : i + 1000]
        s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": batch})

    logger.info(
        f"clear_s3_prefix: deleted {len(keys_to_delete)} objects "
        f"from s3://{S3_BUCKET}/{prefix}"
    )


def extract_failed_indices(result: dict) -> set:
    """
    Trích xuất tập hợp các row index bị lỗi từ kết quả GE validation.

    GE lưu các index lỗi trong trường "partial_unexpected_index_list" bên trong
    chuỗi "result" của mỗi failed expectation.  Hàm này parse chuỗi đó
    (hoặc dict nếu đã được deserialize) và gộp tất cả index lại thành một set.

    Lưu ý: GE chỉ trả về partial_unexpected_index_list (mặc định ≤ 20 phần tử).
    Với batch nhỏ (< 5 phút), danh sách này thường đủ để xác định toàn bộ row lỗi.
    """
    import ast
    failed_indices: set = set()

    for exp in result.get("failed_expectations", []):
        raw_result = exp.get("result", {})

        # Nếu kết quả đã là dict thì dùng trực tiếp, ngược lại parse string
        if isinstance(raw_result, str):
            try:
                parsed = ast.literal_eval(raw_result)
            except (ValueError, SyntaxError):
                logger.warning(
                    f"Cannot parse result string for expectation "
                    f"'{exp.get('expectation_type')}' on column '{exp.get('column')}'"
                )
                continue
        else:
            parsed = raw_result

        # Ưu tiên lấy partial_unexpected_index_list
        idx_list = parsed.get("partial_unexpected_index_list", [])
        if idx_list:
            failed_indices.update(idx_list)
            continue

        # Fallback: unexpected_index_list (đầy đủ, nếu GE trả về)
        full_idx_list = parsed.get("unexpected_index_list", [])
        if full_idx_list:
            failed_indices.update(full_idx_list)

    return failed_indices


def split_by_validation(
    df: pd.DataFrame,
    result: dict,
) -> tuple:
    """
    Tách DataFrame thành 2 phần dựa trên kết quả GE validation:
      - passed_df : các row PASS (không xuất hiện trong danh sách lỗi)
      - failed_df : các row FAIL (index nằm trong partial_unexpected_index_list)

    Trả về (passed_df, failed_df).
    """
    failed_indices = extract_failed_indices(result)

    if not failed_indices:
        # Không tìm được index cụ thể → toàn bộ df pass (hoặc lỗi table-level)
        logger.warning(
            "No row-level failed indices found in validation result. "
            "Treating all rows as passed (check for table-level expectations)."
        )
        return df.copy(), pd.DataFrame(columns=df.columns)

    # Lọc theo positional index (iloc) — GE trả về integer position
    valid_positions = [i for i in failed_indices if i < len(df)]
    failed_df = df.iloc[sorted(valid_positions)].copy()
    passed_mask = ~df.index.isin(df.iloc[sorted(valid_positions)].index)
    passed_df = df[passed_mask].copy()

    logger.info(
        f"Row split: {len(passed_df)} passed, {len(failed_df)} failed "
        f"(from {len(df)} total)"
    )
    return passed_df, failed_df

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
    """
    Task 3: Great Expectations validate raw data.

    Thay vì raise lỗi khi validation fail, task này:
      1. Chạy GE validation trên toàn bộ DataFrame
      2. Tách các row bị lỗi (dựa vào partial_unexpected_index_list trong result)
      3. Upload passed rows trở lại S3 tại cùng đường dẫn raw.parquet
      4. Upload failed rows + reason.txt vào dead-letter trên S3
      5. Pipeline LUÔN tiếp tục (không raise exception)
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    s3_key = f"{S3_RAW}/{batch_id}/raw.parquet"
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read raw data from S3: {e}")

    from ge_validations import validate_raw_data, log_validation_result
    passed, result = validate_raw_data(df)
    log_validation_result(result, "raw_data")

    if not passed:
        # ── Tách row pass / fail ──────────────────────────────────────────
        passed_df, failed_df = split_by_validation(df, result)

        # ── Ghi failed rows vào dead-letter ──────────────────────────────
        if len(failed_df) > 0:
            reason = (
                f"Raw validation failed: {result['failed_count']} expectations failed.\n"
                + json.dumps(result, indent=2)
            )
            write_dead_letter(failed_df, batch_id, "raw_validation", reason)
        else:
            # Không có row cụ thể nào fail (ví dụ table-level expectation)
            # → Ghi toàn bộ df vào dead-letter để không mất dữ liệu
            reason = (
                f"Raw validation failed (table-level): {result['failed_count']} expectations failed.\n"
                + json.dumps(result, indent=2)
            )
            write_dead_letter(df, batch_id, "raw_validation", reason)
            passed_df = df  # giữ nguyên tất cả để pipeline tiếp tục

        # ── Overwrite S3 với passed rows ──────────────────────────────────
        upload_parquet_to_s3(passed_df, s3_key)
        logger.info(
            f"validate_raw: {len(passed_df)} passed rows re-uploaded to {s3_path}, "
            f"{len(failed_df)} failed rows sent to dead-letter."
        )

        send_alert(
            subject=f"Raw Validation PARTIAL FAIL — batch {batch_id}",
            message=(
                f"Great Expectations failed for {result['failed_count']} expectations.\n"
                f"{len(failed_df)} rows moved to dead-letter, "
                f"{len(passed_df)} rows continue in pipeline.\n"
                + json.dumps(result, indent=2)
            ),
            level="warning",
            context={"batch_id": batch_id, "failed": result["failed_count"]}
        )
    else:
        logger.info(f"validate_raw: All {len(df)} rows passed validation.")


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
    """
    Task 5: Great Expectations validate processed data.

    Thay vì raise lỗi khi validation fail, task này:
      1. Chạy GE validation trên toàn bộ DataFrame
      2. Tách các row bị lỗi (dựa vào partial_unexpected_index_list trong result)
      3. Upload passed rows trở lại S3 tại cùng thư mục processed
      4. Upload failed rows + reason.txt vào dead-letter trên S3
      5. Pipeline LUÔN tiếp tục (không raise exception)
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    # PySpark ghi output dạng thư mục: _SUCCESS + part-*.parquet
    # → đọc cả thư mục, pandas tự ghép các part file lại
    s3_prefix = f"{S3_PROCESSED}/{batch_id}/"
    s3_path = f"s3://{S3_BUCKET}/{s3_prefix}"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read processed data from S3: {e}")

    from ge_validations import validate_processed_data, log_validation_result
    passed, result = validate_processed_data(df)
    log_validation_result(result, "processed_data")

    if not passed:
        # ── Tách row pass / fail ──────────────────────────────────────────
        passed_df, failed_df = split_by_validation(df, result)

        # ── Ghi failed rows vào dead-letter ──────────────────────────────
        if len(failed_df) > 0:
            reason = (
                f"Processed validation failed: {result['failed_count']} expectations failed.\n"
                + json.dumps(result, indent=2)
            )
            write_dead_letter(failed_df, batch_id, "processed_validation", reason)
        else:
            reason = (
                f"Processed validation failed (table-level): {result['failed_count']} expectations failed.\n"
                + json.dumps(result, indent=2)
            )
            write_dead_letter(df, batch_id, "processed_validation", reason)
            passed_df = df

        # ── Xoá toàn bộ Spark part-files cũ rồi upload lại passed rows ───
        # Cần xoá trước vì Spark tạo nhiều file (part-*.parquet, _SUCCESS, ...)
        # → upload đè 1 key mới sẽ KHÔNG xoá các file cũ còn lại
        clear_s3_prefix(s3_prefix)
        # Dùng tên part-00000.parquet để Spark downstream vẫn đọc được
        # thư mục như một Parquet dataset bình thường
        clean_key = f"{s3_prefix}part-00000.parquet"
        upload_parquet_to_s3(passed_df, clean_key)
        logger.info(
            f"validate_processed: {len(passed_df)} passed rows re-uploaded to "
            f"s3://{S3_BUCKET}/{clean_key}, "
            f"{len(failed_df)} failed rows sent to dead-letter."
        )

        send_alert(
            subject=f"Processed Validation PARTIAL FAIL — batch {batch_id}",
            message=(
                f"Great Expectations failed for {result['failed_count']} expectations.\n"
                f"{len(failed_df)} rows moved to dead-letter, "
                f"{len(passed_df)} rows continue in pipeline.\n"
                + json.dumps(result, indent=2)
            ),
            level="warning",
            context={"batch_id": batch_id}
        )
    else:
        logger.info(f"validate_processed: All {len(df)} rows passed validation.")


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
    """
    Task 7: Great Expectations validate feature dataset.

    Thay vì ghi toàn bộ df vào dead-letter khi validation fail, task này:
      1. Chạy GE validation trên toàn bộ DataFrame
      2. Tách các row bị lỗi (dựa vào partial_unexpected_index_list trong result)
      3. Upload passed rows trở lại S3 tại cùng thư mục feature-store
      4. Upload failed rows + reason.txt vào dead-letter trên S3
      5. Pipeline LUÔN tiếp tục (không raise exception)
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")

    # PySpark ghi output dạng thư mục: _SUCCESS + part-*.parquet
    # → đọc cả thư mục, pandas tự ghép các part file lại
    s3_prefix = f"{S3_FEATURE}/{batch_id}/"
    s3_path = f"s3://{S3_BUCKET}/{s3_prefix}"
    try:
        df = pd.read_parquet(s3_path)
    except Exception as e:
        raise RuntimeError(f"Cannot read feature data from S3: {e}")

    from ge_validations import validate_feature_store, log_validation_result
    passed, result = validate_feature_store(df)
    log_validation_result(result, "features")

    if not passed:
        # ── Tách row pass / fail ──────────────────────────────────────────
        passed_df, failed_df = split_by_validation(df, result)

        # ── Ghi failed rows vào dead-letter ──────────────────────────────
        if len(failed_df) > 0:
            reason = (
                f"Feature validation failed: {result['failed_count']} expectations failed.\n"
                + json.dumps(result, indent=2)
            )
            write_dead_letter(failed_df, batch_id, "feature_validation", reason)
        else:
            reason = (
                f"Feature validation failed (table-level): {result['failed_count']} expectations failed.\n"
                + json.dumps(result, indent=2)
            )
            write_dead_letter(df, batch_id, "feature_validation", reason)
            passed_df = df

        # ── Xoá toàn bộ Spark part-files cũ rồi upload lại passed rows ───
        # Cần xoá trước vì Spark tạo nhiều file (part-*.parquet, _SUCCESS, ...)
        # → upload đè 1 key mới sẽ KHÔNG xoá các file cũ còn lại
        clear_s3_prefix(s3_prefix)
        # Dùng tên part-00000.parquet để call_fraud_api downstream vẫn đọc được
        # thư mục như một Parquet dataset bình thường
        clean_key = f"{s3_prefix}part-00000.parquet"
        upload_parquet_to_s3(passed_df, clean_key)
        logger.info(
            f"validate_features: {len(passed_df)} passed rows re-uploaded to "
            f"s3://{S3_BUCKET}/{clean_key}, "
            f"{len(failed_df)} failed rows sent to dead-letter."
        )

        send_alert(
            subject=f"Feature Validation PARTIAL FAIL — batch {batch_id}",
            message=(
                f"Great Expectations failed for {result['failed_count']} expectations.\n"
                f"{len(failed_df)} rows moved to dead-letter, "
                f"{len(passed_df)} rows continue in pipeline.\n"
                + json.dumps(result, indent=2)
            ),
            level="warning",
            context={"batch_id": batch_id}
        )
        logger.warning(
            f"Feature validation failed but pipeline continues — "
            f"{len(passed_df)} clean rows forwarded to call_fraud_api."
        )
    else:
        logger.info(f"validate_features: All {len(df)} rows passed validation.")


def call_fraud_api(**context):
    """
    Task 8: Gọi API Gateway → Lambda → ECS ML Model.
    - Đọc feature data từ S3
    - DROP cột is_fraud_label trước khi gửi (chỉ dùng cho training)
    - Gửi toàn bộ request body tới API trong một cuộc gọi duy nhất
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

    api_url = f"{MODEL_ECS_ENDPOINT.rstrip('/')}/predict"
    payload = build_request_body(inference_df)

    logger.info(f"Sending request body with {len(payload['request_data'])} rows to {api_url}")

    try:
        resp = requests.post(
            api_url,
            json=payload,
            # auth=(API_USERNAME, API_PASSWORD),
            timeout=60,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()

        # Log response
        try:
            resp_data = resp.json()
            logger.info(f"API call OK | status: {resp.status_code} | response: {str(resp_data)[:200]}")
        except Exception:
            logger.info(f"API call OK | status: {resp.status_code}")

    except requests.exceptions.HTTPError as e:
        logger.error(f"API call HTTP error: {e} | body: {resp.text[:300] if 'resp' in locals() else ''}")
        raise RuntimeError(f"API call failed with HTTP error: {e}")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"API call Connection error: {e}")
        raise RuntimeError(f"API call failed with Connection error: {e}")
    except requests.exceptions.Timeout as e:
        logger.error(f"API call Timeout after 60s: {e}")
        raise RuntimeError(f"API call failed with Timeout: {e}")
    except Exception as e:
        logger.error(f"API call Unexpected error: {e}")
        raise RuntimeError(f"API call failed with unexpected error: {e}")


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
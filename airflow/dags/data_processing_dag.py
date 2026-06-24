"""
fraud_detection_pipeline.py
============================
Airflow DAG chính: Batch Preprocessing Pipeline cho Fraud Detection MLOps.

Schedule: Chạy mỗi 5 phút
Flow (refactored — deferred S3 upload):
  1. extract_from_rds       → Lấy batch data từ RDS PostgreSQL (push raw DF via XCom)
  2. validate_raw           → Great Expectations validate raw (reads XCom, push passed DF)
  3. save_raw_to_s3         → Upload passed raw DF lên S3 /raw-data/ via PySpark
  4. run_spark_cleaning     → Spark cleaning & transformation (reads S3, returns DF via XCom)
  5. validate_processed     → Great Expectations validate processed (reads XCom, push passed DF)
  6. save_processed_to_s3   → Upload passed processed DF lên S3 /processed-data/ via PySpark
  7. run_spark_features     → Spark feature engineering (reads S3, returns DF via XCom)
  8. validate_features      → Great Expectations validate features (reads XCom, push passed DF)
  9. save_features_to_s3    → Upload passed features DF lên S3 /feature-store/ via PySpark
  10. call_fraud_api        → Gọi API Gateway → Lambda → ECS Model

Rationale:
  - Upload lên S3 chỉ xảy ra MỘT LẦN sau mỗi bước validate,
    không còn upload → đọc lại → clear_s3_prefix → upload đè.
  - clear_s3_prefix() không còn cần thiết và đã bị loại bỏ.
  - Khi validation fail: split_by_validation vẫn tách passed/failed rows,
    failed rows → dead-letter S3, passed rows push tiếp XCom → upload.
"""

import os
import json
import logging
import io
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
    description="Batch preprocessing pipeline: RDS → Validate → S3 → Spark → Validate → S3 → API",
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


def write_dead_letter(df: pd.DataFrame, batch_id: str, step: str, reason: str):
    """Ghi bad data vào dead-letter prefix trên S3."""
    key = f"{S3_DEAD_LETTER}/{step}/{batch_id}/dead.parquet"
    reason_key = f"{S3_DEAD_LETTER}/{step}/{batch_id}/reason.txt"
    s3 = get_s3_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
    s3.put_object(Bucket=S3_BUCKET, Key=reason_key, Body=reason.encode())
    logger.warning(f"Dead letter written: s3://{S3_BUCKET}/{key}")


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
    Push raw DataFrame (JSON) vào XCom để validate_raw đọc trực tiếp —
    KHÔNG upload lên S3 ngay, việc upload diễn ra sau validate_raw.
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

    # Push raw DataFrame dạng JSON string vào XCom để validate_raw đọc trực tiếp.
    # Với batch 5 phút, data thường nhỏ nên XCom là hợp lý.
    context["ti"].xcom_push(
        key="raw_df_json",
        value=df.to_json(orient="records", date_format="iso")
    )

    # Cập nhật last timestamp
    Variable.set("fraud_pipeline_last_timestamp", batch_end.isoformat())


def validate_raw(**context):
    """
    Task 2: Great Expectations validate raw data.

    Đọc raw DataFrame từ XCom (không đọc S3 — vì chưa upload).
    Sau khi validate:
      - Tách passed / failed rows.
      - Push passed_df_json vào XCom cho save_raw_to_s3.
      - Upload failed rows + reason.txt vào dead-letter trên S3.
      - Pipeline LUÔN tiếp tục (không raise exception).
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    raw_json = ti.xcom_pull(key="raw_df_json", task_ids="extract_from_rds")

    if not raw_json:
        raise RuntimeError("validate_raw: raw_df_json XCom is empty — extract_from_rds may have failed")

    df = pd.read_json(raw_json, orient="records")
    logger.info(f"validate_raw: loaded {len(df)} rows from XCom")

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
        logger.info(
            f"validate_raw: {len(passed_df)} passed rows pushed to XCom, "
            f"{len(failed_df)} failed rows sent to dead-letter."
        )
    else:
        passed_df = df
        logger.info(f"validate_raw: All {len(df)} rows passed validation.")

    # Push passed DataFrame vào XCom để save_raw_to_s3 upload lên S3
    ti.xcom_push(
        key="passed_raw_df_json",
        value=passed_df.to_json(orient="records", date_format="iso")
    )


def save_raw_to_s3(**context):
    """
    Task 3: Upload passed raw DataFrame (từ XCom) lên S3 /raw-data/ via PySpark.

    Chỉ upload MỘT LẦN sau khi validation đã tách passed/failed rows.
    Không cần clear_s3_prefix vì output là single parquet file.
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        logger.info("Skipping: no data in this batch")
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    passed_json = ti.xcom_pull(key="passed_raw_df_json", task_ids="validate_raw")

    if not passed_json:
        raise RuntimeError("save_raw_to_s3: passed_raw_df_json XCom is empty")

    df = pd.read_json(passed_json, orient="records")
    logger.info(f"save_raw_to_s3: uploading {len(df)} rows to S3")

    from spark_s3_upload import upload_pandas_df_to_s3, get_spark_session

    s3_output_prefix = f"s3a://{S3_BUCKET}/{S3_RAW}/{batch_id}"
    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        s3_path = upload_pandas_df_to_s3(
            df=df,
            s3_output_path=s3_output_prefix,
            filename="raw.parquet",
            spark=spark,
        )
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"save_raw_to_s3 failed: {e}") from e

    # Lưu path để run_spark_cleaning dùng làm input
    ti.xcom_push(key="raw_s3_path", value=s3_path)
    logger.info(f"Raw data saved: {s3_path}")


def run_spark_cleaning(**context):
    """
    Task 4: Gọi Spark cleaning & transformation job.

    Đọc raw parquet từ S3 (output của save_raw_to_s3).
    clean_and_transform trả về Spark DataFrame (không ghi S3).
    Convert DataFrame → pandas → JSON → push XCom để validate_processed đọc.
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    raw_s3_path = ti.xcom_pull(key="raw_s3_path", task_ids="save_raw_to_s3")

    if not raw_s3_path:
        raise RuntimeError("run_spark_cleaning: raw_s3_path XCom is empty")

    # Đảm bảo dùng s3a:// scheme cho Hadoop-AWS
    input_path = raw_s3_path.replace("s3://", "s3a://")

    from spark_cleaning import clean_and_transform, get_spark_session

    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        # clean_and_transform trả về (spark_df, row_count, fraud_count)
        spark_df, row_count, fraud_count = clean_and_transform(
            input_path=input_path,
            spark=spark,
        )

        # Convert Spark → pandas để push qua XCom
        cleaned_pandas_df = spark_df.toPandas()
        result = {
            "batch_id": batch_id,
            "row_count": row_count,
            "fraud_count": fraud_count,
            "status": "success",
        }
        logger.info(f"Spark cleaning result: {result}")
        ti.xcom_push(key="cleaning_result", value=json.dumps(result))

        # Push cleaned DataFrame vào XCom để validate_processed đọc
        ti.xcom_push(
            key="cleaned_df_json",
            value=cleaned_pandas_df.to_json(orient="records", date_format="iso")
        )

    except Exception as e:
        logger.error(f"Spark cleaning failed: {e}", exc_info=True)
        spark.stop()
        raise


def validate_processed(**context):
    """
    Task 5: Great Expectations validate processed data.

    Đọc cleaned DataFrame từ XCom (không đọc S3 — chưa upload).
    Sau khi validate:
      - Tách passed / failed rows.
      - Push passed_df_json vào XCom cho save_processed_to_s3.
      - Upload failed rows + reason.txt vào dead-letter trên S3.
      - Pipeline LUÔN tiếp tục (không raise exception).
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    cleaned_json = ti.xcom_pull(key="cleaned_df_json", task_ids="run_spark_cleaning")

    if not cleaned_json:
        raise RuntimeError("validate_processed: cleaned_df_json XCom is empty")

    df = pd.read_json(cleaned_json, orient="records")
    logger.info(f"validate_processed: loaded {len(df)} rows from XCom")

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
        logger.info(
            f"validate_processed: {len(passed_df)} passed rows pushed to XCom, "
            f"{len(failed_df)} failed rows sent to dead-letter."
        )
    else:
        passed_df = df
        logger.info(f"validate_processed: All {len(df)} rows passed validation.")

    # Push passed DataFrame vào XCom để save_processed_to_s3 upload lên S3
    ti.xcom_push(
        key="passed_processed_df_json",
        value=passed_df.to_json(orient="records", date_format="iso")
    )


def save_processed_to_s3(**context):
    """
    Task 6: Upload passed processed DataFrame (từ XCom) lên S3 /processed-data/ via PySpark.

    Chỉ upload MỘT LẦN sau khi validation đã tách passed/failed rows.
    Không cần clear_s3_prefix vì output là single parquet file.
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        logger.info("Skipping: no data in this batch")
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    passed_json = ti.xcom_pull(key="passed_processed_df_json", task_ids="validate_processed")

    if not passed_json:
        raise RuntimeError("save_processed_to_s3: passed_processed_df_json XCom is empty")

    df = pd.read_json(passed_json, orient="records")
    logger.info(f"save_processed_to_s3: uploading {len(df)} rows to S3")

    from spark_s3_upload import upload_pandas_df_to_s3, get_spark_session

    s3_output_prefix = f"s3a://{S3_BUCKET}/{S3_PROCESSED}/{batch_id}"
    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        s3_path = upload_pandas_df_to_s3(
            df=df,
            s3_output_path=s3_output_prefix,
            filename="processed.parquet",
            spark=spark,
        )
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"save_processed_to_s3 failed: {e}") from e

    # Lưu path để run_spark_features dùng làm input
    ti.xcom_push(key="processed_s3_path", value=s3_path)
    logger.info(f"Processed data saved: {s3_path}")


def run_spark_features(**context):
    """
    Task 7: Gọi Spark feature engineering job.

    Đọc processed parquet từ S3 (output của save_processed_to_s3).
    feature_engineering trả về Spark DataFrame (không ghi S3).
    Convert DataFrame → pandas → JSON → push XCom để validate_features đọc.
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    processed_s3_path = ti.xcom_pull(key="processed_s3_path", task_ids="save_processed_to_s3")

    if not processed_s3_path:
        raise RuntimeError("run_spark_features: processed_s3_path XCom is empty")

    # Đảm bảo dùng s3a:// scheme cho Hadoop-AWS
    input_path = processed_s3_path.replace("s3://", "s3a://")

    from spark_feature_engineering import feature_engineering, get_spark_session

    # getOrCreate sẽ lấy lại session đang chạy nếu còn tồn tại
    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        # feature_engineering trả về (spark_df, row_count, fraud_count)
        spark_df, row_count, fraud_count = feature_engineering(
            input_path=input_path,
            spark=spark,
        )

        # Convert Spark → pandas để push qua XCom
        feature_pandas_df = spark_df.toPandas()
        result = {
            "batch_id": batch_id,
            "row_count": row_count,
            "fraud_count": fraud_count,
            "status": "success",
        }
        logger.info(f"Spark feature result: {result}")
        ti.xcom_push(key="feature_result", value=json.dumps(result))

        # Push feature DataFrame vào XCom để validate_features đọc
        ti.xcom_push(
            key="feature_df_json",
            value=feature_pandas_df.to_json(orient="records", date_format="iso")
        )

        # Stop Spark session sau task features (task cuối dùng Spark)
        spark.stop()

    except Exception as e:
        logger.error(f"Spark feature engineering failed: {e}", exc_info=True)
        spark.stop()
        raise


def validate_features(**context):
    """
    Task 8: Great Expectations validate feature dataset.

    Đọc feature DataFrame từ XCom (không đọc S3 — chưa upload).
    Sau khi validate:
      - Tách passed / failed rows.
      - Push passed_df_json vào XCom cho save_features_to_s3.
      - Upload failed rows + reason.txt vào dead-letter trên S3.
      - Pipeline LUÔN tiếp tục (không raise exception).
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    feature_json = ti.xcom_pull(key="feature_df_json", task_ids="run_spark_features")

    if not feature_json:
        raise RuntimeError("validate_features: feature_df_json XCom is empty")

    df = pd.read_json(feature_json, orient="records")
    logger.info(f"validate_features: loaded {len(df)} rows from XCom")

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
            f"{len(passed_df)} clean rows forwarded to save_features_to_s3."
        )
        logger.info(
            f"validate_features: {len(passed_df)} passed rows pushed to XCom, "
            f"{len(failed_df)} failed rows sent to dead-letter."
        )
    else:
        passed_df = df
        logger.info(f"validate_features: All {len(df)} rows passed validation.")

    # Push passed DataFrame vào XCom để save_features_to_s3 upload lên S3
    ti.xcom_push(
        key="passed_features_df_json",
        value=passed_df.to_json(orient="records", date_format="iso")
    )


def save_features_to_s3(**context):
    """
    Task 9: Upload passed features DataFrame (từ XCom) lên S3 /feature-store/ via PySpark.

    Chỉ upload MỘT LẦN sau khi validation đã tách passed/failed rows.
    Không cần clear_s3_prefix vì output là single parquet file.
    Push feature_s3_path vào XCom để call_fraud_api đọc.
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        logger.info("Skipping: no data in this batch")
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    passed_json = ti.xcom_pull(key="passed_features_df_json", task_ids="validate_features")

    if not passed_json:
        raise RuntimeError("save_features_to_s3: passed_features_df_json XCom is empty")

    df = pd.read_json(passed_json, orient="records")
    logger.info(f"save_features_to_s3: uploading {len(df)} rows to S3")

    from spark_s3_upload import upload_pandas_df_to_s3, get_spark_session

    s3_output_prefix = f"s3a://{S3_BUCKET}/{S3_FEATURE}/{batch_id}"
    spark = get_spark_session("FraudDetection-Pipeline")
    try:
        s3_path = upload_pandas_df_to_s3(
            df=df,
            s3_output_path=s3_output_prefix,
            filename="features.parquet",
            spark=spark,
        )
        spark.stop()
    except Exception as e:
        spark.stop()
        raise RuntimeError(f"save_features_to_s3 failed: {e}") from e

    # Lưu path để call_fraud_api dùng làm input
    ti.xcom_push(key="feature_s3_path", value=s3_path)
    logger.info(f"Feature data saved: {s3_path}")


def call_fraud_api(**context):
    """
    Task 10: Gọi API Gateway → Lambda → ECS ML Model.
    - Đọc feature data từ S3 (output của save_features_to_s3)
    - DROP cột is_fraud_label trước khi gửi (chỉ dùng cho training)
    - Gửi toàn bộ request body tới API trong một cuộc gọi duy nhất
    - Lưu predictions vào XCom
    """
    ti = context["ti"]
    skip = ti.xcom_pull(key="skip_pipeline", task_ids="extract_from_rds")
    if skip:
        return

    batch_id = ti.xcom_pull(key="batch_id", task_ids="extract_from_rds")
    feature_s3_path = ti.xcom_pull(key="feature_s3_path", task_ids="save_features_to_s3")

    if not feature_s3_path:
        raise RuntimeError("call_fraud_api: feature_s3_path XCom is empty")

    # Đọc từ S3 — dùng s3:// (boto3/pandas) thay vì s3a://
    s3_path = feature_s3_path.replace("s3a://", "s3://")
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

    t2_validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw,
    )

    t3_save_raw = PythonOperator(
        task_id="save_raw_to_s3",
        python_callable=save_raw_to_s3,
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

    t6_save_processed = PythonOperator(
        task_id="save_processed_to_s3",
        python_callable=save_processed_to_s3,
    )

    t7_spark_features = PythonOperator(
        task_id="run_spark_features",
        python_callable=run_spark_features,
        execution_timeout=timedelta(minutes=25),
    )

    t8_validate_features = PythonOperator(
        task_id="validate_features",
        python_callable=validate_features,
    )

    t9_save_features = PythonOperator(
        task_id="save_features_to_s3",
        python_callable=save_features_to_s3,
    )

    t10_call_api = PythonOperator(
        task_id="call_fraud_api",
        python_callable=call_fraud_api,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Pipeline Flow ─────────────────────────────────────────────────────
    # extract → validate_raw → save_raw_to_s3
    # → run_spark_cleaning → validate_processed → save_processed_to_s3
    # → run_spark_features → validate_features → save_features_to_s3
    # → call_fraud_api
    (
        t1_extract
        >> t2_validate_raw
        >> t3_save_raw
        >> t4_spark_clean
        >> t5_validate_processed
        >> t6_save_processed
        >> t7_spark_features
        >> t8_validate_features
        >> t9_save_features
        >> t10_call_api
    )
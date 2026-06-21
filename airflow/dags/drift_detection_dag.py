"""
dag_drift_detection.py
======================
DAG 2: Drift Detection với Evidently AI.

Schedule: Hàng ngày lúc 6 giờ sáng

Flow:
  1. load_reference_data   → Load Kaggle CSV làm reference (baseline)
  2. load_inference_logs   → Gộp /prediction/*.parquet của 7 ngày gần nhất
  3. run_drift_detection   → Chạy Evidently: data drift + prediction drift
  4. save_report           → Lưu HTML report lên S3
  5. evaluate_drift        → So sánh drift score với ngưỡng định nghĩa sẵn
  6. trigger_retraining    → Nếu drift → gọi Airflow REST API trigger DAG training

Hai loại drift được detect:
  - Data Drift    : distribution của input features (V1-V28, amount...) thay đổi
  - Prediction Drift: distribution của fraud_score / predicted_fraud thay đổi

Ngưỡng drift (có thể điều chỉnh qua Airflow Variables):
  - DATA_DRIFT_THRESHOLD      : tỉ lệ features bị drift (default 0.3 = 30%)
  - PREDICTION_DRIFT_THRESHOLD: p-value của prediction distribution (default 0.05)
"""

import os
import io
import json
import logging
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import requests as http_requests

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from alert_utils import send_alert, airflow_failure_callback

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET        = os.environ.get("S3_BUCKET_NAME", "")
S3_PREDICTION    = os.environ.get("S3_PREFIX_PREDICTION", "prediction")
S3_TRAINING_DATASET        = os.environ.get("S3_PREFIX_TRAINING_DATASET")
S3_BASELINE      = f"{S3_TRAINING_DATASET}/baseline/creditcard.csv"
S3_DRIFT_REPORTS = os.environ.get("S3_PREFIX_DRIFT_REPORTS")
AWS_REGION       = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
AIRFLOW_API_BASE = os.environ.get("AIRFLOW_API_URL", "http://airflow-apiserver:8080")
AIRFLOW_API_USER = os.environ.get("AIRFLOW_API_USER", "admin")
AIRFLOW_API_PASS = os.environ.get("AIRFLOW_API_PASS", "admin")

# Ngưỡng drift — có thể override qua Airflow Variable
DATA_DRIFT_THRESHOLD       = float(Variable.get("data_drift_threshold",       default_var=os.environ.get("DEFAULT_DATA_DRIFT_THRESHOLD")))
PREDICTION_DRIFT_THRESHOLD = float(Variable.get("prediction_drift_threshold", default_var=os.environ.get("DEFAULT_PREDICTION_DRIFT_THRESHOLD")))

# Số ngày lookback để lấy inference logs
LOOKBACK_DAYS = int(Variable.get("drift_lookback_days", default_var=os.environ.get("LOOKBACK_DAYS")))

# Các feature columns cần check drift (input features của model)
FEATURE_COLS = (
    ["amount", "amount_normalized", "amount_log1p", "amount_zscore",
     "tx_count_1h", "is_night_hour", "is_high_amount",
     "is_international", "hour_of_day"]
    + [f"V{i}" for i in range(1, 29)]
)

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "mlops-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_failure_callback,
}

dag = DAG(
    dag_id="drift_detection",
    description="Daily drift detection: data drift + prediction drift via Evidently AI",
    schedule="0 6 * * *",      # Mỗi ngày 6h sáng
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fraud-detection", "monitoring", "drift", "evidently"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)


def prepare_reference_features(df_kaggle: pd.DataFrame) -> pd.DataFrame:
    """
    Apply feature engineering lên Kaggle CSV để có cùng schema
    với inference logs từ feature-store.
    """
    import numpy as np

    df = df_kaggle.copy()
    df["hour_of_day"]       = (df["Time"] // 3600) % 24
    df["is_night_hour"]     = df["hour_of_day"].isin([22,23,0,1,2,3,4]).astype(int)
    df["amount_log1p"]      = np.log1p(df["Amount"])
    amount_mean             = df["Amount"].mean()
    amount_std              = df["Amount"].std()
    df["amount_normalized"] = (df["Amount"] - amount_mean) / (amount_std or 1.0)
    df["amount_zscore"]     = df["amount_normalized"]
    times     = df["Time"].values
    start_idx = __import__("numpy").searchsorted(times, times - 3600, side="left")
    df["tx_count_1h"]    = (__import__("numpy").arange(len(times)) - start_idx + 1).astype(int)
    df["is_high_amount"] = (df["Amount"] > 500).astype(int)
    df["is_international"] = 0
    df["amount"]           = df["Amount"]
    # Label
    df["is_fraud_label"] = df["Class"]

    return df


# ── Task Functions ─────────────────────────────────────────────────────────────

def load_reference_data(**context):
    """
    Task 1: Load Kaggle CSV từ S3 làm reference dataset.
    Apply feature engineering để cùng schema với inference logs.
    Lấy sample 10k rows để Evidently chạy nhanh hơn.
    """
    s3 = get_s3()

    logger.info(f"Loading reference data from s3://{S3_BUCKET}/{S3_BASELINE}")
    obj      = s3.get_object(Bucket=S3_BUCKET, Key=S3_BASELINE)
    df_kaggle = pd.read_csv(io.BytesIO(obj["Body"].read()))
    logger.info(f"Kaggle rows: {len(df_kaggle)}")

    # Apply feature engineering
    df_ref = prepare_reference_features(df_kaggle)

    # Lấy sample để đại diện (Evidently không cần toàn bộ 284k rows)
    sample_size = min(10000, len(df_ref))
    df_ref_sample = df_ref.sample(n=sample_size, random_state=42)

    # Chọn feature columns + label
    available_feat = [c for c in FEATURE_COLS if c in df_ref_sample.columns]
    df_ref_out = df_ref_sample[available_feat + ["is_fraud_label"]].reset_index(drop=True)

    # Lưu vào XCom dưới dạng JSON (size nhỏ vì đã sample)
    context["ti"].xcom_push(key="reference_json", value=df_ref_out.to_json(orient="records"))
    context["ti"].xcom_push(key="reference_rows", value=len(df_ref_out))
    logger.info(f"Reference sample: {len(df_ref_out)} rows")


def load_inference_logs(**context):
    """
    Task 2: Gộp các file /prediction/*.parquet của 7 ngày gần nhất.

    File prediction có schema:
      transaction_id, batch_id, predicted_fraud, prediction_timestamp

    Cần join với feature data từ feature-store để có input features.
    → Lấy feature data từ các batch_id tương ứng trong feature-store.
    """
    s3          = get_s3()
    cutoff_time = datetime.now(tz=timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    # ── Bước 1: Tìm prediction files trong N ngày gần nhất ───────────────
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREDICTION}/")

    recent_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") and obj["LastModified"] >= cutoff_time:
                recent_keys.append(obj["Key"])

    if not recent_keys:
        logger.warning(f"No prediction files in last {LOOKBACK_DAYS} days")
        context["ti"].xcom_push(key="current_json",  value=None)
        context["ti"].xcom_push(key="current_rows",  value=0)
        context["ti"].xcom_push(key="has_data",      value=False)
        return

    logger.info(f"Found {len(recent_keys)} prediction files in last {LOOKBACK_DAYS} days")

    # ── Bước 2: Load tất cả prediction files ─────────────────────────────
    pred_dfs = []
    for key in recent_keys:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            pred_dfs.append(df)
        except Exception as e:
            logger.warning(f"Could not load {key}: {e}")

    if not pred_dfs:
        context["ti"].xcom_push(key="current_json",  value=None)
        context["ti"].xcom_push(key="current_rows",  value=0)
        context["ti"].xcom_push(key="has_data",      value=False)
        return

    df_predictions = pd.concat(pred_dfs, ignore_index=True)
    logger.info(f"Total predictions: {len(df_predictions)}")

    # ── Bước 3: Load feature data tương ứng từ feature-store ─────────────
    # Lấy unique batch_ids từ predictions
    batch_ids = df_predictions["batch_id"].unique().tolist()
    logger.info(f"Loading features for {len(batch_ids)} batches...")

    feature_dfs = []
    for batch_id in batch_ids:
        prefix = f"feature-store/{batch_id}/"
        resp   = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".parquet") and "part-" in obj["Key"]:
                try:
                    data = s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])
                    df   = pd.read_parquet(io.BytesIO(data["Body"].read()))
                    feature_dfs.append(df)
                except Exception as e:
                    logger.warning(f"Cannot load feature {obj['Key']}: {e}")
                break  # Chỉ cần 1 file part per batch

    if not feature_dfs:
        logger.warning("No feature data found for batches in prediction window")
        context["ti"].xcom_push(key="current_json",  value=None)
        context["ti"].xcom_push(key="current_rows",  value=0)
        context["ti"].xcom_push(key="has_data",      value=False)
        return

    df_features = pd.concat(feature_dfs, ignore_index=True)

    # ── Bước 4: Join predictions với features ────────────────────────────
    df_current = df_features.merge(
        df_predictions[["transaction_id", "predicted_fraud"]],
        on="transaction_id",
        how="inner",
    )

    # Chọn feature columns + predicted_fraud (để check prediction drift)
    available_feat = [c for c in FEATURE_COLS if c in df_current.columns]
    keep_cols      = available_feat + ["is_fraud_label", "predicted_fraud"]
    df_current_out = df_current[[c for c in keep_cols if c in df_current.columns]]

    logger.info(f"Current window rows: {len(df_current_out)}")

    context["ti"].xcom_push(key="current_json",  value=df_current_out.to_json(orient="records"))
    context["ti"].xcom_push(key="current_rows",  value=len(df_current_out))
    context["ti"].xcom_push(key="has_data",      value=True)


def run_drift_detection(**context):
    """
    Task 3: Chạy Evidently để detect data drift và prediction drift.

    Evidently 0.4.x+ API:
      - Report với DataDriftPreset: check tất cả features
      - Report với TargetDriftPreset: check prediction distribution
    """
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset, TargetDriftPreset
    from evidently.metrics import (
        DataDriftTable,
        ColumnDriftMetric,
    )

    ti = context["ti"]
    has_data      = ti.xcom_pull(key="has_data",        task_ids="load_inference_logs")
    ref_json      = ti.xcom_pull(key="reference_json",  task_ids="load_reference_data")
    current_json  = ti.xcom_pull(key="current_json",    task_ids="load_inference_logs")

    if not has_data or not current_json:
        logger.warning("No current data to compare — skipping drift detection")
        ti.xcom_push(key="drift_detected",      value=False)
        ti.xcom_push(key="data_drift_score",    value=0.0)
        ti.xcom_push(key="pred_drift_detected", value=False)
        ti.xcom_push(key="drift_summary",       value="No data")
        return

    df_ref     = pd.read_json(ref_json,     orient="records")
    df_current = pd.read_json(current_json, orient="records")

    # Align columns: chỉ check columns có ở cả 2 datasets
    common_feat = [c for c in FEATURE_COLS if c in df_ref.columns and c in df_current.columns]
    logger.info(f"Checking drift for {len(common_feat)} features")

    df_ref_feat     = df_ref[common_feat]
    df_current_feat = df_current[common_feat]

    # ── Data Drift Report ─────────────────────────────────────────────────
    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        reference_data=df_ref_feat,
        current_data=df_current_feat,
    )
    data_drift_result = data_drift_report.as_dict()

    # Trích xuất drift score: tỉ lệ features bị drift
    drift_metrics = data_drift_result["metrics"][0]["result"]
    n_drifted     = drift_metrics.get("number_of_drifted_columns", 0)
    n_total       = drift_metrics.get("number_of_columns", len(common_feat))
    data_drift_score = n_drifted / n_total if n_total > 0 else 0.0
    data_drift_detected = data_drift_score > DATA_DRIFT_THRESHOLD

    logger.info(
        f"Data Drift: {n_drifted}/{n_total} features drifted "
        f"({data_drift_score:.1%}) | threshold: {DATA_DRIFT_THRESHOLD:.1%} | "
        f"detected: {data_drift_detected}"
    )

    # ── Prediction Drift Report ───────────────────────────────────────────
    pred_drift_detected = False
    pred_drift_p_value  = 1.0

    if "predicted_fraud" in df_current.columns and "is_fraud_label" in df_ref.columns:
        # So sánh distribution của predicted_fraud (current) vs is_fraud_label (reference)
        df_ref_pred     = df_ref[["is_fraud_label"]].rename(columns={"is_fraud_label": "target"})
        df_current_pred = df_current[["predicted_fraud"]].rename(columns={"predicted_fraud": "target"})

        pred_drift_report = Report(metrics=[TargetDriftPreset()])
        pred_drift_report.run(
            reference_data=df_ref_pred,
            current_data=df_current_pred,
        )
        pred_result = pred_drift_report.as_dict()

        try:
            pred_drift_p_value  = pred_result["metrics"][0]["result"]["drift_detected"]
            pred_drift_detected = pred_drift_p_value < PREDICTION_DRIFT_THRESHOLD
        except (KeyError, TypeError):
            # Evidently trả về bool drift_detected trực tiếp trong một số versions
            try:
                pred_drift_detected = pred_result["metrics"][0]["result"].get("drift_detected", False)
            except Exception:
                pass

        logger.info(
            f"Prediction Drift: detected={pred_drift_detected} | "
            f"threshold: {PREDICTION_DRIFT_THRESHOLD}"
        )

    # ── Lưu HTML reports thành string để task sau upload S3 ──────────────
    # Data drift report
    data_html_path = "/tmp/data_drift_report.html"
    data_drift_report.save_html(data_html_path)
    with open(data_html_path, "r", encoding="utf-8") as f:
        data_drift_html = f.read()

    # Kết hợp kết quả
    overall_drift = data_drift_detected or pred_drift_detected

    drift_summary = {
        "data_drift_score":     data_drift_score,
        "n_drifted_features":   n_drifted,
        "n_total_features":     n_total,
        "data_drift_detected":  data_drift_detected,
        "pred_drift_detected":  pred_drift_detected,
        "overall_drift":        overall_drift,
        "threshold_data":       DATA_DRIFT_THRESHOLD,
        "threshold_pred":       PREDICTION_DRIFT_THRESHOLD,
        "reference_rows":       len(df_ref),
        "current_rows":         len(df_current),
        "lookback_days":        LOOKBACK_DAYS,
        "evaluated_at":         datetime.now(tz=timezone.utc).isoformat(),
    }

    logger.info(f"Drift summary: {json.dumps(drift_summary, indent=2)}")

    ti.xcom_push(key="drift_detected",      value=overall_drift)
    ti.xcom_push(key="data_drift_score",    value=data_drift_score)
    ti.xcom_push(key="pred_drift_detected", value=pred_drift_detected)
    ti.xcom_push(key="drift_summary",       value=json.dumps(drift_summary))
    ti.xcom_push(key="data_drift_html",     value=data_drift_html)


def save_report(**context):
    """
    Task 4: Lưu HTML report và JSON summary lên S3.

    Path:
      drift-reports/{YYYY-MM-DD}/data_drift_report.html
      drift-reports/{YYYY-MM-DD}/drift_summary.json
    """
    ti           = context["ti"]
    drift_html   = ti.xcom_pull(key="data_drift_html", task_ids="run_drift_detection")
    drift_summary = ti.xcom_pull(key="drift_summary",  task_ids="run_drift_detection")

    if not drift_html:
        logger.info("No report to save")
        return

    s3      = get_s3()
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    prefix   = f"{S3_DRIFT_REPORTS}/{date_str}"

    # Upload HTML report
    html_key = f"{prefix}/data_drift_report.html"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=html_key,
        Body=drift_html.encode("utf-8"),
        ContentType="text/html",
    )
    logger.info(f"Report saved: s3://{S3_BUCKET}/{html_key}")

    # Upload JSON summary
    if drift_summary:
        json_key = f"{prefix}/drift_summary.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=json_key,
            Body=drift_summary.encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Summary saved: s3://{S3_BUCKET}/{json_key}")

    ti.xcom_push(key="report_url", value=f"s3://{S3_BUCKET}/{html_key}")


def evaluate_and_alert(**context):
    """
    Task 5: Đánh giá kết quả drift và gửi alert phù hợp.
    """
    ti             = context["ti"]
    drift_detected = ti.xcom_pull(key="drift_detected",      task_ids="run_drift_detection")
    drift_summary  = ti.xcom_pull(key="drift_summary",       task_ids="run_drift_detection")
    report_url     = ti.xcom_pull(key="report_url",          task_ids="save_report")
    data_score     = ti.xcom_pull(key="data_drift_score",    task_ids="run_drift_detection")
    pred_drift     = ti.xcom_pull(key="pred_drift_detected", task_ids="run_drift_detection")

    summary = json.loads(drift_summary) if drift_summary else {}

    if drift_detected:
        send_alert(
            subject="🚨 Model Drift Detected — Retraining Triggered",
            message=(
                f"Drift detected in the last {LOOKBACK_DAYS} days.\n\n"
                f"Data Drift  : {summary.get('n_drifted_features', 0)}/{summary.get('n_total_features', 0)} features "
                f"({data_score:.1%})\n"
                f"Pred Drift  : {pred_drift}\n"
                f"Action      : Triggering retraining DAG now\n"
                f"Report      : {report_url or 'N/A'}"
            ),
            level="error",
            context={
                "data_drift_score":   f"{data_score:.1%}",
                "pred_drift":         str(pred_drift),
                "lookback_days":      str(LOOKBACK_DAYS),
            },
        )
    else:
        send_alert(
            subject="✅ No Drift Detected",
            message=(
                f"Daily drift check passed.\n\n"
                f"Data drift score: {data_score:.1%} (threshold: {DATA_DRIFT_THRESHOLD:.1%})\n"
                f"Prediction drift: {pred_drift}\n"
                f"Report: {report_url or 'N/A'}"
            ),
            level="info",
            context={"data_drift_score": f"{data_score:.1%}"},
        )


def trigger_retraining(**context):
    """
    Task 6: Nếu drift detected → gọi Airflow REST API trigger DAG training.

    Chỉ trigger nếu:
      1. Drift được phát hiện
      2. DAG training không đang chạy (tránh trigger chồng chéo)
    """
    ti             = context["ti"]
    drift_detected = ti.xcom_pull(key="drift_detected", task_ids="run_drift_detection")

    if not drift_detected:
        logger.info("No drift detected — skipping retraining trigger")
        return

    # ── Kiểm tra DAG training có đang chạy không ─────────────────────────
    check_url = f"{AIRFLOW_API_BASE}/api/v2/dags/fraud_model_training/dagRuns"
    try:
        resp = http_requests.get(
            check_url,
            params={"state": "running", "limit": 1},
            auth=(AIRFLOW_API_USER, AIRFLOW_API_PASS),
            timeout=15,
        )
        resp.raise_for_status()
        running_runs = resp.json().get("dag_runs", [])

        if running_runs:
            logger.info("Training DAG is already running — skipping trigger")
            return
    except Exception as e:
        logger.warning(f"Cannot check training DAG status: {e} — will trigger anyway")

    # ── Trigger DAG training qua REST API ────────────────────────────────
    trigger_url = f"{AIRFLOW_API_BASE}/api/v2/dags/fraud_model_training/dagRuns"
    payload = {
        "logical_date": datetime.now(tz=timezone.utc).isoformat(),
        "conf": {
            "trigger_reason":    "drift_detected",
            "triggered_by_dag":  "drift_detection",
            "drift_score":       str(ti.xcom_pull(key="data_drift_score",
                                                   task_ids="run_drift_detection")),
            "pred_drift":        str(ti.xcom_pull(key="pred_drift_detected",
                                                   task_ids="run_drift_detection")),
        },
    }

    try:
        resp = http_requests.post(
            trigger_url,
            json=payload,
            auth=(AIRFLOW_API_USER, AIRFLOW_API_PASS),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        triggered_run = resp.json()
        run_id = triggered_run.get("dag_run_id", "unknown")
        logger.info(f"✅ Training DAG triggered successfully | run_id: {run_id}")

        send_alert(
            subject="Training DAG Triggered by Drift Detection",
            message=f"fraud_model_training started (run_id: {run_id})",
            level="info",
            context={"run_id": run_id, "trigger": "drift_detection"},
        )
    except http_requests.exceptions.HTTPError as e:
        logger.error(f"Failed to trigger training DAG: {e} | response: {resp.text[:500]}")
        raise
    except Exception as e:
        logger.error(f"Failed to trigger training DAG: {e}")
        raise


# ── Task Definitions ──────────────────────────────────────────────────────────
with dag:
    t1_ref     = PythonOperator(task_id="load_reference_data",  python_callable=load_reference_data)
    t2_inf     = PythonOperator(task_id="load_inference_logs",  python_callable=load_inference_logs)
    t3_drift   = PythonOperator(task_id="run_drift_detection",  python_callable=run_drift_detection,
                                execution_timeout=timedelta(minutes=30))
    t4_report  = PythonOperator(task_id="save_report",          python_callable=save_report)
    t5_alert   = PythonOperator(task_id="evaluate_and_alert",   python_callable=evaluate_and_alert)
    t6_trigger = PythonOperator(task_id="trigger_retraining",   python_callable=trigger_retraining)

    # t1 và t2 chạy song song để load data nhanh hơn
    [t1_ref, t2_inf] >> t3_drift >> t4_report >> t5_alert >> t6_trigger
"""
dag_model_training.py
=====================
DAG: Training fraud detection model.

Hai nguồn trigger:
  1. Schedule định kỳ: mỗi thứ Hai 2 giờ sáng (0 2 * * 1)
  2. API call từ DAG drift_detection khi phát hiện drift

Full Retraining strategy với Weekly Merge:
  - Mỗi lần training kết thúc sẽ gộp toàn bộ batch lẻ của tuần đó
    thành 1 file merged-weekly/week_{date}/*.parquet
  - Lần training kế tiếp chỉ cần đọc các file weekly đã merge
    + batch lẻ của tuần hiện tại (chưa merge)
  → Tránh phải scan hàng ngàn batch folders mỗi lần training

Flow:
  1. collect_dataset        → Liệt kê weekly merged files + batch lẻ tuần này
  2. merge_current_batches  → Gộp batch lẻ tuần hiện tại → weekly file tạm
  3. run_training           → Load Kaggle + weekly files → train XGBoost
  4. evaluate_and_register  → So sánh F1, đẩy lên MLflow Registry nếu tốt hơn
  5. finalize_weekly_merge  → Đổi tên file tạm thành file chính thức
  6. notify                 → Gửi alert kết quả

Lưu ý file train_real_data.py gốc của bạn:
  - Đã giữ nguyên logic prepare_features(), Optuna, MLflow tracking
  - Chỉ thay phần load data và promotion logic
"""

import os
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from alert_utils import send_alert, airflow_failure_callback

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET        = os.environ.get("S3_BUCKET_NAME", "")
S3_FEATURE       = os.environ.get("S3_PREFIX_FEATURE", "feature-store")
S3_TRAINING_DATASET        = os.environ.get("S3_PREFIX_TRAINING_DATASET")           # prefix lưu file gộp theo tuần
S3_MERGED        = f"{S3_TRAINING_DATASET}/weekly"         
S3_BASELINE      = f"{S3_TRAINING_DATASET}/baseline/creditcard.csv"
MLFLOW_URI       = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
AWS_REGION       = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
MODEL_NAME       = "CreditCardFraudModel"
AIRFLOW_API_BASE = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
API_GATEWAY_URL = os.environ.get("API_GATEWAY_URL")

# Ngưỡng F1 để promote model mới lên Staging
PROMOTION_DELTA  = os.environ.get("PROMOTION_DELTA")   # model mới phải tốt hơn ít nhất 0.01

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
    dag_id="fraud_model_training",
    description="Weekly fraud model training with drift-triggered retraining support",
    # Schedule mỗi thứ Hai 2h sáng
    # Khi drift DAG trigger thì dùng API call với conf={"trigger_reason": "drift"}
    schedule="0 2 * * 1",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,          # Tránh 2 run training song song
    default_args=default_args,
    tags=["fraud-detection", "training", "mlops"],
    # Cho phép trigger từ bên ngoài (drift DAG)
    params={"trigger_reason": "scheduled"},
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_s3() -> boto3.client:
    return boto3.client("s3", region_name=AWS_REGION)


def get_week_key() -> str:
    """Tạo key cho weekly merge file: week_YYYYMMDD (ngày thứ Hai của tuần)."""
    today   = datetime.now(tz=timezone.utc).date()
    # Lùi về thứ Hai của tuần hiện tại
    monday  = today - timedelta(days=today.weekday())
    return f"week_{monday.strftime('%Y%m%d')}"


def list_merged_weekly_files() -> list[str]:
    """Liệt kê tất cả file training-dataset/weekly/week_YYYYMMDD/*.parquet trên S3, sắp xếp theo thời gian."""
    s3   = get_s3()
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_MERGED}/")
    objs = resp.get("Contents", [])
    keys = sorted(
        [o["Key"] for o in objs if o["Key"].endswith(".parquet")],
        # Sắp xếp theo tên file chứa ngày: week_YYYYMMDD/*.parquet
        key=lambda k: k.split("/")[-1]
    )
    return keys


def list_current_week_batches() -> list[str]:
    """
    Liệt kê tất cả batch folders trong feature-store/ của tuần hiện tại.
    Batch_id format: YYYYMMDD_HHMMSS
    Tuần hiện tại = từ thứ Hai tuần này đến lúc chạy.
    """
    s3 = get_s3()

    today  = datetime.now(tz=timezone.utc).date()
    monday = today - timedelta(days=today.weekday())
    cutoff = datetime.combine(monday, datetime.min.time()).replace(tzinfo=timezone.utc)

    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=f"{S3_FEATURE}/",
        Delimiter="/",
    )

    current_week_batch_paths = []
    for page in pages:
        for prefix_obj in page.get("CommonPrefixes", []):
            # prefix_obj["Prefix"] = "feature-store/20250113_120000/"
            batch_id = prefix_obj["Prefix"].rstrip("/").split("/")[-1]
            try:
                batch_time = datetime.strptime(batch_id, "%Y%m%d_%H%M%S").replace(
                    tzinfo=timezone.utc
                )
                if batch_time >= cutoff:
                    current_week_batch_paths.append(
                        f"s3a://{S3_BUCKET}/{S3_FEATURE}/{batch_id}/*.parquet"
                    )
            except ValueError:
                continue

    return sorted(current_week_batch_paths)


def prepare_features(df_input: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Giữ nguyên logic từ train_real_data.py gốc.
    Xử lý cả raw Kaggle CSV và feature-store parquet.
    """
    df = df_input.copy()

    # Kaggle raw CSV: có cột "Class", chưa có feature engineering
    if "Class" in df.columns and "is_fraud_label" not in df.columns:
        df["is_fraud_label"]    = df["Class"]
        df["hour_of_day"]       = (df["Time"] // 3600) % 24
        df["is_night_hour"]     = df["hour_of_day"].isin([22,23,0,1,2,3,4]).astype(int)
        df["amount_log1p"]      = np.log1p(df["Amount"])
        amount_mean             = df["Amount"].mean()
        amount_std              = df["Amount"].std()
        df["amount_normalized"] = (df["Amount"] - amount_mean) / (amount_std or 1.0)
        df["amount_zscore"]     = df["amount_normalized"]
        times        = df["Time"].values
        start_idx    = np.searchsorted(times, times - 3600, side="left")
        df["tx_count_1h"]    = (np.arange(len(times)) - start_idx + 1).astype(int)
        df["is_high_amount"] = (df["Amount"] > 500).astype(int)
        df["is_international"] = 0
        df["amount"]           = df["Amount"]

    # Sắp xếp theo thời gian
    if "Time" in df.columns:
        df = df.sort_values("Time").reset_index(drop=True)
    elif "timestamp" in df.columns:
        df = df.sort_values("timestamp").reset_index(drop=True)

    # Lọc chỉ giữ các cột feature và label
    available = [c for c in FEATURE_COLS if c in df.columns]
    missing   = [c for c in FEATURE_COLS if c not in df.columns]
    if missing:
        logger.warning(f"Missing feature columns (will be skipped): {missing}")

    X = df[available]
    y = df["is_fraud_label"]
    return X, y


# ── Task Functions ────────────────────────────────────────────────────────────

def collect_dataset(**context):
    """
    Task 1: Kiểm tra trigger reason và liệt kê data sources.

    Xác định:
      - Trigger từ schedule hay từ drift detection
      - Danh sách weekly merged files (tuần cũ)
      - Danh sách batch lẻ của tuần hiện tại
    """
    # Lấy trigger reason từ DAG conf
    trigger_reason = context["dag_run"].conf.get("trigger_reason", "scheduled")
    logger.info(f"Training triggered by: {trigger_reason}")

    # Danh sách weekly merged files (tuần đã hoàn thành)
    merged_files   = list_merged_weekly_files()
    # Batch lẻ tuần hiện tại (chưa merge)
    current_batches = list_current_week_batches()

    logger.info(f"Weekly merged files: {len(merged_files)}")
    logger.info(f"Current week batches (unmerged): {len(current_batches)}")

    if not merged_files and not current_batches:
        # Chỉ dùng Kaggle baseline
        logger.warning("No pipeline data found — will train on Kaggle baseline only")

    week_key = get_week_key()

    ti = context["ti"]
    ti.xcom_push(key="trigger_reason",   value=trigger_reason)
    ti.xcom_push(key="merged_files",     value=merged_files)
    ti.xcom_push(key="current_batches",  value=current_batches)
    ti.xcom_push(key="week_key",         value=week_key)
    ti.xcom_push(key="total_merged",     value=len(merged_files))
    ti.xcom_push(key="total_unmerged",   value=len(current_batches))


def merge_current_batches(**context):
    """
    Task 2: Gộp tất cả batch lẻ của tuần hiện tại thành 1 file parquet tạm.

    File tạm: merged-weekly/week_{date}_temp/*.parquet
    Sau khi training xong, task finalize_weekly_merge đổi tên thành chính thức.

    Dùng PySpark để đọc và gộp hiệu quả.
    Nếu không có batch lẻ → skip.
    """
    ti              = context["ti"]
    current_batches = ti.xcom_pull(key="current_batches", task_ids="collect_dataset")
    week_key        = ti.xcom_pull(key="week_key",        task_ids="collect_dataset")

    if not current_batches:
        logger.info("No current week batches to merge — skipping")
        ti.xcom_push(key="temp_merged_key", value=None)
        return

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("WeeklyMerge")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")   
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") 
        .config("spark.hadoop.fs.s3a.socket.timeout", "30000")
        .config("spark.hadoop.fs.s3a.paging.maximum.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.retry.interval", "5")
        .config("spark.hadoop.fs.s3a.retry.throttled.interval", "10")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        logger.info(f"Merging {len(current_batches)} batch paths...")

        # Đọc tất cả batch parquet — Spark tự xử lý wildcard paths
        df = spark.read.parquet(*current_batches)
        row_count = df.count()
        logger.info(f"Total rows in current week: {row_count}")

        # Ghi ra file temp trên S3
        temp_key    = f"{S3_MERGED}/{week_key}_temp"
        temp_s3path = f"s3a://{S3_BUCKET}/{temp_key}"

        df.coalesce(1).write.mode("overwrite").parquet(temp_s3path)
        logger.info(f"Temp merged file written: {temp_s3path}")

        ti.xcom_push(key="temp_merged_key", value=temp_key)
        ti.xcom_push(key="current_week_rows", value=row_count)

    finally:
        spark.stop()


def run_training(**context):
    """
    Task 3: Load toàn bộ dataset và train XGBoost với Optuna.

    Data sources:
      1. Kaggle baseline CSV từ S3
      2. Weekly merged parquet files (tuần đã hoàn thành)
      3. Temp merged file của tuần hiện tại

    Giữ nguyên logic Optuna + MLflow từ train_real_data.py gốc.
    """
    import mlflow
    import mlflow.xgboost
    import xgboost as xgb
    import optuna
    from sklearn.metrics import f1_score, classification_report

    ti            = context["ti"]
    merged_files  = ti.xcom_pull(key="merged_files",     task_ids="collect_dataset")
    temp_key      = ti.xcom_pull(key="temp_merged_key",  task_ids="merge_current_batches")
    week_key      = ti.xcom_pull(key="week_key",         task_ids="collect_dataset")
    trigger_reason = ti.xcom_pull(key="trigger_reason",  task_ids="collect_dataset")

    s3 = get_s3()

    # ── 1. Load Kaggle baseline ───────────────────────────────────────────
    logger.info("Loading Kaggle baseline CSV...")
    obj    = s3.get_object(Bucket=S3_BUCKET, Key=S3_BASELINE)
    df_kaggle = pd.read_csv(io.BytesIO(obj["Body"].read()))
    print(f"Kaggle rows: {len(df_kaggle)}")
    # Normalize label before concat so pd.concat doesn't produce NaN
    if "Class" in df_kaggle.columns and "is_fraud_label" not in df_kaggle.columns:
        df_kaggle["is_fraud_label"] = df_kaggle["Class"]
    all_dfs = [df_kaggle]

    # ── 2. Load weekly merged files ───────────────────────────────────────
    for s3_key in merged_files:
        # Bỏ qua file temp nếu có trong list (không nên xảy ra nhưng phòng thủ)
        if "_temp" in s3_key:
            continue
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            df_week = pd.read_parquet(io.BytesIO(obj["Body"].read()))
            all_dfs.append(df_week)
            logger.info(f"Loaded {s3_key}: {len(df_week)} rows")
        except Exception as e:
            logger.warning(f"Could not load {s3_key}: {e}")

    # ── 3. Load temp merged file của tuần hiện tại ────────────────────────
    if temp_key:
        try:
            # Spark ghi parquet thành folder, cần tìm file part-*.parquet bên trong
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=temp_key)
            part_files = [
                o["Key"] for o in resp.get("Contents", [])
                if o["Key"].endswith(".parquet") and "part-" in o["Key"]
            ]
            if part_files:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=part_files[0])
                df_current = pd.read_parquet(io.BytesIO(obj["Body"].read()))
                all_dfs.append(df_current)
                logger.info(f"Current week rows: {len(df_current)}")
        except Exception as e:
            logger.warning(f"Could not load temp merged file: {e}")

    # ── 4. Concat và prepare features ────────────────────────────────────
    df_combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Combined dataset: {len(df_combined)} rows")

    X, y = prepare_features(df_combined)

    # Train/Test split theo thứ tự thời gian (80/20)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    num_neg = int((y_train == 0).sum())
    num_pos = int((y_train == 1).sum())
    base_scale_weight = num_neg / num_pos if num_pos > 0 else 1.0
    logger.info(f"Train: {len(X_train)} | Test: {len(X_test)} | Imbalance ratio: {base_scale_weight:.2f}")

    # ── 5. MLflow setup ───────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("Fraud_Detection_Training")
    client = mlflow.tracking.MlflowClient()

    # Lấy F1 của model đang Production
    production_f1 = 0.0
    try:
        prod_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        if prod_versions:
            run = client.get_run(prod_versions[0].run_id)
            production_f1 = run.data.metrics.get("best_f1_score",
                            run.data.metrics.get("f1_score", 0.0))
            logger.info(f"Current Production F1: {production_f1:.4f}")
    except Exception as e:
        logger.warning(f"Cannot get production model F1: {e}")

    # ── 6. Optuna optimization ────────────────────────────────────────────
    run_name = f"training_{week_key}_{trigger_reason}"

    with mlflow.start_run(run_name=run_name) as parent_run:
        mlflow.log_param("trigger_reason",    trigger_reason)
        mlflow.log_param("total_rows",        len(X))
        mlflow.log_param("train_rows",        len(X_train))
        mlflow.log_param("test_rows",         len(X_test))
        mlflow.log_param("fraud_train",       num_pos)
        mlflow.log_param("merged_weeks",      len(merged_files))
        mlflow.log_param("production_f1",     production_f1)

        def objective(trial):
            params = {
                "objective":        "binary:logistic",
                "eval_metric":      "logloss",
                "max_depth":        trial.suggest_int("max_depth", 3, 9),
                "learning_rate":    trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                "n_estimators":     trial.suggest_int("n_estimators", 50, 150),
                "scale_pos_weight": trial.suggest_float(
                    "scale_pos_weight",
                    base_scale_weight * 0.5,
                    base_scale_weight * 1.5
                ),
                "random_state": 42,
            }
            with mlflow.start_run(run_name=f"trial_{trial.number}", nested=True):
                mlflow.log_params(params)
                model = xgb.XGBClassifier(**params)
                model.fit(X_train, y_train)
                preds = model.predict(X_test)
                f1    = f1_score(y_test, preds, zero_division=0)
                mlflow.log_metric("f1_score", f1)
            return f1

        logger.info("Starting Optuna (15 trials)...")
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=15)

        best_params = study.best_params
        best_f1     = study.best_value
        logger.info(f"Best trial F1: {best_f1:.4f} | params: {best_params}")

        mlflow.log_params(best_params)
        mlflow.log_metric("best_f1_score", best_f1)

        # ── 7. Train final model ──────────────────────────────────────────
        final_model = xgb.XGBClassifier(**best_params)
        final_model.fit(X_train, y_train)
        preds    = final_model.predict(X_test)
        final_f1 = f1_score(y_test, preds, zero_division=0)

        logger.info(f"Final model F1: {final_f1:.4f}")
        logger.info(f"\n{classification_report(y_test, preds)}")
        mlflow.log_metric("final_f1_score", final_f1)
        mlflow.xgboost.log_model(xgb_model=final_model, artifact_path="model")

        # Lưu scaler stats
        amount_mean = df_kaggle["Amount"].mean()
        amount_std  = df_kaggle["Amount"].std()
        scaler_stats = {
            "amount_mean": float(amount_mean),
            "amount_std":  float(amount_std if amount_std > 0 else 1.0),
        }
        with open("/tmp/scaler_stats.json", "w") as f:
            json.dump(scaler_stats, f)
        mlflow.log_artifact("/tmp/scaler_stats.json", artifact_path="model")

        # ── 8. Promotion logic ────────────────────────────────────────────
        promotion_threshold = production_f1 + PROMOTION_DELTA
        run_id = parent_run.info.run_id

        if final_f1 >= promotion_threshold or production_f1 == 0.0:
            logger.info(f"Promoting: {final_f1:.4f} >= {promotion_threshold:.4f}")
            model_uri     = f"runs:/{run_id}/model"
            model_version = mlflow.register_model(model_uri, MODEL_NAME)
            new_version   = model_version.version

            client.transition_model_version_stage(
                name=MODEL_NAME, version=new_version, stage="Staging"
            )
            logger.info(f"Model v{new_version} → Staging")
            promoted = True
        else:
            logger.info(f"Not promoted: {final_f1:.4f} < {promotion_threshold:.4f}")
            promoted = False

        # Push kết quả cho task sau
        ti.xcom_push(key="final_f1",   value=final_f1)
        ti.xcom_push(key="promoted",   value=promoted)
        ti.xcom_push(key="mlflow_run_id", value=run_id)
        ti.xcom_push(key="total_rows", value=len(X))

    Variable.set("last_training_timestamp", datetime.now(tz=timezone.utc).isoformat())
    Variable.set("last_training_f1", str(final_f1))


def finalize_weekly_merge(**context):
    """
    Task 4: Sau khi training thành công, đổi tên file tạm thành file chính thức.

    week_{date}_temp → week_{date}

    Lý do làm sau khi training (không phải trước):
    Nếu training fail, file temp vẫn có thể dùng lại ở lần retry
    mà không cần merge lại từ 2016 batches.
    """
    ti       = context["ti"]
    temp_key = ti.xcom_pull(key="temp_merged_key", task_ids="merge_current_batches")
    week_key = ti.xcom_pull(key="week_key",        task_ids="collect_dataset")

    if not temp_key:
        logger.info("No temp merged file to finalize")
        return

    s3 = get_s3()

    # Lấy danh sách tất cả file trong folder temp (Spark ghi thành folder)
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=temp_key)
    objects = resp.get("Contents", [])

    final_prefix = f"{S3_MERGED}/{week_key}"

    for obj in objects:
        old_key = obj["Key"]
        # Đổi prefix từ _temp sang chính thức
        new_key = old_key.replace(f"{week_key}_temp", f"{week_key}")
        # Copy
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={"Bucket": S3_BUCKET, "Key": old_key},
            Key=new_key,
        )
        # Xóa file temp
        s3.delete_object(Bucket=S3_BUCKET, Key=old_key)

    logger.info(f"Weekly merge finalized: s3://{S3_BUCKET}/{final_prefix}")


def notify(**context):
    """Task 5: Gửi alert tổng kết."""
    ti             = context["ti"]
    final_f1       = ti.xcom_pull(key="final_f1",       task_ids="run_training")
    promoted       = ti.xcom_pull(key="promoted",        task_ids="run_training")
    total_rows     = ti.xcom_pull(key="total_rows",      task_ids="run_training")
    trigger_reason = ti.xcom_pull(key="trigger_reason",  task_ids="collect_dataset")
    total_merged   = ti.xcom_pull(key="total_merged",    task_ids="collect_dataset")
    total_unmerged = ti.xcom_pull(key="total_unmerged",  task_ids="collect_dataset")

    status = "PROMOTED → Staging" if promoted else "Not promoted (below threshold)"

    send_alert(
        subject=f"Training Complete — F1: {final_f1:.4f} | {status}",
        message=(
            f"Fraud model training finished.\n\n"
            f"Trigger    : {trigger_reason}\n"
            f"Dataset    : {total_rows:,} rows\n"
            f"Data sources: {total_merged} weekly files + {total_unmerged} current batches\n"
            f"Final F1   : {final_f1:.4f}\n"
            f"Status     : {status}"
        ),
        level="success" if promoted else "info",
        context={
            "trigger_reason": trigger_reason,
            "final_f1":       f"{final_f1:.4f}",
            "promoted":       str(promoted),
            "total_rows":     f"{total_rows:,}",
        },
    )


# ── Task Definitions ──────────────────────────────────────────────────────────
with dag:
    t1_collect  = PythonOperator(task_id="collect_dataset",       python_callable=collect_dataset)
    t2_merge    = PythonOperator(task_id="merge_current_batches", python_callable=merge_current_batches,
                                 execution_timeout=timedelta(hours=1))
    t3_train    = PythonOperator(task_id="run_training",          python_callable=run_training,
                                 execution_timeout=timedelta(hours=3))
    t4_finalize = PythonOperator(task_id="finalize_weekly_merge", python_callable=finalize_weekly_merge)
    t5_notify   = PythonOperator(task_id="notify",                python_callable=notify)

    t1_collect >> t2_merge >> t3_train >> t4_finalize >> t5_notify
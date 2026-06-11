"""
fraud_pipeline_monitor.py
==========================
Airflow DAG monitoring: Kiểm tra sức khỏe của pipeline và S3 data.

Schedule: Mỗi 15 phút
Checks:
  1. check_recent_batches     → Xem có batch nào chạy thành công trong 30 phút qua không
  2. check_s3_data_freshness  → Kiểm tra S3 có data mới không
  3. check_dead_letter        → Xem dead-letter folder có tăng lên không
  4. check_rds_connectivity   → Ping RDS
  5. check_pipeline_lag       → Tính toán lag giữa extracted_time và current time
  6. send_health_report       → Gửi báo cáo tổng hợp
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import psycopg2
from airflow import DAG
from airflow.models import Variable, DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State

import sys
sys.path.insert(0, "/opt/airflow/plugins")
from alert_utils import send_alert, airflow_failure_callback

logger = logging.getLogger(__name__)

S3_BUCKET      = os.environ["S3_BUCKET"]
S3_FEATURE     = os.environ.get("S3_FEATURE_PREFIX", "feature-store")
S3_DEAD_LETTER = os.environ.get("S3_DEAD_LETTER_PREFIX", "dead-letter")
AWS_REGION     = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")

default_args = {
    "owner": "mlops-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": airflow_failure_callback,
}

monitor_dag = DAG(
    dag_id="fraud_pipeline_monitor",
    description="Health monitoring for fraud detection preprocessing pipeline",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fraud-detection", "monitoring", "mlops"],
)


def check_recent_batches(**context):
    """
    Kiểm tra DAG 'fraud_detection_preprocessing' có chạy thành công
    trong 30 phút qua không.
    """
    from airflow.models import DagRun
    from airflow.utils.session import create_session

    threshold = datetime.now(tz=timezone.utc) - timedelta(minutes=30)

    with create_session() as session:
        recent_runs = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "fraud_detection_preprocessing",
                DagRun.execution_date >= threshold,
            )
            .order_by(DagRun.execution_date.desc())
            .limit(10)
            .all()
        )

    if not recent_runs:
        send_alert(
            subject="No pipeline runs in last 30 minutes!",
            message="fraud_detection_preprocessing has not executed in 30 minutes. Check scheduler.",
            level="error",
            context={"threshold": threshold.isoformat()}
        )
        context["ti"].xcom_push(key="pipeline_status", value="no_runs")
        return

    success_runs = [r for r in recent_runs if r.state == State.SUCCESS]
    failed_runs  = [r for r in recent_runs if r.state == State.FAILED]

    logger.info(f"Recent runs: {len(recent_runs)} | success: {len(success_runs)} | failed: {len(failed_runs)}")

    if failed_runs and not success_runs:
        send_alert(
            subject=f"Pipeline FAILING — {len(failed_runs)} consecutive failures",
            message=f"Last {len(failed_runs)} runs all failed. Immediate attention required.",
            level="error",
            context={"failed_count": len(failed_runs), "last_run": str(failed_runs[0].execution_date)}
        )
    elif failed_runs:
        send_alert(
            subject=f"Pipeline has {len(failed_runs)} recent failures",
            message=f"Some runs failed but pipeline is still operational.",
            level="warning",
            context={"failed": len(failed_runs), "success": len(success_runs)}
        )

    context["ti"].xcom_push(key="pipeline_status", value="ok")
    context["ti"].xcom_push(key="success_count", value=len(success_runs))
    context["ti"].xcom_push(key="failed_count", value=len(failed_runs))


def check_s3_data_freshness(**context):
    """
    Kiểm tra S3 feature-store có object nào được tạo trong 30 phút qua không.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    threshold = datetime.now(tz=timezone.utc) - timedelta(minutes=30)

    try:
        resp = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{S3_FEATURE}/",
            MaxKeys=50,
        )
        objects = resp.get("Contents", [])

        if not objects:
            send_alert(
                subject="S3 Feature Store is empty!",
                message=f"No objects found in s3://{S3_BUCKET}/{S3_FEATURE}/",
                level="error",
            )
            return

        # Tìm object mới nhất
        latest = max(objects, key=lambda o: o["LastModified"])
        latest_time = latest["LastModified"]

        if latest_time < threshold:
            age_minutes = (datetime.now(tz=timezone.utc) - latest_time).seconds // 60
            send_alert(
                subject=f"S3 data is stale — last update {age_minutes}min ago",
                message=f"Latest feature store data: {latest['Key']}\nTime: {latest_time}",
                level="warning",
                context={"last_modified": str(latest_time), "age_minutes": age_minutes}
            )
        else:
            logger.info(f"S3 data is fresh. Latest: {latest['Key']} at {latest_time}")

        context["ti"].xcom_push(key="s3_latest_key", value=latest["Key"])
        context["ti"].xcom_push(key="s3_object_count", value=len(objects))

    except Exception as e:
        logger.error(f"S3 check failed: {e}")
        send_alert(subject="S3 check error", message=str(e), level="error")


def check_dead_letter(**context):
    """
    Kiểm tra dead-letter folder có tăng lên so với lần check trước không.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)

    try:
        resp = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=f"{S3_DEAD_LETTER}/",
        )
        current_count = resp.get("KeyCount", 0)

        # Lấy count trước từ Variable
        prev_count = int(Variable.get("dead_letter_count", default_var="0"))

        if current_count > prev_count:
            new_items = current_count - prev_count
            logger.warning(f"Dead letter increased by {new_items} (total: {current_count})")

            # Lấy các object mới nhất
            objects = sorted(
                resp.get("Contents", []),
                key=lambda o: o["LastModified"],
                reverse=True
            )[:5]
            new_keys = [o["Key"] for o in objects]

            send_alert(
                subject=f"Dead Letter increased by {new_items} items",
                message=f"New dead letters detected. Bad data batches need review.\n\nLatest:\n" + "\n".join(new_keys),
                level="warning",
                context={"new_items": new_items, "total": current_count}
            )

        Variable.set("dead_letter_count", current_count)
        context["ti"].xcom_push(key="dead_letter_count", value=current_count)
        logger.info(f"Dead letter count: {current_count}")

    except Exception as e:
        logger.error(f"Dead letter check failed: {e}")


def check_rds_connectivity(**context):
    """Ping RDS để đảm bảo kết nối bình thường."""
    try:
        conn = psycopg2.connect(
            host=os.environ["RDS_HOST"],
            port=int(os.environ.get("RDS_PORT", 5432)),
            database=os.environ["RDS_DB"],
            user=os.environ["RDS_USER"],
            password=os.environ["RDS_PASSWORD"],
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM transactions WHERE created_at > NOW() - INTERVAL '30 minutes'")
            recent_count = cur.fetchone()[0]
        conn.close()
        logger.info(f"RDS OK — {recent_count} transactions in last 30 min")
        context["ti"].xcom_push(key="rds_status", value="ok")
        context["ti"].xcom_push(key="recent_tx_count", value=recent_count)

        if recent_count == 0:
            send_alert(
                subject="No new transactions in RDS (last 30min)",
                message="Transaction simulator may have stopped. Check simulate_transactions.py",
                level="warning",
            )
    except Exception as e:
        logger.error(f"RDS connectivity check failed: {e}")
        send_alert(
            subject="RDS Connection FAILED",
            message=f"Cannot connect to RDS PostgreSQL.\nError: {e}",
            level="error",
        )
        context["ti"].xcom_push(key="rds_status", value="failed")
        raise


def send_health_report(**context):
    """Gửi báo cáo tổng hợp sức khỏe pipeline."""
    ti = context["ti"]

    pipeline_status = ti.xcom_pull(key="pipeline_status", task_ids="check_recent_batches") or "unknown"
    success_count   = ti.xcom_pull(key="success_count",   task_ids="check_recent_batches") or 0
    failed_count    = ti.xcom_pull(key="failed_count",    task_ids="check_recent_batches") or 0
    s3_count        = ti.xcom_pull(key="s3_object_count", task_ids="check_s3_data_freshness") or 0
    dead_letter     = ti.xcom_pull(key="dead_letter_count", task_ids="check_dead_letter") or 0
    rds_status      = ti.xcom_pull(key="rds_status",      task_ids="check_rds_connectivity") or "unknown"
    recent_tx       = ti.xcom_pull(key="recent_tx_count", task_ids="check_rds_connectivity") or 0

    overall_ok = (
        pipeline_status == "ok"
        and rds_status == "ok"
        and failed_count == 0
    )

    report = f"""
=== FRAUD DETECTION PIPELINE HEALTH REPORT ===
Time: {datetime.now(tz=timezone.utc).isoformat()} UTC

Pipeline Status:
  - DAG runs (last 30min): {success_count} success, {failed_count} failed
  - Overall: {"✅ HEALTHY" if overall_ok else "⚠️ DEGRADED"}

Data:
  - RDS status:          {rds_status.upper()}
  - Recent transactions: {recent_tx} (last 30min)
  - S3 feature objects:  {s3_count}
  - Dead letter count:   {dead_letter}
"""

    level = "success" if overall_ok else "warning"
    send_alert(
        subject="Pipeline Health Report",
        message=report,
        level=level,
    )
    logger.info(report)


# ── Task Definitions ─────────────────────────────────────────────────────────
with monitor_dag:
    t1 = PythonOperator(task_id="check_recent_batches",    python_callable=check_recent_batches)
    t2 = PythonOperator(task_id="check_s3_data_freshness", python_callable=check_s3_data_freshness)
    t3 = PythonOperator(task_id="check_dead_letter",       python_callable=check_dead_letter)
    t4 = PythonOperator(task_id="check_rds_connectivity",  python_callable=check_rds_connectivity)
    t5 = PythonOperator(task_id="send_health_report",      python_callable=send_health_report)

    [t1, t2, t3, t4] >> t5
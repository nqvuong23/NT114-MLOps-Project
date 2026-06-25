"""
alert_utils.py
==============
Utility functions để gửi alert qua Slack và Email.
Dùng trong Airflow DAGs và Spark jobs.
"""

import os
import smtplib
import logging
import requests
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

from slack_sdk.webhook import WebhookClient

logger = logging.getLogger(__name__)


def send_slack_alert(message: str, level: str = "warning", context: dict = None):
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return

    webhook = WebhookClient(webhook_url)

    color_map = {
        "info":    "#36a64f",
        "warning": "#ff9500",
        "error":   "#ff0000",
        "success": "#2eb886",
    }
    emoji_map = {
        "info":    ":information_source:",
        "warning": ":warning:",
        "error":   ":rotating_light:",
        "success": ":white_check_mark:",
    }

    fields = []
    if context:
        for k, v in context.items():
            fields.append({"title": k, "value": str(v), "short": True})

    # Slack Block Kit basic attachment
    attachments = [
        {
            "color": color_map.get(level, "#808080"),
            "pretext": f"{emoji_map.get(level, '')} *Fraud Detection MLOps Alert*",
            "text": message,
            "fields": fields,
            "footer": "Fraud Detection Pipeline",
            "ts": int(datetime.now(timezone.utc).timestamp()),
        }
    ]

    try:
        response = webhook.send(attachments=attachments)
        if response.status_code == 200:
            logger.info(f"Slack alert sent: [{level}] {message[:80]}")
        else:
            logger.error(f"Slack alert failed with status: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")

def send_email_alert(subject: str, body: str, level: str = "warning"):
    """
    Gửi alert email qua SMTP.

    Args:
        subject: Tiêu đề email
        body   : Nội dung email (plain text hoặc HTML)
        level  : 'info' | 'warning' | 'error' | 'success'
    """
    smtp_host  = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port  = int(os.environ.get("SMTP_PORT", 587))
    smtp_user  = os.environ.get("SMTP_USER", "")
    smtp_pass  = os.environ.get("SMTP_PASSWORD", "")
    alert_email = os.environ.get("ALERT_EMAIL", smtp_user)

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP credentials not set — skipping email alert")
        return

    prefix_map = {
        "info":    "[INFO]",
        "warning": "[WARNING]",
        "error":   "[ERROR]",
        "success": "[SUCCESS]",
    }
    full_subject = f"{prefix_map.get(level, '[ALERT]')} Fraud Detection Pipeline: {subject}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = full_subject
    msg["From"]    = smtp_user
    msg["To"]      = alert_email

    # FIX: datetime.utcnow() → datetime.now(timezone.utc)
    now_str = datetime.now(timezone.utc).isoformat()
    color = "red" if level == "error" else "orange" if level == "warning" else "green"
    html_body = f"""
    <html><body>
    <h2 style="color: {color};">Fraud Detection MLOps Alert</h2>
    <p><strong>Level:</strong> {level.upper()}</p>
    <p><strong>Time:</strong> {now_str} UTC</p>
    <hr>
    <pre>{body}</pre>
    <hr>
    <small>Fraud Detection Pipeline</small>
    </body></html>
    """
    msg.attach(MIMEText(body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, alert_email, msg.as_string())
        logger.info(f"Email alert sent: {full_subject}")
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")


def send_alert(
    subject: str,
    message: str,
    level: str = "warning",
    context: dict = None,
):
    """Gửi alert qua cả Slack và Email cùng lúc."""
    # send_slack_alert(f"*{subject}*\n{message}", level=level, context=context)
    body = message
    if context:
        body += "\n\nDetails:\n" + "\n".join(f"  {k}: {v}" for k, v in context.items())
    send_email_alert(subject, body, level=level)


def airflow_failure_callback(context):
    """
    Callback cho Airflow task on_failure_callback.

    FIX Airflow 3.x:
      - execution_date bị xóa → dùng logical_date
      - context keys thay đổi: "dag" → "dag_run.dag_id" hoặc dùng task_instance
    """
    ti        = context.get("task_instance")
    task_id   = ti.task_id if ti else "unknown"
    dag_id    = ti.dag_id  if ti else "unknown"
    # FIX: execution_date → logical_date
    run_date  = context.get("logical_date") or context.get("execution_date")
    exception = context.get("exception")
    log_url   = ti.log_url if ti else "N/A"

    send_alert(
        subject=f"Task Failed: {dag_id}.{task_id}",
        message=f"Airflow task failed.\n\nException: {exception}\nLog: {log_url}",
        level="error",
        context={
            "DAG":            dag_id,
            "Task":           task_id,
            "Logical Date":   str(run_date),
        },
    )


# NOTE: airflow_sla_miss_callback đã bị xóa hoàn toàn trong Airflow 3.x
# (SLA feature bị remove). Hàm dưới chỉ là stub để tránh ImportError
# nếu có chỗ nào còn import nó — KHÔNG đăng ký vào DAG.
def airflow_sla_miss_callback(*args, **kwargs):
    """STUB — SLA feature bị xóa trong Airflow 3.x. Không dùng."""
    logger.warning("airflow_sla_miss_callback called but SLA is removed in Airflow 3.x")
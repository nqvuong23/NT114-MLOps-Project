"""
spark_s3_upload.py
==================
PySpark utility: Upload a pandas / Spark DataFrame vào S3 dưới dạng một
single parquet file (coalesce(1)).

Dùng pip install pyspark — import trực tiếp từ DAG, không cần spark-submit.

Design decisions:
  - Nhận pandas DataFrame rồi convert sang Spark để ghi qua s3a://, giống
    cách spark_cleaning.py và spark_feature_engineering.py sử dụng Hadoop-AWS.
  - Luôn coalesce(1) để output là 1 file duy nhất → downstream task chỉ cần
    đọc 1 key cụ thể, không cần clear_s3_prefix khi muốn ghi đè.
  - Trả về s3a:// path thực sự đã ghi để downstream dùng làm input_path.
"""

import logging
import os
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")


def get_spark_session(app_name: str = "FraudDetection-S3Upload") -> SparkSession:
    """
    Lấy hoặc tạo SparkSession với Hadoop-AWS để ghi lên S3.
    Dùng getOrCreate() để tái sử dụng session nếu đã tồn tại trong process.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        # Hadoop-AWS để ghi lên S3 qua s3a://
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "30000")
        .config("spark.hadoop.fs.s3a.paging.maximum.timeout", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
        .config("spark.hadoop.fs.s3a.retry.interval", "5")
        .config("spark.hadoop.fs.s3a.retry.throttled.interval", "10")
        # Tắt Spark UI khi chạy trong Airflow worker
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def upload_pandas_df_to_s3(
    df: pd.DataFrame,
    s3_output_path: str,
    filename: str = "data.parquet",
    spark: SparkSession = None,
) -> str:
    """
    Upload pandas DataFrame lên S3 dưới dạng single parquet file qua PySpark.

    Args:
        df            : pandas DataFrame cần upload.
        s3_output_path: S3 prefix dạng s3a://bucket/prefix/batch_id
                        (KHÔNG có trailing slash).
        filename      : Tên file parquet cuối cùng, mặc định "data.parquet".
                        Output thực tế: {s3_output_path}/{filename}
        spark         : SparkSession (tạo mới nếu None).

    Returns:
        s3a:// full path tới file đã ghi, ví dụ:
        "s3a://my-bucket/raw-data/20250101_120000/data.parquet"

    Notes:
        - coalesce(1) đảm bảo output là 1 file duy nhất, không có _SUCCESS hay
          part-* nhiều file → downstream không cần clear_s3_prefix.
        - Nếu key đã tồn tại, PySpark sẽ overwrite (mode="overwrite").
    """
    should_stop = spark is None
    if spark is None:
        spark = get_spark_session()

    try:
        logger.info(
            f"Uploading pandas DataFrame ({len(df)} rows, {len(df.columns)} cols) "
            f"to {s3_output_path}/{filename}"
        )

        # Convert pandas → Spark DataFrame
        # spark.createDataFrame tự suy kiểu dữ liệu từ pandas schema
        spark_df = spark.createDataFrame(df)

        # Ghi single file — tên file xác định bởi `filename`
        # Dùng temporary prefix rồi rename không khả thi trên S3 với Hadoop;
        # thay vào đó ghi thẳng vào key đích với 1 partition.
        full_output_path = f"{s3_output_path.rstrip('/')}/{filename}"
        spark_df.coalesce(1).write.mode("overwrite").parquet(full_output_path)

        logger.info(f"Successfully uploaded to: {full_output_path}")
        return full_output_path

    finally:
        if should_stop:
            spark.stop()

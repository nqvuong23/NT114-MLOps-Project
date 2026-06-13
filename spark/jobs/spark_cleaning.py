"""
spark_cleaning.py
=================
Spark job: Cleaning & Transformation cho raw transaction data.

Khi dùng pip install pyspark:
  - KHÔNG cần spark-submit
  - Import trực tiếp hàm clean_and_transform() từ DAG
  - SparkSession tự tìm Java qua JAVA_HOME
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

logger = logging.getLogger(__name__)

V_COLS = [f"V{i}" for i in range(1, 29)]

VALID_CATEGORIES = {
    "grocery", "online", "travel", "restaurant", "entertainment",
    "gas_station", "pharmacy", "electronics", "clothing", "atm"
}


def get_spark_session(app_name: str = "FraudDetection-Cleaning") -> SparkSession:
    """
    Tạo SparkSession khi dùng pip install pyspark.
    Không cần spark-submit hay SPARK_HOME.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        # S3 support — cần hadoop-aws jar
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # Tắt UI để nhẹ hơn khi chạy trong Airflow worker
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def clean_and_transform(
    input_path: str,
    output_path: str,
    spark: SparkSession = None,
) -> tuple[int, int]:
    """
    Đọc raw parquet từ S3, cleaning & transformation, ghi processed parquet.

    Args:
        input_path : s3a://bucket/raw-data/{batch_id}/raw.parquet
        output_path: s3a://bucket/processed-data/{batch_id}/processed.parquet
        spark      : SparkSession (tạo mới nếu None)

    Returns:
        (row_count, fraud_count)
    """
    should_stop = spark is None
    if spark is None:
        spark = get_spark_session()

    try:
        logger.info(f"Reading raw data: {input_path}")
        df = spark.read.parquet(input_path)

        original_count = df.count()
        logger.info(f"Raw rows: {original_count}")

        # ── Drop duplicates ────────────────────────────────────────────────
        df = df.dropDuplicates(["transaction_id"])

        # ── Drop null ở critical columns ──────────────────────────────────
        critical = ["transaction_id", "user_id", "card_id",
                    "timestamp", "amount", "is_fraud_label"] + V_COLS
        df = df.dropna(subset=critical)

        # ── Fill remaining nulls ───────────────────────────────────────────
        df = df.fillna({
            "merchant_id": "UNKNOWN",
            "merchant_category": "other",
            "is_international": False,
            "hour_of_day": -1,
        })

        # ── Chuẩn hóa timestamp ───────────────────────────────────────────
        df = df.withColumn(
            "timestamp",
            F.date_format(F.to_timestamp("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
        )

        # ── is_international → int ────────────────────────────────────────
        df = df.withColumn("is_international", F.col("is_international").cast(IntegerType()))

        # ── Clamp amount ──────────────────────────────────────────────────
        df = df.filter(F.col("amount") >= 0)
        df = df.withColumn(
            "amount",
            F.when(F.col("amount") > 50000, 50000.0).otherwise(F.col("amount"))
        )

        # ── Chuẩn hóa merchant_category ──────────────────────────────────
        df = df.withColumn(
            "merchant_category",
            F.when(F.col("merchant_category").isin(list(VALID_CATEGORIES)),
                   F.col("merchant_category")).otherwise(F.lit("other"))
        )

        # ── Normalize amount (StandardScaler) ─────────────────────────────
        assembler = VectorAssembler(inputCols=["amount"], outputCol="amount_vec")
        scaler = StandardScaler(
            inputCol="amount_vec", outputCol="amount_scaled_vec",
            withMean=True, withStd=True
        )
        pipeline = Pipeline(stages=[assembler, scaler])
        model = pipeline.fit(df)
        df = model.transform(df)

        from pyspark.ml.linalg import DenseVector, SparseVector

        @F.udf(DoubleType())
        def extract_first(vec):
            if vec is None:
                return 0.0
            if isinstance(vec, (DenseVector, SparseVector)):
                return float(vec.toArray()[0])
            try:
                # Fallback nếu nó biến thành list
                return float(vec[0])
            except:
                return 0.0

        df = df.withColumn("amount_normalized", extract_first("amount_scaled_vec"))
        df = df.drop("amount_vec", "amount_scaled_vec")

        # ── Cast is_fraud_label ───────────────────────────────────────────
        df = df.withColumn("is_fraud_label", F.col("is_fraud_label").cast(IntegerType()))

        # ── Select output columns ─────────────────────────────────────────
        output_cols = (
            ["transaction_id", "user_id", "card_id", "timestamp",
             "amount", "amount_normalized", "merchant_id", "merchant_category",
             "is_international", "hour_of_day"]
            + V_COLS + ["is_fraud_label"]
        )
        df = df.select(output_cols)

        row_count = df.count()
        fraud_count = df.filter(F.col("is_fraud_label") == 1).count()
        logger.info(f"Processed: {row_count} rows | fraud: {fraud_count}")

        # ── Ghi S3 ────────────────────────────────────────────────────────
        df.coalesce(1).write.mode("overwrite").parquet(output_path)
        logger.info(f"Written to: {output_path}")

        return row_count, fraud_count

    finally:
        # Chỉ stop nếu chúng ta tạo session — không stop session của caller
        if should_stop:
            spark.stop()
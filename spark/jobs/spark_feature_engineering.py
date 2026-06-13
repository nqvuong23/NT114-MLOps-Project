"""
spark_feature_engineering.py
=============================
Spark job: Feature Engineering từ processed data.

Dùng pip install pyspark — import trực tiếp, không cần spark-submit.
"""

import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logger = logging.getLogger(__name__)

V_COLS = [f"V{i}" for i in range(1, 29)]
NIGHT_HOURS = [22, 23, 0, 1, 2, 3, 4]


def get_spark_session(app_name: str = "FraudDetection-FeatureEng") -> SparkSession:
    """Lấy hoặc tạo SparkSession (getOrCreate tránh tạo duplicate)."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def feature_engineering(
    input_path: str,
    output_path: str,
    spark: SparkSession = None,
) -> tuple[int, int]:
    """
    Đọc processed parquet, tính features, ghi feature parquet.

    Returns:
        (row_count, fraud_count)
    """
    should_stop = spark is None
    if spark is None:
        spark = get_spark_session()

    try:
        logger.info(f"Reading processed data: {input_path}")
        df = spark.read.parquet(input_path)
        df = df.withColumn("timestamp_ts", F.to_timestamp("timestamp"))

        # ── Feature 1: is_night_hour ───────────────────────────────────────
        df = df.withColumn(
            "is_night_hour",
            F.when(F.col("hour_of_day").isin(NIGHT_HOURS), 1).otherwise(0)
        )

        # ── Feature 2: amount_log1p ────────────────────────────────────────
        df = df.withColumn("amount_log1p", F.log1p("amount"))

        # ── Feature 3: amount_zscore per card ─────────────────────────────
        card_window = Window.partitionBy("card_id")
        df = (df
              .withColumn("_card_mean", F.avg("amount").over(card_window))
              .withColumn("_card_std",  F.stddev("amount").over(card_window))
              .withColumn("amount_zscore",
                          F.when(F.col("_card_std") > 0,
                                 (F.col("amount") - F.col("_card_mean")) / F.col("_card_std"))
                          .otherwise(F.lit(0.0)))
              .drop("_card_mean", "_card_std"))

        # ── Feature 4: tx_count_1h per card ───────────────────────────────
        df = df.withColumn("epoch_ts", F.col("timestamp_ts").cast("long"))
        one_hour_window = (
            Window.partitionBy("card_id")
            .orderBy("epoch_ts")
            .rangeBetween(-3600, 0)
        )
        df = df.withColumn(
            "tx_count_1h",
            F.count("transaction_id").over(one_hour_window).cast(IntegerType())
        )

        # ── Feature 5: is_high_amount ──────────────────────────────────────
        df = df.withColumn(
            "is_high_amount",
            F.when(F.col("amount") > 500, 1).otherwise(0)
        )

        # ── Cleanup temp columns ───────────────────────────────────────────
        df = df.drop("timestamp_ts", "epoch_ts")

        # ── Select output ──────────────────────────────────────────────────
        feature_cols = (
            ["transaction_id", "user_id", "card_id", "timestamp",
             "amount", "amount_normalized", "amount_log1p", "amount_zscore",
             "tx_count_1h", "is_night_hour", "is_high_amount",
             "is_international", "hour_of_day", "merchant_category"]
            + V_COLS + ["is_fraud_label"]
        )
        df = df.select(feature_cols)

        row_count = df.count()
        fraud_count = df.filter(F.col("is_fraud_label") == 1).count()
        logger.info(f"Features: {row_count} rows | fraud: {fraud_count}")

        df.coalesce(1).write.mode("overwrite").parquet(output_path)
        logger.info(f"Written to: {output_path}")

        return row_count, fraud_count

    finally:
        if should_stop:
            spark.stop()
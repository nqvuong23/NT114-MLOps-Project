"""
spark_feature_engineering.py
=============================
Spark job: Feature Engineering từ processed data.

Input:  S3 s3://{bucket}/processed-data/{batch_id}/processed.parquet
Output: S3 s3://{bucket}/feature-store/{batch_id}/features.parquet
        (dùng cho cả inference và training)

Features được tính thêm:
  - amount_zscore   : z-score của amount so với lịch sử card
  - tx_count_1h     : số giao dịch của card trong 1h tính từ tx này
  - is_night_hour   : 1 nếu hour_of_day trong [22,23,0,1,2,3,4]
  - amount_log1p    : log1p(amount) — giảm skewness

NOTE:
  - is_fraud_label ĐƯỢC GIỮ LẠI trong feature-store để dùng cho training
  - Khi gọi model inference, Airflow sẽ DROP cột này trước khi gửi đến API
"""

import os
import sys
import json
import argparse
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import pyspark.sql.functions as sf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("SparkFeatureEngineering")

V_COLS = [f"V{i}" for i in range(1, 29)]
NIGHT_HOURS = [22, 23, 0, 1, 2, 3, 4]


def create_spark_session(app_name: str = "FraudDetection-FeatureEng") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def feature_engineering(spark: SparkSession, input_path: str, output_path: str):
    logger.info(f"Reading processed data from: {input_path}")
    df = spark.read.parquet(input_path)
    df = df.withColumn("timestamp_ts", F.to_timestamp(F.col("timestamp")))

    logger.info(f"Input row count: {df.count()}")

    # ── Feature 1: is_night_hour ──────────────────────────────────────────
    night_hours_list = NIGHT_HOURS
    df = df.withColumn(
        "is_night_hour",
        F.when(F.col("hour_of_day").isin(night_hours_list), 1).otherwise(0)
    )

    # ── Feature 2: amount_log1p ───────────────────────────────────────────
    df = df.withColumn(
        "amount_log1p",
        F.log1p(F.col("amount"))
    )

    # ── Feature 3: amount_zscore per card ────────────────────────────────
    # Z-score = (amount - mean_amount_per_card) / std_amount_per_card
    # Dùng window function trên toàn bộ lịch sử của card trong batch hiện tại
    card_window = Window.partitionBy("card_id")
    df = df.withColumn(
        "card_mean_amount",
        F.avg("amount").over(card_window)
    ).withColumn(
        "card_std_amount",
        F.stddev("amount").over(card_window)
    )
    df = df.withColumn(
        "amount_zscore",
        F.when(
            F.col("card_std_amount") > 0,
            (F.col("amount") - F.col("card_mean_amount")) / F.col("card_std_amount")
        ).otherwise(F.lit(0.0))
    ).drop("card_mean_amount", "card_std_amount")

    # ── Feature 4: tx_count_1h per card ──────────────────────────────────
    # Đếm số giao dịch của cùng card trong vòng 1 giờ trước tx hiện tại
    # Dùng window với rangeBetween theo epoch seconds
    df = df.withColumn(
        "epoch_ts",
        F.col("timestamp_ts").cast("long")
    )
    one_hour_window = (
        Window.partitionBy("card_id")
        .orderBy("epoch_ts")
        .rangeBetween(-3600, 0)  # 3600 giây = 1 giờ
    )
    df = df.withColumn(
        "tx_count_1h",
        F.count("transaction_id").over(one_hour_window).cast(IntegerType())
    )

    # ── Feature 5: is_high_amount ─────────────────────────────────────────
    # 1 nếu amount > 500 (threshold tham khảo từ fraud pattern)
    df = df.withColumn(
        "is_high_amount",
        F.when(F.col("amount") > 500, 1).otherwise(0)
    )

    # ── Feature 6: is_international (đã có, đảm bảo type đúng) ──────────
    df = df.withColumn(
        "is_international",
        F.col("is_international").cast(IntegerType())
    )

    # ── Cleanup: drop temp columns ────────────────────────────────────────
    df = df.drop("timestamp_ts", "epoch_ts")

    # ── Final column order ────────────────────────────────────────────────
    # Training cols = tất cả features + is_fraud_label
    # Inference cols = tất cả features (không có is_fraud_label)
    #   → Airflow sẽ xử lý việc drop khi gọi API
    feature_cols = (
        ["transaction_id", "user_id", "card_id", "timestamp",
         "amount", "amount_normalized", "amount_log1p", "amount_zscore",
         "tx_count_1h", "is_night_hour", "is_high_amount",
         "is_international", "hour_of_day", "merchant_category"]
        + V_COLS
        + ["is_fraud_label"]  # GIỮ LẠI cho training, sẽ drop khi inference
    )

    df = df.select(feature_cols)

    # Thống kê
    row_count = df.count()
    fraud_count = df.filter(F.col("is_fraud_label") == 1).count()
    logger.info(
        f"Feature output: {row_count} rows | "
        f"fraud: {fraud_count} ({fraud_count/max(row_count,1)*100:.3f}%) | "
        f"features: {len(feature_cols)} columns"
    )

    # ── Ghi ra S3 ─────────────────────────────────────────────────────────
    logger.info(f"Writing features to: {output_path}")
    (df
     .coalesce(1)
     .write
     .mode("overwrite")
     .parquet(output_path))

    logger.info("Feature Engineering complete ✅")
    return row_count, fraud_count, len(feature_cols)


def main():
    parser = argparse.ArgumentParser(description="Spark Feature Engineering Job")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--processed-prefix", default="processed-data")
    parser.add_argument("--feature-prefix", default="feature-store")
    args = parser.parse_args()

    input_path = f"s3a://{args.bucket}/{args.processed_prefix}/{args.batch_id}/processed.parquet"
    output_path = f"s3a://{args.bucket}/{args.feature_prefix}/{args.batch_id}/features.parquet"

    spark = create_spark_session()
    try:
        row_count, fraud_count, n_features = feature_engineering(spark, input_path, output_path)
        print(json.dumps({
            "batch_id": args.batch_id,
            "output_path": output_path,
            "row_count": row_count,
            "fraud_count": fraud_count,
            "feature_count": n_features,
            "status": "success"
        }))
    except Exception as e:
        logger.error(f"Spark feature engineering failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
"""
spark_cleaning.py
=================
Spark job: Cleaning & Transformation cho raw transaction data.

Input:  S3 s3://{bucket}/raw-data/{batch_id}/raw.parquet
Output: S3 s3://{bucket}/processed-data/{batch_id}/processed.parquet

Các bước:
  1. Đọc raw parquet từ S3
  2. Drop duplicates theo transaction_id
  3. Xử lý missing values
  4. Chuẩn hóa timestamp → UTC ISO string
  5. Encode is_international (bool → int)
  6. Normalize amount bằng StandardScaler
  7. Chuẩn hóa merchant_category
  8. Ghi output ra S3

Chạy:
  spark-submit \
    --master local[*] \
    spark_cleaning.py \
    --batch-id <batch_id> \
    --bucket <s3_bucket>
"""

import os
import sys
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("SparkCleaning")

V_COLS = [f"V{i}" for i in range(1, 29)]

VALID_CATEGORIES = {
    "grocery", "online", "travel", "restaurant", "entertainment",
    "gas_station", "pharmacy", "electronics", "clothing", "atm"
}


def create_spark_session(app_name: str = "FraudDetection-Cleaning") -> SparkSession:
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


def clean_and_transform(spark: SparkSession, input_path: str, output_path: str):
    logger.info(f"Reading raw data from: {input_path}")
    df = spark.read.parquet(input_path)

    original_count = df.count()
    logger.info(f"Raw row count: {original_count}")

    # ── Step 1: Drop duplicates ────────────────────────────────────────────
    df = df.dropDuplicates(["transaction_id"])
    after_dedup = df.count()
    logger.info(f"After dedup: {after_dedup} (removed {original_count - after_dedup})")

    # ── Step 2: Drop rows with null in critical columns ────────────────────
    critical_cols = ["transaction_id", "user_id", "card_id", "timestamp",
                     "amount", "is_fraud_label"] + V_COLS
    df = df.dropna(subset=critical_cols)

    # ── Step 3: Fill remaining nulls ───────────────────────────────────────
    df = df.fillna({
        "merchant_id": "UNKNOWN",
        "merchant_category": "other",
        "is_international": False,
        "hour_of_day": -1,
    })

    # ── Step 4: Chuẩn hóa timestamp → UTC ISO string ──────────────────────
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp")).cast("timestamp")
    )
    df = df.withColumn(
        "timestamp_utc",
        F.date_format(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # ── Step 5: Encode boolean → integer ──────────────────────────────────
    df = df.withColumn(
        "is_international",
        F.col("is_international").cast(IntegerType())
    )

    # ── Step 6: Validate và clamp amount ──────────────────────────────────
    # Loại bỏ amount âm (không hợp lệ)
    df = df.filter(F.col("amount") >= 0)
    # Clamp amount cực lớn (outlier thực sự bất thường > $50000)
    df = df.withColumn(
        "amount",
        F.when(F.col("amount") > 50000, 50000.0).otherwise(F.col("amount"))
    )

    # ── Step 7: Chuẩn hóa merchant_category ──────────────────────────────
    valid_cats_expr = F.array([F.lit(c) for c in sorted(VALID_CATEGORIES)])
    df = df.withColumn(
        "merchant_category",
        F.when(
            F.col("merchant_category").isin(list(VALID_CATEGORIES)),
            F.col("merchant_category")
        ).otherwise(F.lit("other"))
    )

    # ── Step 8: Normalize amount bằng StandardScaler ──────────────────────
    assembler = VectorAssembler(inputCols=["amount"], outputCol="amount_vec")
    scaler = StandardScaler(
        inputCol="amount_vec",
        outputCol="amount_scaled_vec",
        withMean=True,
        withStd=True
    )
    pipeline = Pipeline(stages=[assembler, scaler])
    model = pipeline.fit(df)
    df = model.transform(df)

    # Extract scalar từ vector
    @F.udf(DoubleType())
    def extract_first(vec):
        return float(vec[0]) if vec is not None else 0.0

    df = df.withColumn("amount_normalized", extract_first(F.col("amount_scaled_vec")))
    df = df.drop("amount_vec", "amount_scaled_vec")

    # ── Step 9: Cast is_fraud_label ───────────────────────────────────────
    df = df.withColumn("is_fraud_label", F.col("is_fraud_label").cast(IntegerType()))

    # ── Step 10: Chọn và sắp xếp columns output ───────────────────────────
    output_cols = (
        ["transaction_id", "user_id", "card_id", "timestamp_utc",
         "amount", "amount_normalized", "merchant_id", "merchant_category",
         "is_international", "hour_of_day"]
        + V_COLS
        + ["is_fraud_label"]
    )
    # Đổi tên timestamp_utc → timestamp
    df = df.withColumnRenamed("timestamp_utc", "timestamp")
    output_cols[3] = "timestamp"
    df = df.select(output_cols)

    final_count = df.count()
    fraud_count = df.filter(F.col("is_fraud_label") == 1).count()
    logger.info(
        f"Clean output: {final_count} rows | "
        f"fraud: {fraud_count} ({fraud_count/max(final_count,1)*100:.3f}%)"
    )

    # ── Step 11: Ghi ra S3 ────────────────────────────────────────────────
    logger.info(f"Writing processed data to: {output_path}")
    (df
     .coalesce(1)
     .write
     .mode("overwrite")
     .parquet(output_path))

    logger.info("Cleaning & Transformation complete ✅")
    return final_count, fraud_count


def main():
    parser = argparse.ArgumentParser(description="Spark Cleaning & Transformation Job")
    parser.add_argument("--batch-id", required=True, help="Batch ID (e.g. 20240115_120000)")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--raw-prefix", default="raw-data", help="S3 prefix for raw data")
    parser.add_argument("--processed-prefix", default="processed-data", help="S3 prefix for output")
    args = parser.parse_args()

    input_path = f"s3a://{args.bucket}/{args.raw_prefix}/{args.batch_id}/raw.parquet"
    output_path = f"s3a://{args.bucket}/{args.processed_prefix}/{args.batch_id}/processed.parquet"

    spark = create_spark_session()
    try:
        row_count, fraud_count = clean_and_transform(spark, input_path, output_path)
        # Ghi metadata ra stdout để Airflow XCom có thể capture
        import json
        print(json.dumps({
            "batch_id": args.batch_id,
            "output_path": output_path,
            "row_count": row_count,
            "fraud_count": fraud_count,
            "status": "success"
        }))
    except Exception as e:
        logger.error(f"Spark cleaning failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
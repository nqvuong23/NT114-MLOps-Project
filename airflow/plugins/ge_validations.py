"""
ge_validations.py
=================
Great Expectations validation suites cho 3 giai đoạn:
  1. raw_data_suite       — kiểm tra data thô từ RDS
  2. processed_data_suite — kiểm tra sau cleaning & transformation
  3. feature_suite        — kiểm tra sau feature engineering

Mỗi suite trả về (passed: bool, results: dict)
Tương thích với Great Expectations v1.x+ và Python 3.13
"""

import os
import logging
import json
from datetime import datetime, timezone
from typing import Tuple

import pandas as pd
import great_expectations as gx
import great_expectations.expectations as gxe

logger = logging.getLogger(__name__)

# Tên 28 PCA columns
V_COLS = [f"V{i}" for i in range(1, 29)]


# ── Lớp Proxy: Dịch cú pháp Imperative cũ sang Object-Oriented mới của GX 1.x ──
class GxValidatorProxy:
    """Giúp giữ nguyên logic viết code cũ của người dùng mà không làm crash hệ thống v1.x"""
    def __init__(self, suite):
        self.suite = suite

    def expect_column_to_exist(self, column):
        self.suite.add_expectation(gxe.ExpectColumnToExist(column=column))

    def expect_column_values_to_not_be_null(self, column):
        self.suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=column))

    def expect_column_values_to_be_unique(self, column):
        self.suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column=column))

    def expect_column_values_to_be_between(self, column, min_value=None, max_value=None):
        self.suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column=column, min_value=min_value, max_value=max_value))

    def expect_column_values_to_be_in_set(self, column, value_set):
        self.suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column=column, value_set=value_set))

    def expect_table_row_count_to_be_between(self, min_value=None, max_value=None):
        self.suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=min_value, max_value=max_value))

    def expect_column_stdev_to_be_between(self, column, min_value=None, max_value=None):
        self.suite.add_expectation(gxe.ExpectColumnStdevToBeBetween(column=column, min_value=min_value, max_value=max_value))


# ── Helper: tạo ephemeral GE context ──────────────────────────────────────────
def _get_ge_context():
    return gx.get_context(mode="ephemeral")


def _run_checkpoint(df: pd.DataFrame, suite_name: str, expectations_fn) -> Tuple[bool, dict]:
    """
    Chạy GX validation sử dụng cấu trúc ValidationDefinition của bản 1.x
    Returns: (success, result_dict)
    """
    context = _get_ge_context()

    # 1. Khởi tạo Datasource và Asset theo chuẩn GX 1.x (.data_sources thay vì .sources)
    datasource = context.data_sources.add_pandas(name=f"ds_{suite_name}")
    asset = datasource.add_dataframe_asset(name=f"asset_{suite_name}")
    
    # Định nghĩa cấu trúc nhận DataFrame động lúc runtime
    batch_definition = asset.add_batch_definition_whole_dataframe(name=f"batch_def_{suite_name}")

    # 2. Tạo Expectation Suite 
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # Chạy hàm nạp expectations qua lớp Proxy bảo vệ
    proxy_validator = GxValidatorProxy(suite)
    expectations_fn(proxy_validator)

    # 3. Tạo Validation Definition để gắn Batch dữ liệu với bộ luật Suite
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"val_def_{suite_name}",
            batch_definition=batch_definition,
            expectation_suite=suite,
        )
    )

    # 4. Thực thi kiểm tra trực tiếp bằng cách truyền DataFrame vào parameters
    validation_result = validation_definition.run(batch_parameters={"dataframe": df})

    success = validation_result.success

    # Trích xuất danh sách các luật bị fail
    failed_expectations = [
        {
            "expectation_type": getattr(r.expectation_config, "type", "Unknown"),
            "column": r.expectation_config.kwargs.get("column", "N/A"),
            "result": str(r.result),
            "success": r.success,
        }
        for r in validation_result.results
        if not r.success
    ]

    return success, {
        "suite": suite_name,
        "passed": success,
        "total_expectations": len(validation_result.results),
        "failed_count": len(failed_expectations),
        "failed_expectations": failed_expectations,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Suite 1: Raw Data Validation ─────────────────────────────────────────────
def validate_raw_data(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Validate data thô vừa lấy từ RDS.
    Kiểm tra: schema, null, kiểu dữ liệu, range cơ bản.
    """
    required_cols = (
        ["transaction_id", "user_id", "card_id", "timestamp",
         "amount", "merchant_id", "merchant_category",
         "is_international", "hour_of_day"]
        + V_COLS
        + ["is_fraud_label"]
    )

    def add_expectations(v):
        for col in required_cols:
            v.expect_column_to_exist(col)

        for col in ["transaction_id", "user_id", "card_id", "timestamp", "amount", "is_fraud_label"]:
            v.expect_column_values_to_not_be_null(col)

        v.expect_column_values_to_be_unique("transaction_id")
        v.expect_column_values_to_be_between("amount", min_value=0)
        v.expect_column_values_to_be_in_set("is_fraud_label", [0, 1])
        v.expect_column_values_to_be_between("hour_of_day", min_value=0, max_value=23)

        for col in V_COLS:
            v.expect_column_values_to_not_be_null(col)

        v.expect_table_row_count_to_be_between(min_value=1)

    logger.info(f"Running raw_data validation on {len(df)} rows...")
    return _run_checkpoint(df, "raw_data_suite", add_expectations)


# ── Suite 2: Processed Data Validation ───────────────────────────────────────
def validate_processed_data(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Validate data sau cleaning & transformation.
    Kiểm tra: không còn null, types đúng, giá trị hợp lệ.
    """
    def add_expectations(v):
        for col in df.columns:
            v.expect_column_values_to_not_be_null(col)

        v.expect_column_values_to_be_between("amount", min_value=0, max_value=50000)

        if "amount_normalized" in df.columns:
            v.expect_column_values_to_be_between("amount_normalized", min_value=-10, max_value=10)

        v.expect_column_values_to_be_in_set("is_fraud_label", [0, 1])
        v.expect_column_values_to_be_unique("transaction_id")

        for col in V_COLS:
            v.expect_column_values_to_be_between(col, min_value=-50, max_value=50)

        valid_categories = [
            "grocery", "online", "travel", "restaurant", "entertainment",
            "gas_station", "pharmacy", "electronics", "clothing", "atm", "other"
        ]
        v.expect_column_values_to_be_in_set("merchant_category", valid_categories)

    logger.info(f"Running processed_data validation on {len(df)} rows...")
    return _run_checkpoint(df, "processed_data_suite", add_expectations)


# ── Suite 3: Feature Dataset Validation ──────────────────────────────────────
def validate_features(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Validate feature dataset sau feature engineering.
    Kiểm tra: không NaN, không constant feature, distribution hợp lý.
    """
    def add_expectations(v):
        for col in df.columns:
            v.expect_column_values_to_not_be_null(col)

        if "amount_zscore" in df.columns:
            v.expect_column_values_to_be_between("amount_zscore", min_value=-20, max_value=20)

        if "tx_count_1h" in df.columns:
            v.expect_column_values_to_be_between("tx_count_1h", min_value=0, max_value=1000)

        if "is_night_hour" in df.columns:
            v.expect_column_values_to_be_in_set("is_night_hour", [0, 1])

        v.expect_column_values_to_be_in_set("is_international", [0, 1])

        numeric_cols = ["amount", "amount_zscore"] + V_COLS
        for col in numeric_cols:
            if col in df.columns:
                v.expect_column_stdev_to_be_between(col, min_value=0.0001)

        v.expect_column_values_to_be_in_set("is_fraud_label", [0, 1])
        v.expect_table_row_count_to_be_between(min_value=1)

    logger.info(f"Running feature validation on {len(df)} rows...")
    return _run_checkpoint(df, "feature_suite", add_expectations)


def log_validation_result(result: dict, step: str):
    """Log kết quả validation ra console/CloudWatch."""
    if result["passed"]:
        logger.info(
            f"✅ [{step}] Validation PASSED | Total: {result['total_expectations']} expectations"
        )
    else:
        logger.error(
            f"❌ [{step}] Validation FAILED | Failed: {result['failed_count']}/{result['total_expectations']}"
        )
        for fail in result["failed_expectations"]:
            logger.error(
                f"   - {fail['expectation_type']} | column: {fail['column']}"
            )
    return result
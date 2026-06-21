"""
ge_validations.py
=================
Great Expectations validation suites cho 3 giai đoạn:
  1. raw_data_suite       — kiểm tra data thô từ RDS
  2. processed_data_suite — kiểm tra sau cleaning & transformation
  3. feature_suite        — kiểm tra sau feature engineering

Mỗi suite trả về (passed: bool, results: dict)
"""
import os
import logging
from datetime import datetime, timezone
from typing import Tuple

import pandas as pd
import great_expectations as gx

logger = logging.getLogger(__name__)

# Tên 28 PCA columns
V_COLS = [f"V{i}" for i in range(1, 29)]

def _run_checkpoint(df: pd.DataFrame, suite_name: str, expectations_fn) -> Tuple[bool, dict]:
    """
    Sử dụng Batch Definition & Checkpoint chuẩn GX 1.x kết hợp Validator 
    để chạy mượt mà với các hàm định nghĩa suite cũ.
    """
    # 1. Khởi tạo Ephemeral context (Chạy hoàn toàn trên RAM)
    context = gx.get_context(mode="ephemeral")
    
    # 2. Tạo Data Source và Data Asset bằng Fluent API của 1.x
    data_source = context.data_sources.add_pandas(name=f"{suite_name}_source")
    data_asset = data_source.add_dataframe_asset(name=f"{suite_name}_asset")

    # 3. Tạo Batch Definition để định nghĩa tập dữ liệu kiểm tra
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name=f"{suite_name}_batch_def" 
    )

    # 4. Khởi tạo Object Suite trống
    suite = gx.ExpectationSuite(name=f"{suite_name}_suite")

    # 5. Dùng Validator để nạp toàn bộ hàm `expectations_fn` vào Suite
    batch_request = batch_definition.build_batch_request(batch_parameters={"dataframe": df})
    validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
    
    # Chạy hàm callback chứa các luật v.expect_... của bạn
    expectations_fn(validator)
    
    # Lưu lại vào context
    validator.save_expectation_suite()

    # Lấy suite đã được lưu chính thức trong context ra
    saved_suite = context.suites.get(name=f"{suite_name}_suite")

    # 6. Tạo Validation Definition kết hợp Batch Definition và Suite đã lưu
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"{suite_name}_val_def",
            data=batch_definition,
            suite=saved_suite
        )
    )

    # 7. Khởi tạo Checkpoint và thực thi việc validate dữ liệu
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name=f"{suite_name}_checkpoint",
            validation_definitions=[validation_definition]
        )
    )
    
    # Truyền dữ liệu DataFrame động vào tham số chạy checkpoint
    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})

    # =====================================================================
    # 8. TRÍCH XUẤT KẾT QUẢ ĐÃ ĐƯỢC TINH GỌN & TỐI ƯU
    # =====================================================================
    success = checkpoint_result.success
    failed_expectations = []
    total_expectations = 0

    # Bộ trích xuất động: Tự nhận diện cấu trúc để lấy data từ cả Dict hoặc Object thuộc tính
    get_val = lambda obj, key, default=None: obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)

    for val_res in checkpoint_result.run_results.values():
        for r in get_val(val_res, "results", []):
            total_expectations += 1
            
            # Nếu quy tắc kiểm tra bị thất bại (success == False)
            if not get_val(r, "success", False):
                config = get_val(r, "expectation_config", {})
                kwargs = get_val(config, "kwargs", {})
                
                failed_expectations.append({
                    # Đọc key "type" chuẩn theo log thực tế bạn cung cấp
                    "expectation_type": get_val(config, "type", get_val(config, "expectation_type", "N/A")),
                    "column": kwargs.get("column", "N/A"),
                    "result": str(get_val(r, "result", {})),
                    "success": False,
                })

    return success, {
        "suite": suite_name,
        "passed": success,
        "total_expectations": total_expectations,
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
        # 1. Tất cả cột bắt buộc phải tồn tại
        for col in required_cols:
            v.expect_column_to_exist(col)

        # 2. Không null ở các cột quan trọng
        for col in ["transaction_id", "user_id", "card_id", "timestamp", "amount", "is_fraud_label"]:
            v.expect_column_values_to_not_be_null(col)

        # 3. transaction_id phải unique
        v.expect_column_values_to_be_unique("transaction_id")

        # 4. amount không âm
        v.expect_column_values_to_be_between("amount", min_value=0)

        # 5. is_fraud_label chỉ nhận 0 hoặc 1
        v.expect_column_values_to_be_in_set("is_fraud_label", [0, 1])

        # 6. hour_of_day trong [0, 23]
        v.expect_column_values_to_be_between("hour_of_day", min_value=0, max_value=23)

        # 7. V1..V28 phải là số (không null — có thể miss vài giá trị)
        for col in V_COLS:
            v.expect_column_values_to_not_be_null(col)

        # 8. Row count > 0
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
        # 1. Không còn null trong bất kỳ cột nào
        for col in df.columns:
            v.expect_column_values_to_not_be_null(col)

        # 2. amount sau chuẩn hóa trong range hợp lý
        v.expect_column_values_to_be_between("amount", min_value=0, max_value=50000)

        # 3. amount_normalized (StandardScaler) phải trong [-10, 10]
        if "amount_normalized" in df.columns:
            v.expect_column_values_to_be_between("amount_normalized", min_value=-10, max_value=10)

        # 4. is_fraud_label vẫn chỉ 0 hoặc 1
        v.expect_column_values_to_be_in_set("is_fraud_label", [0, 1])

        # 5. Không duplicate transaction_id
        v.expect_column_values_to_be_unique("transaction_id")

        # 6. V1..V28 vẫn trong range thực tế [-30, 30] (từ Kaggle stats)
        for col in V_COLS:
            v.expect_column_values_to_be_between(col, min_value=-50, max_value=50)

        # 7. merchant_category không có giá trị lạ
        valid_categories = [
            "grocery", "online", "travel", "restaurant", "entertainment",
            "gas_station", "pharmacy", "electronics", "clothing", "atm", "other"
        ]
        v.expect_column_values_to_be_in_set("merchant_category", valid_categories)

    logger.info(f"Running processed_data validation on {len(df)} rows...")
    return _run_checkpoint(df, "processed_data_suite", add_expectations)


# ── Suite 3: Feature Dataset Validation ──────────────────────────────────────
def validate_feature_store(df: pd.DataFrame) -> Tuple[bool, dict]:
    """
    Validate feature dataset sau feature engineering.
    Kiểm tra: không NaN, không constant feature, distribution hợp lý.
    """
    def add_expectations(v):
        # 1. Không null trong toàn bộ
        for col in df.columns:
            v.expect_column_values_to_not_be_null(col)

        # 2. amount_zscore: z-score không quá extreme (fraud detection: [-20, 20])
        if "amount_zscore" in df.columns:
            v.expect_column_values_to_be_between("amount_zscore", min_value=-20, max_value=20)

        # 3. tx_count_1h: số giao dịch trong 1h phải >= 0 và thực tế < 1000
        if "tx_count_1h" in df.columns:
            v.expect_column_values_to_be_between("tx_count_1h", min_value=0, max_value=1000)

        # 4. is_night_hour chỉ 0 hoặc 1
        if "is_night_hour" in df.columns:
            v.expect_column_values_to_be_in_set("is_night_hour", [0, 1])

        # 5. is_international chỉ 0 hoặc 1 (đã convert từ bool)
        v.expect_column_values_to_be_in_set("is_international", [0, 1])

        # 6. Kiểm tra không có constant column (std > 0 cho numeric cols)
        numeric_cols = ["amount", "amount_zscore"] + V_COLS
        for col in numeric_cols:
            if col in df.columns:
                v.expect_column_stdev_to_be_between(col, min_value=0.0001)

        # 7. is_fraud_label vẫn nhị phân
        v.expect_column_values_to_be_in_set("is_fraud_label", [0, 1])

        # 8. Row count > 0
        v.expect_table_row_count_to_be_between(min_value=1)

    logger.info(f"Running feature validation on {len(df)} rows...")
    return _run_checkpoint(df, "feature_suite", add_expectations)


def log_validation_result(result: dict, step: str):
    """Log kết quả validation ra console/CloudWatch."""
    if result["passed"]:
        logger.info(
            f"✅ [{step}] Validation PASSED | "
            f"Total: {result['total_expectations']} expectations"
        )
    else:
        logger.error(
            f"❌ [{step}] Validation FAILED | "
            f"Failed: {result['failed_count']}/{result['total_expectations']}"
        )
        for fail in result["failed_expectations"]:
            logger.error(
                f"   - {fail['expectation_type']} | column: {fail['column']}"
            )
    return result
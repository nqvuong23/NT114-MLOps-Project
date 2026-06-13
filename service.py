import os
import json
import numpy as np
import pandas as pd
import xgboost as xgb
import bentoml
import boto3
import io
from datetime import datetime

# Hàm đọc tệp .env thủ công để nạp các cấu hình AWS/RDS
def load_env_file():
    if os.path.exists(".env"):
        print("Loading environment variables from .env...")
        with open(".env", "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    os.environ[key] = val

load_env_file()

# Định nghĩa các đặc trưng cần thiết cho training để đảm bảo đúng thứ tự cột
FEATURE_COLS = (
    ["amount", "amount_normalized", "amount_log1p", "amount_zscore",
     "tx_count_1h", "is_night_hour", "is_high_amount",
     "is_international", "hour_of_day"]
    + [f"V{i}" for i in range(1, 29)]
)

@bentoml.service(
    name="credit_card_fraud_service"
)
class CreditCardFraudService:
    def __init__(self):
        # 1. Tải thông số chuẩn hóa và khởi tạo mô hình
        with open("model/scaler_stats.json", "r") as f:
            stats = json.load(f)
        self.amount_mean = stats["amount_mean"]
        self.amount_std = stats["amount_std"]

        # Khởi tạo mô hình XGBoost từ artifact đã tải về
        self.model = xgb.XGBClassifier()
        model_path = "model/model.ubj"
        if not os.path.exists(model_path):
            model_path = "model/model.xgb"
        if not os.path.exists(model_path):
            model_path = "model/model.json"

        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Không tìm thấy file mô hình XGBoost trong thư mục model/! Các file có sẵn: {os.listdir('model')}")

        self.model.load_model(model_path)
        print("Mô hình XGBoost và scaler_stats đã được load thành công!")

    def apply_feature_engineering(self, df_input):
        df = df_input.copy()
        
        # 1. Tính toán giờ và giờ đêm
        df["hour_of_day"] = (df["Time"] // 3600) % 24
        df["is_night_hour"] = df["hour_of_day"].isin([22, 23, 0, 1, 2, 3, 4]).astype(int)
        
        # 2. Log1p của Amount
        df["amount_log1p"] = np.log1p(df["Amount"])
        
        # 3. Chuẩn hóa Amount dùng scaler_stats.json
        df["amount_normalized"] = (df["Amount"] - self.amount_mean) / self.amount_std
        df["amount_zscore"] = df["amount_normalized"]
        
        # 4. Tính toán số giao dịch trong 1h qua (rolling window trên Batch)
        times = df["Time"].values
        start_indices = np.searchsorted(times, times - 3600, side="left")
        df["tx_count_1h"] = (np.arange(len(times)) - start_indices + 1).astype(int)
        
        # 5. Các đặc trưng khác
        df["is_high_amount"] = (df["Amount"] > 500).astype(int)
        df["is_international"] = 0
        df["amount"] = df["Amount"]
        
        return df[FEATURE_COLS]

    def upload_excel_to_s3(self, df_data, predictions):
        # Gắn cột dự đoán vào tập dữ liệu
        df_output = df_data.copy()
        df_output["Predicted_Class"] = predictions
        
        # Ghi file excel ra buffer RAM
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            df_output.to_excel(writer, index=False, sheet_name="Predictions")
        excel_buffer.seek(0)
        
        # Upload lên S3
        bucket_name = os.environ.get("S3_BUCKET", "nt114-mlops-bucket")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_key = f"prediction/predictions_{timestamp}.xlsx"
        
        s3_client = boto3.client("s3")
        s3_client.upload_fileobj(
            Fileobj=excel_buffer,
            Bucket=bucket_name,
            Key=file_key
        )
        
        s3_url = f"s3://{bucket_name}/{file_key}"
        print(f"Đã upload thành công file kết quả dự đoán lên: {s3_url}")
        return s3_url

    @bentoml.api
    def predict(self, request_data: list) -> dict:
        """
        Nhận danh sách JSON các giao dịch để dự đoán
        """
        try:
            df_raw = pd.DataFrame(request_data)
            
            # Xử lý đặc trưng
            X = self.apply_feature_engineering(df_raw)
            
            # Dự đoán
            preds = self.model.predict(X)
            preds_list = [int(p) for p in preds]
            
            # Ghi Excel lên S3
            s3_url = self.upload_excel_to_s3(df_raw, preds_list)
            
            return {
                "status": "success",
                "predictions": preds_list,
                "total_records": len(preds_list),
                "fraud_detected": sum(preds_list),
                "output_excel_s3": s3_url
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }

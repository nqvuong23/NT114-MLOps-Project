import pandas as pd
import numpy as np
import xgboost as xgb
import optuna
import mlflow
import mlflow.xgboost
from sklearn.metrics import f1_score, classification_report
import os
import io
import boto3

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

# 1. Cấu hình kết nối tới MLflow và S3
tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(tracking_uri)
print(f"MLflow Tracking URI: {tracking_uri}")

# Thiết lập AWS credentials từ .env hoặc môi trường
if "AWS_ACCESS_KEY_ID" not in os.environ:
    # Fallback về MinIO local nếu không có trong env
    os.environ["AWS_ACCESS_KEY_ID"] = "minio_admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minio_password"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

mlflow.set_experiment("Real_Data_Fraud_Detection")

# Khởi tạo MLflow client
client = mlflow.tracking.MlflowClient()
model_name = "CreditCardFraudModel"

# Lấy điểm F1-score của mô hình đang ở stage Production (nếu có)
production_f1 = 0.0
try:
    latest_versions = client.get_latest_versions(model_name, stages=["Production"])
    if latest_versions:
        prod_version = latest_versions[0]
        run_id = prod_version.run_id
        run = client.get_run(run_id)
        # Kiểm tra metric f1_score hoặc best_f1_score
        production_f1 = run.data.metrics.get("f1_score", 0.0)
        if production_f1 == 0.0:
            production_f1 = run.data.metrics.get("best_f1_score", 0.0)
        print(f"Current Production Model Version: {prod_version.version}, F1-score: {production_f1:.4f}")
    else:
        print("No active Production model version found.")
except Exception as e:
    print(f"Notice: No active Production model found or error querying registry: {e}")

# 2. Đọc dữ liệu từ S3 Feature Store (hoặc local file fallback)
def load_data():
    bucket = os.environ.get("S3_BUCKET", "nt114-mlops-bucket")
    endpoint_url = os.environ.get("MLFLOW_S3_ENDPOINT_URL")
    
    # Khởi tạo boto3 client
    s3_client = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1"),
        endpoint_url=endpoint_url if endpoint_url else None
    )
    
    possible_keys = ["feature-store/creditcard.csv", "feature-store/credit.csv"]
    df = None
    
    for key in possible_keys:
        try:
            print(f"Attempting to download s3://{bucket}/{key}...")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            print(f"Successfully connected to S3. Loading CSV data...")
            df = pd.read_csv(io.BytesIO(response['Body'].read()))
            print(f"Data loaded from S3. Shape: {df.shape}")
            break
        except Exception as e:
            print(f"Could not load from s3://{bucket}/{key}: {e}")
            
    if df is None:
        local_file = "creditcard.csv"
        if os.path.exists(local_file):
            print(f"Falling back to local file: {local_file}")
            df = pd.read_csv(local_file)
            print(f"Data loaded from local file. Shape: {df.shape}")
        else:
            raise FileNotFoundError("Could not find data on S3 or locally.")
            
    return df

df = load_data()

# 3. Tiền xử lý đặc trưng (Feature Engineering)
def prepare_features(df_input):
    df_processed = df_input.copy()
    
    # Kiểm tra xem đây là dữ liệu thô (raw) hay đã qua feature store xử lý sẵn
    if "Class" in df_processed.columns and "is_fraud_label" not in df_processed.columns:
        print("Raw dataset detected. Applying feature engineering matching Spark jobs...")
        
        # 1. is_fraud_label
        df_processed["is_fraud_label"] = df_processed["Class"]
        
        # 2. hour_of_day
        df_processed["hour_of_day"] = (df_processed["Time"] // 3600) % 24
        
        # 3. is_night_hour: [22, 23, 0, 1, 2, 3, 4]
        df_processed["is_night_hour"] = df_processed["hour_of_day"].isin([22, 23, 0, 1, 2, 3, 4]).astype(int)
        
        # 4. amount_log1p
        df_processed["amount_log1p"] = np.log1p(df_processed["Amount"])
        
        # 5. amount_normalized
        amount_mean = df_processed["Amount"].mean()
        amount_std = df_processed["Amount"].std()
        df_processed["amount_normalized"] = (df_processed["Amount"] - amount_mean) / (amount_std if amount_std > 0 else 1.0)
        
        # 6. amount_zscore
        df_processed["amount_zscore"] = df_processed["amount_normalized"]
        
        # 7. tx_count_1h
        times = df_processed["Time"].values
        start_indices = np.searchsorted(times, times - 3600, side="left")
        df_processed["tx_count_1h"] = (np.arange(len(times)) - start_indices + 1).astype(int)
        
        # 8. is_high_amount
        df_processed["is_high_amount"] = (df_processed["Amount"] > 500).astype(int)
        
        # 9. is_international
        df_processed["is_international"] = 0
        
        # 10. amount
        df_processed["amount"] = df_processed["Amount"]
        
    # Đảm bảo sắp xếp cột đúng thứ tự các đặc trưng dùng để huấn luyện
    feature_cols = (
        ["amount", "amount_normalized", "amount_log1p", "amount_zscore",
         "tx_count_1h", "is_night_hour", "is_high_amount",
         "is_international", "hour_of_day"]
        + [f"V{i}" for i in range(1, 29)]
    )
    
    # Sắp xếp lại theo Time để chia Train/Test theo thời gian một cách tuần tự
    if "Time" in df_processed.columns:
        df_processed = df_processed.sort_values(by="Time").reset_index(drop=True)
    elif "timestamp" in df_processed.columns:
        df_processed = df_processed.sort_values(by="timestamp").reset_index(drop=True)
        
    X_processed = df_processed[feature_cols]
    y_processed = df_processed["is_fraud_label"]
    return X_processed, y_processed

X, y = prepare_features(df)

# Phân tách Train / Test theo tỷ lệ 80/20
split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Train set: {X_train.shape[0]} samples (Fraud: {y_train.sum()})")
print(f"Test set: {X_test.shape[0]} samples (Fraud: {y_test.sum()})")

# Tính toán tỷ lệ mất cân bằng để làm baseline cho scale_pos_weight
num_neg = (y_train == 0).sum()
num_pos = (y_train == 1).sum()
base_scale_weight = num_neg / num_pos if num_pos > 0 else 1.0
print(f"Imbalance ratio in Train set: {num_neg} neg / {num_pos} pos = {base_scale_weight:.2f}")

# 4. Định nghĩa hàm tối ưu hóa Optuna
def objective(trial):
    params = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": trial.suggest_int("max_depth", 3, 9),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 50, 150),
        "scale_pos_weight": trial.suggest_float("scale_pos_weight", base_scale_weight * 0.5, base_scale_weight * 1.5),
        "random_state": 42
    }
    
    with mlflow.start_run(run_name=f"optuna_trial_{trial.number}", nested=True):
        mlflow.log_params(params)
        
        # Huấn luyện mô hình
        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train)
        
        # Đánh giá F1-score trên Test Set
        preds = model.predict(X_test)
        f1 = f1_score(y_test, preds, zero_division=0)
        
        mlflow.log_metric("f1_score", f1)
        return f1

# 5. Chạy Optuna tối ưu hóa tham số
print("\nStarting Optuna optimization (Running 15 trials)...")
with mlflow.start_run(run_name="optuna_parent_run") as parent_run:
    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=15)
    
    best_params = study.best_params
    best_value = study.best_value
    print(f"\nBest trial F1-score: {best_value:.4f}")
    print(f"Best parameters: {best_params}")
    
    mlflow.log_params(best_params)
    mlflow.log_metric("best_f1_score", best_value)
    
    # 6. Huấn luyện mô hình cuối cùng với tham số tốt nhất
    print("\nTraining final model with best parameters...")
    final_model = xgb.XGBClassifier(**best_params)
    final_model.fit(X_train, y_train)
    
    # Đánh giá hiệu năng chi tiết
    preds = final_model.predict(X_test)
    final_f1 = f1_score(y_test, preds, zero_division=0)
    print(f"Final Model F1-score: {final_f1:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, preds))
    
    # Luôn log mô hình vào parent run của MLflow để lưu trữ
    mlflow.xgboost.log_model(xgb_model=final_model, artifact_path="model")
    
    # Tính toán và lưu thông số chuẩn hóa Amount để phục vụ cho serving
    amount_mean = df["Amount"].mean()
    amount_std = df["Amount"].std()
    scaler_stats = {
        "amount_mean": float(amount_mean),
        "amount_std": float(amount_std if amount_std > 0 else 1.0)
    }
    import json
    with open("scaler_stats.json", "w") as f:
        json.dump(scaler_stats, f)
        
    mlflow.log_artifact("scaler_stats.json", artifact_path="model")
    print(f"Logged scaler_stats.json to MLflow (mean: {amount_mean:.4f}, std: {amount_std:.4f})")
    
    # 7. So sánh và thăng cấp (F1 mới >= F1 cũ + 0.01 hoặc chưa có mô hình chạy thực tế)
    promotion_threshold = production_f1 + 0.01
    if final_f1 >= promotion_threshold or production_f1 == 0.0:
        print(f"\n[PROMOTION] New model F1-score ({final_f1:.4f}) meets the promotion threshold (>= {promotion_threshold:.4f}).")
        
        # Đăng ký mô hình vào Registry từ run hiện tại
        run_uri = f"runs:/{parent_run.info.run_id}/model"
        model_version = mlflow.register_model(run_uri, model_name)
        new_version = model_version.version
        print(f"New model registered as version {new_version} in MLflow Registry.")
        
        # Tự động chuyển version mới lên Staging
        print(f"Transitioning version {new_version} to 'Staging'...")
        client.transition_model_version_stage(
            name=model_name,
            version=new_version,
            stage="Staging"
        )
        print(f"[SUCCESS] Version {new_version} is now in 'Staging' stage and ready for integration tests!")
    else:
        print(f"\n[REJECTED] New model F1-score ({final_f1:.4f}) is lower than the promotion threshold (< {promotion_threshold:.4f}).")
        print("Skipped model registration and promotion.")

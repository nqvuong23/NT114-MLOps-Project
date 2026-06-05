import pandas as pd
import numpy as np
import xgboost as xgb
import optuna
import mlflow
import mlflow.xgboost
from sklearn.metrics import f1_score, classification_report, confusion_matrix
import os

# 1. Cấu hình kết nối tới MLflow và MinIO S3 Mock
mlflow.set_tracking_uri("http://localhost:5000")
os.environ["AWS_ACCESS_KEY_ID"] = "minio_admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio_password"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

mlflow.set_experiment("Real_Data_Fraud_Detection")

# 2. Đọc dữ liệu thật từ file CSV
print("Loading real dataset (creditcard.csv)...")
df = pd.read_csv("creditcard.csv")
print(f"Dataset loaded. Total shape: {df.shape}")

# 3. Phân tách Train / Test theo thời gian (cột Time)
# Sắp xếp theo Time trước để đảm bảo tính tuần tự
df = df.sort_values(by="Time").reset_index(drop=True)

X = df.drop(columns=["Class"])
y = df["Class"]

# Tính mốc thời gian để cắt 80% dữ liệu đầu cho Train, 20% sau cho Test
split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Train set: {X_train.shape[0]} samples (Fraud: {y_train.sum()})")
print(f"Test set: {X_test.shape[0]} samples (Fraud: {y_test.sum()})")

# Tính toán tỷ lệ mất cân bằng để làm baseline cho scale_pos_weight
num_neg = (y_train == 0).sum()
num_pos = (y_train == 1).sum()
base_scale_weight = num_neg / num_pos
print(f"Imbalance ratio in Train set: {num_neg} neg / {num_pos} pos = {base_scale_weight:.2f}")

# 4. Định nghĩa hàm tối ưu hóa Optuna
def objective(trial):
    params = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": trial.suggest_int("max_depth", 3, 9),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 50, 150),
        # Optuna sẽ tự điều chỉnh scale_pos_weight quanh tỷ lệ mất cân bằng cơ sở
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
    
    # Đẩy mô hình lên S3 và đăng ký vào Model Registry
    mlflow.xgboost.log_model(
        xgb_model=final_model,
        artifact_path="model",
        registered_model_name="CreditCardFraudModel"
    )
    print("\n[SUCCESS] Final model logged and registered in MLflow Registry as 'CreditCardFraudModel'!")

import os
import json
import mlflow
from mlflow.tracking import MlflowClient

def main():
    # Lấy thông tin URL của MLflow Tracking từ biến môi trường
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    
    model_name = "CreditCardFraudModel"
    print(f"Đang tìm kiếm mô hình ở trạng thái Staging cho {model_name}...")
    
    # Tìm kiếm phiên bản Staging mới nhất
    versions = client.get_latest_versions(model_name, stages=["Staging"])
    if not versions:
        print("Không thấy phiên bản Staging nào. Thử tìm kiếm phiên bản Production...")
        versions = client.get_latest_versions(model_name, stages=["Production"])
        
    if not versions:
        raise Exception("Không tìm thấy bất kỳ phiên bản mô hình nào ở Staging hoặc Production!")
        
    latest_version = versions[0]
    print(f"Đang tải Version {latest_version.version} (Run ID: {latest_version.run_id}) từ S3 MLflow...")
    
    # Tải toàn bộ thư mục model (gồm model và scaler_stats.json) về thư mục gốc
    mlflow.artifacts.download_artifacts(
        run_id=latest_version.run_id,
        artifact_path="model",
        dst_path="."
    )
    print("Tải mô hình thành công về thư mục ./model")

if __name__ == "__main__":
    main()

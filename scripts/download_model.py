import os
import sys
import psycopg2
import boto3
import shutil

# Đảm bảo in ký tự Unicode tiếng Việt không bị lỗi trên Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass


def load_env(env_path=".env"):
    if os.path.exists(env_path):
        print("Loading local .env file...")
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    parts = line.split("=", 1)
                    key = parts[0].strip()
                    val = parts[1].strip()
                    if val.startswith('"') and val.endswith('"'):
                        val = val[1:-1]
                    elif val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]
                    os.environ[key] = val

def map_uri_to_s3_key(uri):
    if uri.startswith("mlflow-artifacts:/"):
        path = uri.replace("mlflow-artifacts:/", "")
    elif uri.startswith("mlflow-artifacts://"):
        path = uri.replace("mlflow-artifacts://", "")
    else:
        path = uri
    return f"mlflow-artifacts-store/{path.lstrip('/')}"

def download_s3_folder(s3_client, bucket_name, s3_prefix, local_dir):
    print(f"Downloading from s3://{bucket_name}/{s3_prefix} to {local_dir}...")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
    if 'Contents' not in response:
        print(f"No files found under prefix {s3_prefix}")
        return False
        
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('/'):
            continue
            
        # Get relative path from prefix
        rel_path = os.path.relpath(key, s3_prefix)
        if rel_path == '.':
            rel_path = os.path.basename(key)
            
        dest_path = os.path.join(local_dir, rel_path)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        print(f"Downloading {key} -> {dest_path}")
        s3_client.download_file(bucket_name, key, dest_path)
    return True

def main():
    load_env()
    
    db_uri = os.environ.get("MLFLOW_TRACKING_URI") or os.environ.get("MLFLOW_BACKEND_STORE_URI")
    if not db_uri:
        raise Exception("Không tìm thấy biến môi trường MLFLOW_TRACKING_URI hoặc MLFLOW_BACKEND_STORE_URI!")
        
    print(f"Sử dụng DB URI: {db_uri.split('@')[-1] if '@' in db_uri else db_uri} để truy vấn thông tin mô hình...")
    
    if db_uri.startswith("postgresql+psycopg2://"):
        db_uri = db_uri.replace("postgresql+psycopg2://", "postgresql://")

    # Đảm bảo thư mục model/ tồn tại và trống rỗng
    dest_dir = "./model"
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir, exist_ok=True)

    try:
        conn = psycopg2.connect(db_uri)
        cursor = conn.cursor()
        
        model_name = "CreditCardFraudModel"
        print(f"Truy vấn thông tin mô hình '{model_name}' ở trạng thái Staging/Production...")
        cursor.execute("""
            SELECT version, run_id, current_stage, storage_location 
            FROM model_versions 
            WHERE name = %s AND current_stage IN ('Staging', 'Production')
            ORDER BY CASE current_stage WHEN 'Staging' THEN 1 WHEN 'Production' THEN 2 ELSE 3 END, version DESC;
        """, (model_name,))
        
        rows = cursor.fetchall()
        if not rows:
            raise Exception("Không tìm thấy bất kỳ phiên bản mô hình nào ở trạng thái Staging hoặc Production!")
            
        latest = rows[0]
        version, run_id, stage, storage_location = latest
        print(f"Phiên bản mới nhất tìm thấy: Version {version} ({stage})")
        print(f"Run ID: {run_id}")
        print(f"Storage Location URI: {storage_location}")
        
        # Lấy experiment_id từ run_id
        cursor.execute("SELECT experiment_id FROM runs WHERE run_uuid = %s;", (run_id,))
        run_row = cursor.fetchone()
        if not run_row:
            raise Exception(f"Không tìm thấy run tương ứng trong bảng runs cho run_id: {run_id}")
        experiment_id = run_row[0]
        
        cursor.close()
        conn.close()
        
        # Bắt đầu tải các tệp từ S3
        bucket_name = os.environ.get("S3_BUCKET", "nt114-mlops-bucket")
        print(f"Khởi tạo kết nối boto3 S3 tới bucket: {bucket_name}...")
        s3_client = boto3.client('s3')
        
        # 1. Tải các tệp mô hình từ model storage location
        model_s3_prefix = map_uri_to_s3_key(storage_location)
        download_s3_folder(s3_client, bucket_name, model_s3_prefix, dest_dir)
        
        # 2. Đảm bảo có tệp scaler_stats.json (tải từ run prefix nếu chưa có trong model storage location)
        scaler_stats_path = os.path.join(dest_dir, "scaler_stats.json")
        if not os.path.exists(scaler_stats_path):
            run_model_prefix = f"mlflow-artifacts-store/{experiment_id}/{run_id}/artifacts/model"
            print(f"scaler_stats.json chưa có trong model storage location. Đang tải từ run prefix: {run_model_prefix}")
            scaler_stats_s3_key = f"{run_model_prefix}/scaler_stats.json"
            try:
                s3_client.download_file(bucket_name, scaler_stats_s3_key, scaler_stats_path)
                print(f"Đã tải thành công scaler_stats.json từ run artifacts về {scaler_stats_path}")
            except Exception as ex:
                raise Exception(f"Lỗi khi tải scaler_stats.json từ S3 run prefix: {ex}")
                
        print(f"Tải thành công toàn bộ mô hình phiên bản {version} về {dest_dir}!")
        print(f"Danh sách tệp tin trong thư mục model: {os.listdir(dest_dir)}")
        
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
        raise e

if __name__ == "__main__":
    main()

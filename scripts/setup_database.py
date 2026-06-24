import os, psycopg2, sys
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()

try:
    # Bước 1: Kết nối tới DB mặc định 'postgres' để có quyền tạo DB mới
    admin_conn = psycopg2.connect(
        host=os.environ['RDS_HOST'],
        port=int(os.environ.get('RDS_PORT', 5431)),
        database=os.environ['RDS_DEFAULT_DB'],  
        user=os.environ['RDS_USER'],
        password=os.environ['RDS_PASSWORD'],
        connect_timeout=10,
    )
    # Bắt buộc phải bật autocommit để chạy lệnh CREATE DATABASE
    admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    admin_cur = admin_conn.cursor()

    tx_db = os.environ['RDS_TRANSACTIONS_DB']
    mlflow_db = os.environ['RDS_MLFLOW_DB']

    # Kiểm tra và tạo database Transactions
    admin_cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{tx_db}';")
    if not admin_cur.fetchone():
        print(f"[*] Creating database '{tx_db}'...")
        admin_cur.execute(f"CREATE DATABASE {tx_db};")
    else:
        print(f"[✓] Database '{tx_db}' already exists")

    # Kiểm tra và tạo database MLflow
    admin_cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{mlflow_db}';")
    if not admin_cur.fetchone():
        print(f"[*] Creating database '{mlflow_db}'...")
        admin_cur.execute(f"CREATE DATABASE {mlflow_db};")
    else:
        print(f"[✓] Database '{mlflow_db}' already exists")

    admin_cur.close()
    admin_conn.close()

    # Bước 2: Kết nối trực tiếp vào database Transactions để tạo bảng
    print(f"[*] Connecting to '{tx_db}' to create tables...")
    conn = psycopg2.connect(
        host=os.environ['RDS_HOST'],
        port=int(os.environ.get('RDS_PORT', 5432)),
        database=tx_db, 
        user=os.environ['RDS_USER'],
        password=os.environ['RDS_PASSWORD'],
        connect_timeout=10,
    )
    
    v_cols = "\n".join([f'    "V{i}" DOUBLE PRECISION,' for i in range(1, 29)])
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id   VARCHAR(36) PRIMARY KEY,
            user_id          VARCHAR(20) NOT NULL,
            card_id          VARCHAR(20) NOT NULL,
            timestamp        TIMESTAMPTZ NOT NULL,
            amount           NUMERIC(12, 2) NOT NULL,
            merchant_id      VARCHAR(20),
            merchant_category VARCHAR(50),
            is_international  BOOLEAN DEFAULT FALSE,
            hour_of_day      SMALLINT,
{v_cols}
            is_fraud_label   SMALLINT NOT NULL DEFAULT 0,
            created_at       TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (timestamp);
        CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions (created_at);
        CREATE INDEX IF NOT EXISTS idx_transactions_card_id ON transactions (card_id);
        """)
    conn.commit()
    conn.close()
    print(f"[✓] RDS table 'transactions' ready inside '{tx_db}'")

except Exception as e:
    print(f"[!] RDS setup failed: {e}")
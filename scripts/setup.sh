#!/bin/bash
# =============================================================================
# setup.sh
# =============================================================================
set -euo pipefail

echo "========================================="
echo " VM Setup"
echo "========================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_ROOT}/.venv"

# Tải các package cần thiết
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3-pip ca-certificates curl

# ── Tạo và kích hoạt Virtual Environment ─────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    echo "[+] Creating Python virtual environment at $VENV_DIR ..."
    python3.12 -m venv "$VENV_DIR"
    echo "[✓] Virtual environment created"
else
    echo "[✓] Virtual environment already exists — skipping creation"
fi
 
# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"
echo "[✓] Virtual environment activated: ${VIRTUAL_ENV}"

# Cài các thư viện của Python
pip install -r "${PROJECT_ROOT}/requirements.txt"

# Cài Docker
if ! command -v docker &>/dev/null; then
    echo "[+] Installing Docker..."

    sudo apt-get remove -y \
        docker.io docker-compose docker-compose-v2 \
        docker-doc podman-docker containerd runc 2>/dev/null || true
 
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc
 
    . /etc/os-release
    UBUNTU_CODENAME="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
    if [ -z "$UBUNTU_CODENAME" ]; then
        echo "[✗] Cannot determine Ubuntu codename from /etc/os-release"
        exit 1
    fi
 
    sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<DOCKEREOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
DOCKEREOF
 
    sudo apt-get update
    sudo apt-get install -y \
        docker-ce docker-ce-cli containerd.io \
        docker-buildx-plugin docker-compose-plugin
 
    sudo usermod -aG docker "$USER"
    sudo systemctl enable docker
    sudo systemctl start docker
    echo "[✓] Docker installed: $(docker --version)"
else
    echo "[✓] Docker already installed: $(docker --version) — skipping"
fi

# ── Load env ──────────────────────────────────────────────────────────────────
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
else
    echo "[✗] .env not found!"
    exit 1
fi

# ── Tạo RDS Databases và Table ─────────────────────────────────────────
echo ""
echo "[4/4] Setting up RDS Databases and Tables..."

if python3 -c "import psycopg2" 2>/dev/null; then
    python3 << PYEOF
import os, psycopg2, sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    # Bước 1: Kết nối tới DB mặc định 'postgres' để có quyền tạo DB mới
    admin_conn = psycopg2.connect(
        host=os.environ['RDS_HOST'],
        port=int(os.environ.get('RDS_PORT', 5432)),
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
PYEOF
else
    echo "[!] psycopg2 not installed — skipping RDS setup"
    echo "    Install: pip install psycopg2-binary"
fi

# Cài Apache Airflow và MLflow và Spark
echo "[+] Setting Airflow directory permissions..."
sudo mkdir -p "${PROJECT_ROOT}/airflow/"{logs,dags,plugins,config}
sudo chown -R 50000:0 "${PROJECT_ROOT}/airflow/"{logs,dags,plugins,config}
sudo chmod -R 777 "${PROJECT_ROOT}/airflow/"{logs,dags,plugins,config}

echo "[+] Starting Airflow + MLflow via docker compose..."
docker compose -f "${PROJECT_ROOT}/docker-compose.yaml" up -d || true
echo "[✓] Docker Compose services started"

echo ""
echo "========================================="
echo " VM Setup COMPLETE ✅"
echo "========================================="
echo ""

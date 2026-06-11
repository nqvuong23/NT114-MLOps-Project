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

# Tải các package cần thiết
sudo apt update
sudo apt install -y python3 python3-venv python3-pip unzip ca-certificates curl tar

# Cài các thư viện của Python
pip3 install -r "${PROJECT_ROOT}/requirements.txt"

# Cài AWS CLI
if ! command -v aws &>/dev/null; then
    echo "[+] Installing AWS CLI..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
    unzip -q /tmp/awscliv2.zip -d /tmp/
    sudo /tmp/aws/install
    rm -rf /tmp/awscliv2.zip /tmp/aws
    echo "[✓] AWS CLI installed: $(aws --version)"
else
    echo "[✓] AWS CLI already installed: $(aws --version) — skipping"
fi

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
 
    sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME}
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
 
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

# Cài Apache Spark
mkdir -p ~/spark_notebooks
chmod 777 ~/spark_notebooks
echo "[+] Starting Spark notebook container..."
docker run -d \
  --name my_spark_lab \
  -p 8888:8888 \
  -p 4040:4040 \
  -v ~/spark_notebooks:/home/jovyan/work \
  jupyter/pyspark-notebook
echo "[✓] Spark container started"

# ── Load env ──────────────────────────────────────────────────────────────────
if [ -f "$PROJECT_ROOT/.env.production" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env.production" | xargs)
else
    echo "[✗] .env.production not found!"
    exit 1
fi

# Cài Apache Airflow và MLflow
echo "[+] Starting Airflow + MLflow via docker compose..."
docker compose -f "${PROJECT_ROOT}/docker-compose.yaml" up -d
echo "[✓] Docker Compose services started"

# ── Tạo RDS table (nếu đã có RDS) ─────────────────────────────────────────
echo ""
echo "[4/4] Setting up RDS table..."

if python3 -c "import psycopg2" 2>/dev/null; then
    python3 << PYEOF
import os, psycopg2, sys

try:
    conn = psycopg2.connect(
        host=os.environ['RDS_HOST'],
        port=int(os.environ.get('RDS_PORT', 5432)),
        database=os.environ['RDS_DB'],
        user=os.environ['RDS_USER'],
        password=os.environ['RDS_PASSWORD'],
        sslmode=os.environ.get('RDS_SSLMODE', 'verify-full'),
        sslrootcert=os.environ.get('RDS_SSLROOTCERT', './global-bundle.pem'),
        connect_timeout=10,
    )
    print("[✓] RDS connection OK")

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
    print("[✓] RDS table 'transactions' ready")
except Exception as e:
    print(f"[!] RDS setup skipped: {e}")
    print("    Run scripts/simulate_transactions.py with --setup to create table later")
PYEOF
else
    echo "[!] psycopg2 not installed — skipping RDS setup"
    echo "    Install: pip install psycopg2-binary"
fi

echo ""
echo "========================================="
echo " VM Setup COMPLETE ✅"
echo "========================================="
echo ""

echo "========================================="
echo " DVC Setup for Fraud Detection MLOps"
echo "========================================="

# ── Cài DVC + S3 support ───────────────────────────────────────────────────
if ! command -v dvc &>/dev/null; then
    echo "[1/5] Installing DVC with S3 support..."
    # FIX: dùng pip3
    pip3 install "dvc[s3]" --quiet
    echo "[✓] DVC installed: $(dvc --version)"
else
    echo "[1/5] DVC already installed: $(dvc --version) — skipping"
fi

# ── Init DVC project ───────────────────────────────────────────────────────
DVC_PROJECT_DIR="$PROJECT_ROOT/dvc_project"
mkdir -p "$DVC_PROJECT_DIR"
cd "$DVC_PROJECT_DIR"
 
if [ ! -d ".dvc" ]; then
    echo ""
    echo "[2/5] Initializing DVC project..."
    git init 2>/dev/null || true
    dvc init
    echo "[✓] DVC initialized"
else
    echo "[2/5] DVC already initialized — skipping"
fi


# ── Cấu hình S3 remote ─────────────────────────────────────────────────────
echo ""
echo "[3/5] Configuring S3 remote..."
 
S3_REMOTE_URL="s3://${S3_BUCKET}/${S3_DVC_REMOTE:-dvc-store}"
 
dvc remote add -f myremote "$S3_REMOTE_URL" 2>/dev/null || true
dvc remote modify myremote region "${AWS_DEFAULT_REGION:-ap-southeast-1}"
dvc remote default myremote
 
echo "[✓] DVC remote: $S3_REMOTE_URL"
 
# ── Tạo .dvcignore ─────────────────────────────────────────────────────────
echo ""
echo "[4/5] Creating .dvcignore..."
cat > .dvcignore << 'EOF'
# DVC ignore file
__pycache__
*.pyc
.DS_Store
*.log
EOF
 
# ── Commit initial DVC config ──────────────────────────────────────────────
echo ""
echo "[5/5] Committing DVC config to git..."
git add .dvc/ .dvcignore 2>/dev/null || true
git config user.email "mlops@fraud-detection.local" 2>/dev/null || true
git config user.name "MLOps Pipeline" 2>/dev/null || true
git commit -m "feat: initialize DVC with S3 remote" --allow-empty 2>/dev/null || true
 
echo ""
echo "========================================="
echo " DVC Setup COMPLETE ✅"
echo " Remote: $S3_REMOTE_URL"
echo " Project: $DVC_PROJECT_DIR"
echo "========================================="
echo ""
echo "Test DVC connection:"
echo "  cd $DVC_PROJECT_DIR"
echo "  dvc remote list"

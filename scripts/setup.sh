#!/bin/bash
# =============================================================================
# setup.sh
# =============================================================================
set -euo pipefail

echo "========================================="
echo " AWS Infrastructure Setup"
echo "========================================="

# ── Load env ──────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ -f "$PROJECT_ROOT/.env.production" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env.production" | xargs)
else
    echo "[✗] .env.production not found!"
    exit 1
fi

REGION="${AWS_DEFAULT_REGION:-ap-southeast-1}"
BUCKET="$S3_BUCKET"

# ── 4. Tạo RDS table (nếu đã có RDS) ─────────────────────────────────────────
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
        sslrootcert=os.environ.get('RDS_SSLROOTCERT', './global-bundle.pem')
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
echo " AWS Setup COMPLETE ✅"
echo "========================================="
echo ""

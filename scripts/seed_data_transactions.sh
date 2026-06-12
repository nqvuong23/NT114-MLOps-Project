#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load env variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
else
    echo "[✗] .env not found!"
    exit 1
fi

# Seed 500 transactions (cold start)
python3 scripts/simulate_transactions.py --count 500 --batch-seconds 3600

# Kiểm tra data trong RDS
python3 - << 'EOF'
import psycopg2, os
conn = psycopg2.connect(
    host=os.environ['RDS_HOST'],
    port=int(os.environ.get('RDS_PORT', 5432)),
    database=os.environ['RDS_TRANSACTIONS_DB'],
    user=os.environ['RDS_USER'],
    password=os.environ['RDS_PASSWORD'],
    connect_timeout=10,
)
with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*), SUM(is_fraud_label) FROM transactions;")
    total, frauds = cur.fetchone()
    print(f"Total: {total} | Frauds: {frauds} ({frauds/total*100:.3f}%)")
conn.close()
EOF
"""
simulate_transactions.py
========================
Giả lập dữ liệu giao dịch thô của user và INSERT vào RDS PostgreSQL.
Script này chạy định kỳ (hoặc một lần để seed) để tạo dữ liệu mới.

Raw input schema:
  - transaction_id  : UUID duy nhất
  - user_id         : ID user
  - card_id         : ID thẻ
  - timestamp       : thời điểm giao dịch (UTC)
  - amount          : số tiền giao dịch (USD)
  - merchant_id     : ID merchant
  - merchant_category: loại merchant (grocery, online, travel, ...)
  - is_international: giao dịch quốc tế hay không
  - hour_of_day     : giờ trong ngày (0-23)
  - V1..V28         : 28 PCA features từ Kaggle dataset
                      (đã là PCA nên không có ý nghĩa thực, dùng để giữ
                       đúng schema cho model Kaggle)
  - is_fraud_label  : 0 hoặc 1 — TRƯỜNG GÁN NHÃN CHỈ DÙNG CHO TRAINING
                      KHÔNG được đưa vào model khi inference

NOTE: Trong production thực tế, is_fraud_label KHÔNG tồn tại.
      Đây chỉ là trường đặc biệt cho đồ án để có labeled data.
"""

import os
import uuid
import random
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Kaggle dataset statistics (dùng để sinh V1..V28 thực tế) ──────────────────
# Mean và std của từng feature V1-V28 từ Kaggle creditcard.csv
# Source: gpreda notebook analysis
KAGGLE_STATS = {
    "V1":  {"mean": -0.016, "std": 1.931, "fraud_mean": -4.771, "fraud_std": 3.246},
    "V2":  {"mean":  0.005, "std": 1.726, "fraud_mean":  3.623, "fraud_std": 5.043},
    "V3":  {"mean": -0.002, "std": 1.577, "fraud_mean": -7.033, "fraud_std": 4.929},
    "V4":  {"mean":  0.000, "std": 1.415, "fraud_mean":  4.413, "fraud_std": 2.980},
    "V5":  {"mean": -0.000, "std": 1.401, "fraud_mean": -3.102, "fraud_std": 7.297},
    "V6":  {"mean": -0.000, "std": 1.332, "fraud_mean": -1.360, "fraud_std": 4.091},
    "V7":  {"mean": -0.000, "std": 1.237, "fraud_mean": -5.568, "fraud_std": 6.836},
    "V8":  {"mean":  0.000, "std": 1.194, "fraud_mean":  0.570, "fraud_std": 7.003},
    "V9":  {"mean":  0.000, "std": 1.099, "fraud_mean": -2.581, "fraud_std": 4.163},
    "V10": {"mean":  0.000, "std": 1.088, "fraud_mean": -4.346, "fraud_std": 4.456},
    "V11": {"mean":  0.000, "std": 1.020, "fraud_mean":  3.748, "fraud_std": 3.413},
    "V12": {"mean": -0.000, "std": 0.999, "fraud_mean": -7.024, "fraud_std": 4.986},
    "V13": {"mean":  0.000, "std": 0.995, "fraud_mean": -0.050, "fraud_std": 2.960},
    "V14": {"mean":  0.000, "std": 0.958, "fraud_mean": -8.109, "fraud_std": 5.106},
    "V15": {"mean":  0.000, "std": 0.915, "fraud_mean":  0.091, "fraud_std": 2.749},
    "V16": {"mean":  0.000, "std": 0.876, "fraud_mean": -6.055, "fraud_std": 4.784},
    "V17": {"mean": -0.000, "std": 0.849, "fraud_mean": -8.993, "fraud_std": 6.476},
    "V18": {"mean":  0.000, "std": 0.839, "fraud_mean": -2.984, "fraud_std": 3.820},
    "V19": {"mean":  0.000, "std": 0.814, "fraud_mean":  1.413, "fraud_std": 3.305},
    "V20": {"mean":  0.000, "std": 0.771, "fraud_mean":  0.389, "fraud_std": 3.413},
    "V21": {"mean":  0.000, "std": 0.734, "fraud_mean":  0.553, "fraud_std": 1.985},
    "V22": {"mean":  0.000, "std": 0.725, "fraud_mean": -0.085, "fraud_std": 1.378},
    "V23": {"mean": -0.000, "std": 0.624, "fraud_mean": -0.278, "fraud_std": 2.029},
    "V24": {"mean":  0.000, "std": 0.606, "fraud_mean":  0.077, "fraud_std": 1.101},
    "V25": {"mean":  0.000, "std": 0.521, "fraud_mean":  0.107, "fraud_std": 1.195},
    "V26": {"mean":  0.000, "std": 0.482, "fraud_mean": -0.122, "fraud_std": 0.864},
    "V27": {"mean":  0.000, "std": 0.404, "fraud_mean":  0.326, "fraud_std": 1.174},
    "V28": {"mean":  0.000, "std": 0.330, "fraud_mean":  0.116, "fraud_std": 0.570},
}

MERCHANT_CATEGORIES = ["grocery", "online", "travel", "restaurant", "entertainment",
                        "gas_station", "pharmacy", "electronics", "clothing", "atm"]

USER_IDS = [f"USER_{i:05d}" for i in range(1, 501)]     # 500 users
CARD_IDS  = [f"CARD_{i:05d}" for i in range(1, 601)]    # 600 cards
MERCHANT_IDS = [f"MER_{i:04d}" for i in range(1, 201)]  # 200 merchants


def generate_pca_features(is_fraud: bool) -> dict:
    """Sinh V1..V28 theo phân phối thực tế từ Kaggle dataset."""
    result = {}
    for col, stats in KAGGLE_STATS.items():
        if is_fraud:
            val = np.random.normal(stats["fraud_mean"], stats["fraud_std"])
        else:
            val = np.random.normal(stats["mean"], stats["std"])
        result[col] = round(float(val), 6)
    return result


def generate_transaction(base_time: datetime) -> dict:
    """
    Tạo một transaction giả lập.
    Tỉ lệ fraud ~0.172% như Kaggle dataset.
    """
    is_fraud = random.random() < 0.00172

    if is_fraud:
        # Fraud pattern: amount cao, giờ đêm, quốc tế
        amount = round(random.uniform(100, 5000) * random.choice([1, 1, 2, 5]), 2)
        hour = random.choice([0, 1, 2, 3, 4, 22, 23])
        is_international = random.random() < 0.7
    else:
        # Normal pattern
        amount = round(random.expovariate(1/50), 2)  # mean ~$50
        amount = min(amount, 3000)
        hour = random.choices(range(24), weights=[
            1,1,1,1,1,1, 2,4,6,7,8,8, 8,8,7,7,6,6, 7,7,6,5,3,2
        ])[0]
        is_international = random.random() < 0.05

    pca_features = generate_pca_features(is_fraud)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "card_id": random.choice(CARD_IDS),
        "timestamp": base_time.isoformat(),
        "amount": amount,
        "merchant_id": random.choice(MERCHANT_IDS),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "is_international": is_international,
        "hour_of_day": hour,
        **pca_features,
        # ── LABEL FIELD (chỉ dùng cho training, KHÔNG inference) ──
        "is_fraud_label": int(is_fraud),
    }


def get_db_connection():
    return psycopg2.connect(
        host=os.environ["RDS_HOST"],
        port=int(os.environ.get("RDS_PORT", 5432)),
        database=os.environ["RDS_TRANSACTIONS_DB"],
        user=os.environ["RDS_USER"],
        password=os.environ["RDS_PASSWORD"],
        connect_timeout=10,
    )


def setup_table(conn):
    """Tạo bảng transactions nếu chưa tồn tại."""
    v_cols = "\n".join([f'    "{v}" DOUBLE PRECISION,' for v in KAGGLE_STATS.keys()])
    ddl = f"""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id  VARCHAR(36) PRIMARY KEY,
        user_id         VARCHAR(20) NOT NULL,
        card_id         VARCHAR(20) NOT NULL,
        timestamp       TIMESTAMPTZ NOT NULL,
        amount          NUMERIC(12, 2) NOT NULL,
        merchant_id     VARCHAR(20),
        merchant_category VARCHAR(50),
        is_international BOOLEAN,
        hour_of_day     SMALLINT,
{v_cols}
        is_fraud_label  SMALLINT NOT NULL DEFAULT 0,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_transactions_timestamp
        ON transactions (timestamp);
    CREATE INDEX IF NOT EXISTS idx_transactions_created_at
        ON transactions (created_at);
    CREATE INDEX IF NOT EXISTS idx_transactions_card_id 
        ON transactions (card_id);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("Table 'transactions' ready.")


def insert_transactions(conn, records: list[dict]):
    """Batch insert transactions vào PostgreSQL."""
    v_col_names = list(KAGGLE_STATS.keys())
    all_cols = (
        ["transaction_id", "user_id", "card_id", "timestamp", "amount",
         "merchant_id", "merchant_category", "is_international", "hour_of_day"]
        + v_col_names
        + ["is_fraud_label"]
    )
    placeholders = ", ".join(["%s"] * len(all_cols))
    col_str = ", ".join([f'"{c}"' for c in all_cols])
    sql = f'INSERT INTO transactions ({col_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'

    rows = [[r[c] for c in all_cols] for r in records]
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    logger.info(f"Inserted {len(rows)} transactions.")


def main(n: int, batch_seconds: int = 300):
    """
    Sinh n transactions phân bố đều trong khoảng batch_seconds giây vừa qua.
    """
    now = datetime.now(tz=timezone.utc)
    transactions = []
    for _ in range(n):
        offset = random.uniform(0, batch_seconds)
        from datetime import timedelta
        tx_time = now - timedelta(seconds=offset)
        transactions.append(generate_transaction(tx_time))

    fraud_count = sum(t["is_fraud_label"] for t in transactions)
    logger.info(f"Generated {n} transactions | fraud: {fraud_count} ({fraud_count/n*100:.3f}%)")

    conn = get_db_connection()
    try:
        setup_table(conn)
        insert_transactions(conn, transactions)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate fraud detection transactions")
    parser.add_argument("--count", type=int, default=500,
                        help="Number of transactions to generate (default: 100)")
    parser.add_argument("--batch-seconds", type=int, default=300,
                        help="Spread transactions over this many seconds (default: 300 = 5min)")
    args = parser.parse_args()

    required_env = ["RDS_HOST", "RDS_TRANSACTIONS_DB", "RDS_USER", "RDS_PASSWORD"]
    missing = [e for e in required_env if not os.environ.get(e)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {missing}")

    main(args.count, args.batch_seconds)
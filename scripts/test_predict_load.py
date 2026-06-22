import requests
import time
import random
import uuid

# URL API của ECS Task Fargate đang chạy
API_URL = "http://3.0.181.57:3000/predict"

def generate_mock_transaction():
    """Tạo ra một bản ghi giao dịch giả lập khớp với schema của BentoML /predict"""
    is_fraud = random.random() < 0.05  # Tăng tỷ lệ fraud để test dashboard hiển thị
    
    if is_fraud:
        amount = round(random.uniform(500, 3000), 2)
        hour = random.choice([0, 1, 2, 3, 4, 22, 23])
    else:
        amount = round(random.uniform(5, 150), 2)
        hour = random.choice(range(24))
        
    record = {
        "Time": float(hour),  # hour_of_day dùng làm proxy cho Time
        "Amount": float(amount)
    }
    
    # Sinh 28 đặc trưng V1..V28 ngẫu nhiên
    for i in range(1, 29):
        val = random.normalvariate(-4.0, 3.0) if is_fraud else random.normalvariate(0.0, 1.0)
        record[f"V{i}"] = round(val, 6)
        
    return record

def main():
    print(f"Bắt đầu gửi tải giả lập tới API: {API_URL}...")
    print("Mỗi giây sẽ gửi từ 1 - 5 requests. Nhấn Ctrl+C để dừng.")
    
    total_sent = 0
    try:
        while True:
            # Gửi ngẫu nhiên từ 1 đến 5 requests mỗi giây
            num_requests = random.randint(1, 5)
            
            # Gộp thành 1 batch gồm nhiều giao dịch để gửi
            batch = [generate_mock_transaction() for _ in range(num_requests)]
            payload = {"request_data": batch}
            
            start_time = time.time()
            try:
                response = requests.post(API_URL, json=payload, timeout=5)
                latency = (time.time() - start_time) * 1000  # ms
                
                if response.status_code == 200:
                    resp_json = response.json()
                    print(f"[OK] Gửi thành công {len(batch)} records | Latency: {latency:.2f}ms | Fraud detected: {resp_json.get('fraud_detected')}")
                    total_sent += len(batch)
                else:
                    print(f"[FAIL] HTTP {response.status_code}: {response.text}")
            except Exception as e:
                print(f"[ERROR] Không kết nối được tới API: {e}")
                
            time.sleep(random.uniform(0.5, 1.5))
            
    except KeyboardInterrupt:
        print(f"\nĐã dừng gửi. Tổng cộng đã gửi thành công {total_sent} records!")

if __name__ == "__main__":
    main()

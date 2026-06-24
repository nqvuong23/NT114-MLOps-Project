# Hướng dẫn deploy các tool cho MLOps

## Điều kiện tiên quyết

Phải tạo trước AWS RDS PostgreSQL và lấy `RDS HOST` + `USERNAME` + `PASSWORD`

## Clone và setup project

```
sudo apt update
sudo apt install -y git
cd ~
git clone https://github.com/nqvuong23/NT114-MLOps-Project.git nt114-mlops
cd nt114-mlops
```

## Chạy script deploy các tool cần thiết

Copy và customize giá trị của biến môi trường trong file ``.env để sử dụng (**QUAN TRỌNG**)

```
cp .env.example .env
```

```
# Chạy script setup AWS RDS PostgreSQL
chmod +x scripts/setup_database.sh
bash scripts/setup_database.sh
```

```
# Chạy script setup
chmod +x scripts/setup.sh
bash scripts/setup.sh
```

# Hướng dẫn deploy các tool cho MLOps

## Clone và setup project

```
sudo apt update
sudo apt install -y git
cd ~
git clone https://github.com/nqvuong23/NT114-MLOps-Project.git nt114-mlops
cd nt114-mlops
```

## Chạy script cấu hình AWS RDS

```
# Copy và customize giá trị của biến môi trường để sử dụng (QUAN TRỌNG)
cp .env.example .env
```

**Thêm giá trị cho biến môi trường trong file `.env.production`**

```
# Chạy script setup
chmod +x scripts/setup.sh
bash scripts/setup.sh
```

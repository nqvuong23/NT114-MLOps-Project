# Hướng dẫn deploy các tool cho MLOps

## Cài đặt Docker

```
sudo apt remove $(dpkg --get-selections docker.io docker-compose docker-compose-v2 docker-doc podman-docker containerd runc | cut -f1)
```

```
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
```

- Copy toàn bộ các dòng trong block code sau:

```
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
```

```
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

```
sudo usermod -aG docker $USER

sudo systemctl enable docker
sudo systemctl start docker
sudo systemctl status docker
```

## Deploy Apache Airflow và MLflow

- Nếu dùng máy ảo để deploy thì hãy pull source code từ Repository của GitHub

```
# Di chuyển vào thư mục root của project
docker compose up -d
```

## Deploy Apache Spark

1. Cài Java Runtime

```
sudo apt update
sudo apt install openjdk-21-jre
java -version
```

2. Cài Apache Spark

- Tải và giải nén Apache Spark

```
curl -O https://dlcdn.apache.org/spark/spark-4.0.2/spark-4.0.2-bin-hadoop3.tgz
tar -xvf spark-4.0.2-bin-hadoop3.tgz
sudo mv spark-4.0.2-bin-hadoop3 /opt/spark
```

- Cấu hình biến môi trường

```
# Mở file
nano ~/.bashrc
```

```
# Cấu hình đường dẫn cho Apache Spark (thêm các dòng sau vào file)
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
```

```
# Sau khi lưu và thoát file, chạy lệnh sau để apply thay đổi
source ~/.bashrc
```

3. Kiểm tra

```
spark-shell
```
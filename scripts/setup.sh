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
# if [ ! -d "$VENV_DIR" ]; then
#     echo "[+] Creating Python virtual environment at $VENV_DIR ..."
#     python3.12 -m venv "$VENV_DIR"
#     echo "[✓] Virtual environment created"
# else
#     echo "[✓] Virtual environment already exists — skipping creation"
# fi
 
# # shellcheck source=/dev/null
# source "${VENV_DIR}/bin/activate"
# echo "[✓] Virtual environment activated: ${VIRTUAL_ENV}"

# # Cài các thư viện của Python
# pip install -r "${PROJECT_ROOT}/requirements.txt"

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

# Cài Apache Airflow và MLflow
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

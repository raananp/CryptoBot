#!/bin/bash
# ==============================
# User-Data bootstrap for CryptoBot
# Tested on Amazon Linux 2023 (AL2023)
# ==============================

# Exit on error
set -e

echo "[*] Updating system..."
dnf update -y

echo "[*] Installing base tools (docker, git)..."
dnf install -y docker git

echo "[*] Enabling and starting Docker..."
systemctl enable docker
systemctl start docker

echo "[*] Installing docker-compose v2 plugin..."
dnf install -y docker-compose-plugin
# sanity check
docker compose version || echo "compose plugin not found!"

echo "[*] Adding ec2-user to docker group (so you can run docker without sudo)..."
usermod -aG docker ec2-user

echo "[*] Cloning CryptoBot repo..."
cd /home/ec2-user
git clone https://github.com/raananp/CryptoBot.git
chown -R ec2-user:ec2-user CryptoBot
cd CryptoBot

echo "[*] Bringing up containers..."
# detach mode
docker compose up -d

echo "[*] Bootstrap complete."
#!/usr/bin/env bash
# 현재 프로젝트 경로 기준으로 systemd 유닛만 등록·시작한다.
# 사용: sudo ./service_add.sh
set -euo pipefail

SERVICE_NAME="middleware-edge-forwarder"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}.service"
VENV_PYTHON="${ROOT}/.venv/bin/python"

if [[ "${EUID}" -ne 0 ]]; then
  echo "root 권한이 필요합니다. 예: sudo ./service_add.sh" >&2
  exit 1
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo ".venv 가 없습니다: ${VENV_PYTHON}" >&2
  exit 1
fi

cat > "${UNIT_DST}" <<EOF
[Unit]
Description=Edge Forwarder (collector ZeroMQ -> northbound OPC UA)
After=network-online.target collector-plc.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${ROOT}
EnvironmentFile=-${ROOT}/.env
ExecStart=${VENV_PYTHON} main.py

Restart=on-failure
RestartSec=5
TimeoutStopSec=30

StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service"
systemctl restart "${SERVICE_NAME}.service"
systemctl --no-pager --full status "${SERVICE_NAME}.service" || true

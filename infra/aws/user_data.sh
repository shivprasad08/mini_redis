#!/usr/bin/env bash
set -euo pipefail

# Log all output to console and user-data log
exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Initializing mini-redis node: ${NODE_ID} (${ROLE}) ==="

# Wait for cloud-init / apt locks
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
# Install Python 3 (3.12 on Ubuntu 24.04 LTS), git, venv, and build essentials
apt-get install -y python3 python3-pip python3-venv python3-dev git curl

# Verify Python version >= 3.11
PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Detected system Python version: $PYTHON_VERSION"
python3 -c "import sys; assert sys.version_info >= (3, 11), f'Python 3.11+ required, got {sys.version}'"

# Create application directory and dedicated system user
useradd -m -s /bin/bash miniredis || true
APP_DIR="/opt/mini_redis"
mkdir -p "$APP_DIR"

# Clone or fetch the mini-redis repository
if [ ! -d "$APP_DIR/.git" ]; then
    git clone https://github.com/shivprasad08/mini_redis.git "$APP_DIR"
fi

cd "$APP_DIR"
git fetch origin
git checkout main || true

# Set up python virtual environment using Python 3.11+
python3 -m venv venv
"$APP_DIR/venv/bin/pip" install --upgrade pip
"$APP_DIR/venv/bin/pip" install "grpcio>=1.59.0" "grpcio-tools>=1.59.0"

# Compile protobuf stubs directly from checked-in .proto definitions
"$APP_DIR/venv/bin/python" -m grpc_tools.protoc \
    -I src \
    --python_out=src \
    --grpc_python_out=src \
    src/miniredis/election/proto/election.proto \
    src/miniredis/health/proto/health.proto \
    src/miniredis/mutex/proto/mutex.proto \
    src/miniredis/replication/proto/replication.proto \
    src/miniredis/time_sync/proto/time.proto

# Install package in editable mode
"$APP_DIR/venv/bin/pip" install --no-deps -e .

# Verify application package imports cleanly
"$APP_DIR/venv/bin/python" -c "import miniredis; from miniredis.server import main; print('mini-redis imported successfully')"

chown -R miniredis:miniredis "$APP_DIR"

# Assemble CLI arguments based on node role and peers
CMD_ARGS="--role ${ROLE} --node-id ${NODE_ID} --host 0.0.0.0 --port 6380 --grpc-port 6381 --write-mode ${WRITE_MODE} --mutex-algo ${MUTEX_ALGO} --election-algo ${ELECTION_ALGO}"

if [ "${ROLE}" = "primary" ]; then
    CMD_ARGS="$CMD_ARGS ${REPLICA_OF_ARGS}"
else
    CMD_ARGS="$CMD_ARGS --primary-address ${PRIMARY_ADDRESS} --primary-node-id ${PRIMARY_NODE_ID}"
fi

CMD_ARGS="$CMD_ARGS ${PEER_ARGS}"

# Create systemd service
cat <<SERVICE > /etc/systemd/system/mini-redis.service
[Unit]
Description=mini-redis distributed key-value store (${NODE_ID})
After=network.target

[Service]
Type=simple
User=miniredis
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python -m miniredis.server $CMD_ARGS
Restart=on-failure
RestartSec=3s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SERVICE

systemctl daemon-reload
systemctl enable mini-redis
systemctl restart mini-redis

echo "=== mini-redis service started successfully on ${NODE_ID} ==="

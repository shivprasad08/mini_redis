#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

echo "Generating gRPC code..."
python -m grpc_tools.protoc \
    -I src \
    --python_out=src \
    --grpc_python_out=src \
    src/miniredis/election/proto/election.proto \
    src/miniredis/health/proto/health.proto \
    src/miniredis/replication/proto/replication.proto \
    src/miniredis/time_sync/proto/time.proto

echo "Done."

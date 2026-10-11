FROM python:3.11-slim

WORKDIR /app

# Install runtime and build dependencies
COPY pyproject.toml README.md ./

# Install grpcio and grpcio-tools required for compilation and execution
RUN pip install --no-cache-dir grpcio>=1.59.0 grpcio-tools>=1.59.0

# Copy application source including .proto files
COPY src/ ./src/

# Compile gRPC protobuf definitions directly from .proto files inside the build container
RUN python -m grpc_tools.protoc \
    -I src \
    --python_out=src \
    --grpc_python_out=src \
    src/miniredis/election/proto/election.proto \
    src/miniredis/health/proto/health.proto \
    src/miniredis/mutex/proto/mutex.proto \
    src/miniredis/replication/proto/replication.proto \
    src/miniredis/time_sync/proto/time.proto

# Install mini-redis package
RUN pip install --no-cache-dir --no-deps -e .

# Expose default TCP server port and gRPC replication/election port
EXPOSE 6380 6381

# Default entrypoint runs miniredis.server with CLI arguments
ENTRYPOINT ["python", "-m", "miniredis.server"]
CMD []

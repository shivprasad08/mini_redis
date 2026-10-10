FROM python:3.11-slim

WORKDIR /app

# Copy dependency definition first for optimal layer caching
COPY pyproject.toml README.md ./

# Install runtime dependencies (grpcio, grpcio-tools)
RUN pip install --no-cache-dir grpcio>=1.59.0 grpcio-tools>=1.59.0

# Copy application source
COPY src/ ./src/

# Install mini-redis package in editable/module mode without reinstalling dependencies
RUN pip install --no-cache-dir --no-deps -e .

# Expose default TCP server port and gRPC replication port
EXPOSE 6380 6381

# Default entrypoint runs miniredis.server with arguments provided by docker-compose.yml
ENTRYPOINT ["python", "-m", "miniredis.server"]
CMD []

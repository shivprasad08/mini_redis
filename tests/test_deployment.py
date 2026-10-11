import asyncio
import os
import re
from pathlib import Path

import pytest


def test_dockerfile_structure():
    """Verify Dockerfile compiles proto files and sets up environment cleanly."""
    dockerfile_path = Path(__file__).parent.parent / "Dockerfile"
    assert dockerfile_path.exists(), "Dockerfile must exist"
    content = dockerfile_path.read_text(encoding="utf-8")

    assert "FROM python:3.11-slim" in content
    assert "grpc_tools.protoc" in content
    assert "src/miniredis/election/proto/election.proto" in content
    assert "src/miniredis/health/proto/health.proto" in content
    assert "src/miniredis/mutex/proto/mutex.proto" in content
    assert "src/miniredis/replication/proto/replication.proto" in content
    assert "src/miniredis/time_sync/proto/time.proto" in content
    assert 'ENTRYPOINT ["python", "-m", "miniredis.server"]' in content


def test_dockerignore_excludes_unwanted():
    """Verify .dockerignore keeps local stubs, virtualenvs, and secrets out of build context."""
    dockerignore_path = Path(__file__).parent.parent / ".dockerignore"
    assert dockerignore_path.exists(), ".dockerignore must exist"
    content = dockerignore_path.read_text(encoding="utf-8")

    assert ".venv/" in content
    assert ".git/" in content
    assert "*_pb2.py" in content
    assert "*_pb2_grpc.py" in content
    assert "*.log" in content


def test_docker_compose_topology_and_healthcheck():
    """Verify docker-compose.yml defines 3 nodes with correct peer addresses and non-blocking restart policies."""
    compose_path = Path(__file__).parent.parent / "docker-compose.yml"
    assert compose_path.exists(), "docker-compose.yml must exist"
    content = compose_path.read_text(encoding="utf-8")

    # Verify 3 distinct nodes exist
    assert "node-a:" in content
    assert "node-b:" in content
    assert "node-c:" in content

    # Verify healthchecks test actual application readiness using RESP PING
    assert "healthcheck:" in content
    assert "PING" in content
    assert "+PONG" in content

    # Verify flags passed and internal port bindings
    assert "--role primary" in content
    assert "--role replica" in content
    assert "--node-id node-a" in content
    assert "--node-id node-b" in content
    assert "--node-id node-c" in content

    # Peer lists use container DNS hostnames on internal port 6381/6380, never host ports
    assert "node-b:6381,node-b:6380" in content
    assert "node-c:6381,node-c:6380" in content
    assert "node-a:6381,node-a:6380" in content

    # Host published ports remain configurable
    assert "${NODE_A_PORT:-6380}:6380" in content
    assert "${NODE_B_PORT:-6381}:6380" in content
    assert "${NODE_C_PORT:-6382}:6380" in content

    # Isolated bridge network
    assert "miniredis-net:" in content


def test_terraform_aws_ubuntu_and_python():
    """Verify Terraform configuration selects Ubuntu 24.04 (Python 3.12 compatible) and valid Python checks."""
    tf_dir = Path(__file__).parent.parent / "infra" / "aws"
    assert tf_dir.exists(), "infra/aws must exist"

    main_tf = (tf_dir / "main.tf").read_text(encoding="utf-8")
    user_data = (tf_dir / "user_data.sh").read_text(encoding="utf-8")

    # Ubuntu 24.04 Noble LTS AMI for Python 3.12 compatibility
    assert "ubuntu-noble-24.04" in main_tf

    # user_data asserts Python >= 3.11 at runtime
    assert "assert sys.version_info >= (3, 11)" in user_data
    assert "grpc_tools.protoc" in user_data
    assert "mini-redis imported successfully" in user_data


def test_terraform_least_privilege_security_groups():
    """Verify security group restricts inter-node and public traffic with least privilege."""
    tf_dir = Path(__file__).parent.parent / "infra" / "aws"
    main_tf = (tf_dir / "main.tf").read_text(encoding="utf-8")
    vars_tf = (tf_dir / "variables.tf").read_text(encoding="utf-8")

    # Ingress on port 6381 (gRPC internal cluster communication) has self = true
    assert "from_port   = 6381" in main_tf
    assert "self        = true" in main_tf

    # Admin CIDR validation prevents 0.0.0.0/0
    assert 'var.admin_cidr != "0.0.0.0/0"' in vars_tf

    # Subnets mapped across 3 distinct AZs
    assert "data.aws_availability_zones.available.names" in main_tf
    assert "count                   = 3" in main_tf

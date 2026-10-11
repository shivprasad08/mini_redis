"""
Docker Compose integration test for failover and old-primary rejoin.

Runs scripts/verify_failover_recovery.py against a throwaway Compose project
(own container names and host ports), so a cluster already running from
docker-compose.yml is never touched. Builds images, so it is opt-in:

    MINIREDIS_DOCKER_TESTS=1 python -m pytest tests/test_docker_failover.py -v
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(
    os.environ.get("MINIREDIS_DOCKER_TESTS") != "1" or shutil.which("docker") is None,
    reason="set MINIREDIS_DOCKER_TESTS=1 (and have Docker running) to run Docker integration tests",
)


@pytest.mark.parametrize("election_algo", ["bully", "ring"])
def test_docker_failover_and_old_primary_rejoin(election_algo: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "verify_failover_recovery.py"),
            "--algorithms",
            election_algo,
            "--project-prefix",
            "mrtest",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    print(result.stdout)
    assert result.returncode == 0, result.stderr or result.stdout
    assert f"PASS: {election_algo}" in result.stdout
    assert "never two primaries at once" in result.stdout

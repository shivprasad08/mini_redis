"""
Verify safe failover and old-primary rejoin on a Docker Compose cluster.

For each election algorithm (bully and ring by default) the script:
  1. builds and starts a throwaway Compose project (own container names,
     host ports, network and images) from docker-compose.yml
  2. checks node-a is the single primary and node-b/node-c follow it
  3. writes data through node-a and confirms it on both replicas
  4. stops node-a and waits (bounded polling) for a majority failover to node-c
  5. writes through node-c and confirms it on node-b
  6. starts node-a again (same container, i.e. the newly built image)
  7. waits until node-a catches up: STALENESS on node-c reports every node
     IN_SYNC with the same epoch, sequence and history digest
  8. confirms node-a is still a replica and refuses writes, node-c keeps
     leadership in the same epoch, and replication to node-a resumes
  9. samples all three nodes continuously during failover and rejoin and
     fails if two nodes ever answer as primary at the same time
 10. takes a SNAPSHOT, VERIFYs every ledger, then stops and starts all three
     containers and checks the data replays from the per-node volumes
 11. removes only the containers, network, volumes and images of that project

A node counts as primary when it answers STALENESS: the command is refused
on every node whose client writes are disabled. Sampling is periodic, so
very short overlaps can go unobserved; the in-process tests in
tests/test_leader_recovery.py check this at a much finer grain.

Usage (safe, never touches an already running cluster):
  python scripts/verify_failover_recovery.py
  python scripts/verify_failover_recovery.py --algorithms ring --keep

Running against the existing cluster requires --use-existing-cluster and
will stop/start its node-a (in-memory data on the old containers is at risk).
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent
NODES = ("node-a", "node-b", "node-c")
DEFAULT_PROJECT = ROOT.name.lower().replace(" ", "")  # what plain `docker compose` uses


class VerificationError(AssertionError):
    pass


def free_ports(count: int) -> list[int]:
    sockets = []
    for _ in range(count):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    ports = [sock.getsockname()[1] for sock in sockets]
    for sock in sockets:
        sock.close()
    return ports


def existing_projects() -> set[str]:
    result = subprocess.run(
        ["docker", "compose", "ls", "-a", "--format", "json"], capture_output=True, text=True, timeout=60
    )
    if result.returncode != 0:
        raise VerificationError(f"docker compose ls failed: {result.stderr.strip()}")
    return {entry["Name"] for entry in json.loads(result.stdout or "[]")}


class Cluster:
    def __init__(self, project: Optional[str], ports: dict[str, int], election_algo: Optional[str]) -> None:
        self.project = project
        self.ports = ports
        self.env = dict(os.environ)
        if election_algo:
            self.env["ELECTION_ALGO"] = election_algo
        self.compose = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]
        if project is not None:
            # Isolation: own project (=> own network), own container names and host ports.
            override = Path(tempfile.mkdtemp(prefix="miniredis-verify-")) / "docker-compose.isolated.yml"
            override.write_text(
                "services:\n" + "".join(f"  {n}:\n    container_name: {project}-{n}\n" for n in (*NODES, "gateway")),
                encoding="utf-8",
            )
            self.compose += ["-p", project, "-f", str(override)]
            self.env.update(
                NODE_A_PORT=str(ports["node-a"]),
                NODE_B_PORT=str(ports["node-b"]),
                NODE_C_PORT=str(ports["node-c"]),
                GATEWAY_PORT=str(free_ports(1)[0]),
            )

    # ── docker ------------------------------------------------───────

    def docker(self, *args: str, timeout: float = 600, quiet: bool = False) -> str:
        cmd = [*self.compose, *args]
        if not quiet:
            print(f"$ docker compose {' '.join(args)}", flush=True)
        result = subprocess.run(cmd, cwd=ROOT, env=self.env, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            raise VerificationError(f"docker compose {' '.join(args)} failed: {result.stderr.strip()}")
        return result.stdout

    def containers(self) -> dict[str, dict]:
        out = self.docker("ps", "-a", "--format", "json", quiet=True)
        rows = [json.loads(line) for line in out.splitlines() if line.strip()]
        if len(rows) == 1 and isinstance(rows[0], list):
            rows = rows[0]
        return {row["Service"]: row for row in rows}

    def health(self) -> dict[str, str]:
        return {svc: row.get("Health") or row.get("State", "") for svc, row in self.containers().items()}

    def image_ids(self) -> dict[str, str]:
        ids = {}
        for svc, row in self.containers().items():
            result = subprocess.run(
                ["docker", "inspect", "--format", "{{.Image}}", row["Name"]], capture_output=True, text=True
            )
            ids[svc] = result.stdout.strip()
        return ids

    # ── client protocol ----------------------------------------──────

    def command(self, node: str, *args: str) -> str:
        payload = f"*{len(args)}\r\n" + "".join(f"${len(a.encode())}\r\n{a}\r\n" for a in args)
        with socket.create_connection(("127.0.0.1", self.ports[node]), timeout=3) as sock:
            sock.settimeout(3)
            sock.sendall(payload.encode())
            data = b""
            while not _complete(data):
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
        return _decode(data)

    def try_command(self, node: str, *args: str) -> Optional[str]:
        try:
            return self.command(node, *args)
        except OSError:
            return None

    def staleness(self, node: str) -> Optional[dict]:
        reply = self.try_command(node, "STALENESS")
        if reply is None or reply.startswith("-"):
            return None
        return json.loads(reply)

    def answers_as_primary(self, node: str) -> bool:
        # Any non-error reply means client writes are enabled on that node;
        # the JSON body is not needed (and may be cut short by a stopping node).
        reply = self.try_command(node, "STALENESS")
        return bool(reply) and not reply.startswith("-")

    def primaries(self) -> list[str]:
        return [n for n in NODES if self.answers_as_primary(n)]

    # ── diagnostics ------------------------------------------------──

    def diagnostics(self) -> None:
        print("\n-------- diagnostics --------", flush=True)
        try:
            for svc, row in self.containers().items():
                print(f"{svc}: {row.get('State')} {row.get('Health', '')} ({row.get('Name')})")
        except VerificationError as exc:
            print(f"(container listing failed: {exc})")
        for node in NODES:
            print(f"\n{node} PING -> {self.try_command(node, 'PING')!r}")
            print(f"{node} STALENESS -> {self.try_command(node, 'STALENESS')!r}")
        for node in NODES:
            try:
                logs = self.docker("logs", "--no-color", "--tail", "400", node, quiet=True)
            except VerificationError as exc:
                print(f"\n(logs for {node} unavailable: {exc})")
                continue
            interesting = [
                line for line in logs.splitlines()
                if "lamport" not in line and "Connection" not in line and "Time synchronization" not in line
            ]
            print(f"\n-- last log lines: {node}")
            print("\n".join(interesting[-25:]))
        print("-------- end diagnostics --------\n", flush=True)


def _complete(data: bytes) -> bool:
    if not data.endswith(b"\r\n"):
        return False
    if data[:1] == b"$" and not data.startswith(b"$-1"):
        header, _, rest = data.partition(b"\r\n")
        return len(rest) >= int(header[1:]) + 2
    return True


def _decode(data: bytes) -> str:
    if data[:1] == b"$":
        if data.startswith(b"$-1"):
            return "(nil)"
        return data.partition(b"\r\n")[2][:-2].decode(errors="replace")
    return data.decode(errors="replace").strip()


class PrimarySampler:
    """Background sampler recording any instant with more than one primary."""

    def __init__(self, cluster: Cluster, interval: float = 0.1) -> None:
        self._cluster = cluster
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.samples = 0
        self.violations: list[tuple[float, list[str]]] = []
        self.errors: list[str] = []

    def __enter__(self) -> "PrimarySampler":
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                primaries = self._cluster.primaries()
            except Exception as exc:  # never let the sampler die silently
                self.errors.append(repr(exc))
            else:
                self.samples += 1
                if len(primaries) > 1:
                    self.violations.append((time.time(), primaries))
            self._stop.wait(self._interval)


def wait_for(description: str, check: Callable[[], object], timeout: float, interval: float = 0.5):
    print(f"... waiting (<= {timeout:.0f}s): {description}", flush=True)
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        try:
            last = check()
        except (OSError, ValueError, KeyError) as exc:  # e.g. reply cut short by a stopping node
            last = exc
        if last and not isinstance(last, Exception):
            print(f"OK   {description}", flush=True)
            return last
        time.sleep(interval)
    raise VerificationError(f"timed out after {timeout:.0f}s: {description} (last observation: {last!r})")


def expect(condition: bool, description: str) -> None:
    if not condition:
        raise VerificationError(description)
    print(f"OK   {description}", flush=True)


def all_in_sync(cluster: Cluster, leader: str, expected: tuple[str, ...]) -> Optional[dict]:
    report = cluster.staleness(leader)
    if report is None:
        return None
    nodes = {entry["node_id"]: entry for entry in report["nodes"]}
    leader_entry = nodes[leader]
    for node in expected:
        entry = nodes.get(node, {})
        if (
            entry.get("status") != "IN_SYNC"
            or entry.get("epoch") != leader_entry["epoch"]
            or entry.get("last_applied_sequence") != leader_entry["last_applied_sequence"]
            or entry.get("history_digest") != leader_entry["history_digest"]
        ):
            return None
    return report


def scenario(cluster: Cluster, up: bool) -> None:
    if up:
        cluster.docker("up", "--build", "-d", timeout=1200)
    images = cluster.image_ids()
    print(f"     images: {images}")

    wait_for("all containers healthy", lambda: all(cluster.health().get(n) == "healthy" for n in NODES), 120)
    wait_for("node-a is the single primary", lambda: cluster.primaries() == ["node-a"], 60)
    gateway_port = cluster.env.get("GATEWAY_PORT")
    if gateway_port:  # M25 gateway (throwaway projects publish it on a free port)
        import urllib.request

        base = f"http://127.0.0.1:{gateway_port}"

        def gateway_status():
            with urllib.request.urlopen(base + "/status", timeout=3) as resp:
                return json.loads(resp.read())

        wait_for("gateway /status reports node-a as primary", lambda: gateway_status()["primary"] == "node-a", 60)
        put = urllib.request.Request(base + "/kv/verify:http", data=b'{"value": "ok"}', method="PUT")
        with urllib.request.urlopen(put, timeout=10) as resp:
            expect(resp.status == 200, "gateway PUT /kv/verify:http -> 200")
        with urllib.request.urlopen(base + "/kv/verify:http", timeout=10) as resp:
            expect(json.loads(resp.read())["value"] == "ok", "gateway GET returns the stored value")
    wait_for("node-b and node-c follow node-a (IN_SYNC)", lambda: all_in_sync(cluster, "node-a", ("node-b", "node-c")), 30)

    expect(cluster.command("node-a", "SET", "verify:boot", "1") == "+OK", "write accepted by node-a")
    wait_for(
        "write visible on node-b and node-c",
        lambda: all(cluster.command(n, "GET", "verify:boot") == "1" for n in ("node-b", "node-c")),
        30,
    )
    boot = wait_for("replicas IN_SYNC after the write", lambda: all_in_sync(cluster, "node-a", ("node-b", "node-c")), 30)
    boot_epoch = boot["primary_epoch"]

    with PrimarySampler(cluster) as sampler:
        cluster.docker("stop", "node-a")
        wait_for("majority failover: node-c is the single primary", lambda: cluster.primaries() == ["node-c"], 60)
        expect(cluster.command("node-c", "SET", "verify:after", "failover") == "+OK", "write accepted by node-c")
        wait_for("post-failover write on node-b", lambda: cluster.command("node-b", "GET", "verify:after") == "failover", 30)
        report = wait_for("node-b IN_SYNC with node-c", lambda: all_in_sync(cluster, "node-c", ("node-b",)), 30)
        failover_epoch = report["primary_epoch"]
        expect(failover_epoch > boot_epoch, f"failover epoch {failover_epoch} > bootstrap epoch {boot_epoch}")

        cluster.docker("start", "node-a")
        expect(cluster.image_ids().get("node-a") == images.get("node-a"), "restarted node-a runs the newly built image")
        report = wait_for(
            "node-a catches up: node-a and node-b IN_SYNC with node-c (epoch, sequence, digest)",
            lambda: all_in_sync(cluster, "node-c", ("node-a", "node-b")),
            90,
        )
        expect(cluster.primaries() == ["node-c"], "node-c is still the only primary")
        expect(report["primary_epoch"] == failover_epoch, "node-c kept leadership in the same epoch")
        expect(cluster.command("node-a", "GET", "verify:after") == "failover", "node-a has the post-failover write")
        expect(cluster.command("node-a", "GET", "verify:boot") == "1", "node-a has the pre-failover write")
        rejected = cluster.command("node-a", "SET", "verify:after", "conflict")
        expect(rejected.startswith("-"), f"node-a refuses writes ({rejected})")
        expect(cluster.command("node-c", "GET", "verify:after") == "failover", "leader data unchanged")

        expect(cluster.command("node-c", "SET", "verify:resumed", "yes") == "+OK", "new write accepted by node-c")
        wait_for(
            "replication resumes to node-a and node-b",
            lambda: all(cluster.command(n, "GET", "verify:resumed") == "yes" for n in ("node-a", "node-b")),
            30,
        )
        report = wait_for("all nodes IN_SYNC with node-c", lambda: all_in_sync(cluster, "node-c", ("node-a", "node-b")), 30)

    expect(sampler.samples >= 10 and not sampler.errors, f"primary sampler ran ({sampler.samples} samples, errors: {sampler.errors[:3]})")
    expect(
        not sampler.violations,
        f"never two primaries at once ({sampler.samples} samples; violations: {sampler.violations})",
    )
    wait_for("all containers healthy", lambda: all(cluster.health().get(n) == "healthy" for n in NODES), 60)

    print("\nFinal STALENESS report from node-c:")
    print(json.dumps(report, indent=2))

    # -- M17 / M20 ---------------------------------------------------
    snapshot = json.loads(cluster.command("node-a", "SNAPSHOT"))
    expect(
        snapshot["status"] == "COMPLETED" and snapshot["summary"]["channels_closed"] == 6,
        f"SNAPSHOT {snapshot['snapshot_id']} completed over 6 channels",
    )
    heads = set()
    for node in NODES:
        verified = json.loads(cluster.command(node, "VERIFY"))
        expect(verified["ok"], f"VERIFY on {node}: {verified['records']} records, head {verified['head_hash'][:12]}")
        heads.add(verified["head_hash"])
    expect(len(heads) == 1, "all ledgers end at the same chain head")

    cluster.docker("stop", *NODES)
    cluster.docker("start", *NODES)
    leader = wait_for(
        "one primary after a full-cluster restart",
        lambda: (p := cluster.primaries()) and len(p) == 1 and p,
        90,
    )[0]
    wait_for(
        "replayed data on every node",
        lambda: all(cluster.command(n, "GET", "verify:resumed") == "yes" for n in NODES),
        60,
    )
    expect(all(json.loads(cluster.command(n, "VERIFY"))["ok"] for n in NODES), "VERIFY ok on every node after restart")
    expect(cluster.command(leader, "SET", "verify:after-restart", "1") == "+OK", f"{leader} accepts writes after restart")
    wait_for("all containers healthy", lambda: all(cluster.health().get(n) == "healthy" for n in NODES), 60)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--algorithms", default="bully,ring", help="comma-separated: bully,ring")
    parser.add_argument("--project-prefix", default="mrverify", help="prefix for throwaway Compose projects")
    parser.add_argument("--ports", default=None, help="host ports for node-a,node-b,node-c (default: free ports)")
    parser.add_argument("--keep", action="store_true", help="leave the throwaway project running afterwards")
    parser.add_argument(
        "--use-existing-cluster",
        action="store_true",
        help="DANGER: run against the default project; stops and starts its node-a",
    )
    args = parser.parse_args()

    algorithms = [a.strip() for a in args.algorithms.split(",") if a.strip()]
    if any(a not in ("bully", "ring") for a in algorithms):
        raise SystemExit("--algorithms accepts bully and/or ring")

    if args.use_existing_cluster:
        ports = dict(zip(NODES, (int(p) for p in (args.ports or "6380,6381,6382").split(","))))
        cluster = Cluster(None, ports, None)
        try:
            scenario(cluster, up=False)
            print("\nPASS: existing cluster")
            return 0
        except VerificationError as exc:
            cluster.diagnostics()
            print(f"\nFAIL: {exc}", file=sys.stderr)
            return 1

    taken = existing_projects()
    results: dict[str, str] = {}
    for algo in algorithms:
        project = f"{args.project_prefix}-{algo}-{uuid.uuid4().hex[:6]}"
        if project in taken or project == DEFAULT_PROJECT:
            raise SystemExit(f"refusing to reuse existing Compose project {project!r}")
        port_list = [int(p) for p in args.ports.split(",")] if args.ports else free_ports(3)
        cluster = Cluster(project, dict(zip(NODES, port_list)), algo)
        print(f"\n======== {algo}: project {project}, host ports {port_list} ========", flush=True)
        created = False
        try:
            created = True
            scenario(cluster, up=True)
            results[algo] = "PASS"
        except (VerificationError, subprocess.TimeoutExpired) as exc:
            cluster.diagnostics()
            print(f"\nFAIL ({algo}): {exc}", file=sys.stderr)
            results[algo] = "FAIL"
        finally:
            if created and not args.keep:
                # Only this project's containers, network, ledger volumes and
                # locally built images (all named after the throwaway project).
                cluster.docker("down", "--remove-orphans", "--volumes", "--rmi", "local")

    print("\n======== summary ========")
    for algo, result in results.items():
        print(f"{result}: {algo}")
    if all(r == "PASS" for r in results.values()):
        print("PASS: safe failover and rejoin verified")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())

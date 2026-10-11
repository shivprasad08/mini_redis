# mini-redis

A distributed key-value store built from scratch in Python using `asyncio`, raw TCP framing for client traffic, and `gRPC` for internal node-to-node replication, failure detection, leader election, and cluster management.

---

## Features

- **Custom RESP-like Protocol**: Hand-rolled, streaming TCP wire protocol parser (no third-party protocol libraries).
- **In-Memory Store**: Supports `GET`, `SET`, `DEL`, `EXPIRE`, `EXISTS` with lazy TTL expiration.
- **Pub/Sub Engine**: Channel-based async message broadcasting (`SUBSCRIBE`, `PUBLISH`).
- **Primary-Replica Replication**: Supports both **Async (fire-and-forget)** and **Quorum** write modes.
- **Heartbeat & Failure Detection**: Automatic health monitoring over gRPC with configurable failure thresholds.
- **Leader Election**: Bully algorithm for automatic primary failover when the primary node dies.
- **Split-Brain Protection**: Cluster majority monitoring and write-admission gates to prevent inconsistent state during network partitions.
- **Consistent Hashing Sharding**: Client-side key routing across multiple primary nodes with 150 virtual nodes per physical node.
- **Docker Compose Cluster**: Package multi-node topology into containerized setup.

---

## Quickstart with Docker Compose

### 1. Build and Start Cluster

Build image and launch 4-node cluster (1 Primary `node-a` + 3 Replicas `node-b`, `node-c`, `node-d`):

```bash
docker compose up --build -d
```

Verify running containers:

```bash
docker compose ps
```

---

### 2. Connect CLI Client from Host Machine

Connect the CLI client directly to the published primary port (`6380`):

```bash
python -m miniredis.cli_client --port 6380
```

Perform KV operations on the primary:

```text
mini-redis> SET demo:key "hello-from-docker"
OK
mini-redis> GET demo:key
hello-from-docker
```

---

### 3. Demo Primary Failure & Automatic Failover (FA-2 Centerpiece)

#### Step A: Tail Logs to Watch Election Live
Open a terminal window and stream logs from remaining replica nodes:

```bash
docker compose logs -f node-b node-c node-d
```

#### Step B: Simulate Primary Failure
In a separate terminal, simulate node failure of the primary container `node-a`.

- **Graceful Stop**:
  ```bash
  docker compose stop node-a
  ```
- **Hard Kill** (simulates abrupt crash / power loss, matching `Stop-Process -Force`):
  ```bash
  docker compose kill node-a
  ```

#### Step C: Observe Failover & Promotion
In the log stream (`docker compose logs -f`), observe:
1. Replicas detect missing heartbeats: `PRIMARY DECLARED DEAD`.
2. Bully election algorithm triggers among surviving replicas.
3. Highest priority node (e.g. `node-d`) wins election and promotes itself to `PRIMARY`.

#### Step D: Verify Data Accessibility After Promotion
Connect the CLI to the promoted node's published host port (e.g. `node-b` on `6381` or `node-d` on `6383`):

```bash
python -m miniredis.cli_client --port 6381
```

Confirm data persisted across failover:

```text
mini-redis> GET demo:key
hello-from-docker
```

---

### 4. Sharded Mode (Consistent Hashing)

Connect client in sharded mode across all published node ports:

```bash
python -m miniredis.cli_client \
  --shard node-a=127.0.0.1:6380 \
  --shard node-b=127.0.0.1:6381 \
  --shard node-c=127.0.0.1:6382 \
  --shard node-d=127.0.0.1:6383
```

---

### 5. Cluster Teardown

Stop and remove containers, networks, and volumes:

```bash
docker compose down
```

---

## Manual Multi-Node Startup (Without Docker)

You can also run nodes locally in separate terminal windows:

### 1. Start Replicas

```bash
# Terminal 1 (Replica 1)
python -m miniredis.server --role replica --port 6381 --grpc-port 7381 --node-id node-b --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380

# Terminal 2 (Replica 2)
python -m miniredis.server --role replica --port 6382 --grpc-port 7382 --node-id node-c --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380
```

### 2. Start Primary

```bash
# Terminal 3 (Primary)
python -m miniredis.server --role primary --port 6380 --grpc-port 7380 --node-id node-a --replica-of 127.0.0.1:7381 --replica-of 127.0.0.1:7382 --peer node-b,127.0.0.1:7381,127.0.0.1:6381 --peer node-c,127.0.0.1:7382,127.0.0.1:6382
```

---

## Running Tests

Run the full pytest suite:

```bash
python -m pytest -q
```

---

## FA-2: Latency Benchmark, Lamport Timeline, and Clock Sync

### M21: Simulated Replication Latency

`miniredis-server` accepts:

```bash
--simulate-latency-ms 50
```

The value is a per-replication-RPC artificial one-way delay. It is applied inside the real gRPC replication path, so both async fire-and-forget replication and quorum acknowledgement collection include the delay in their timeout and latency behavior. The default is `0`.

Run the write benchmark against a running node:

```bash
python -m miniredis.benchmark --host 127.0.0.1 --port 6380 --mode async --operations 100 --value-size 32 --key-prefix fa2-async --seed 7 --output async-results.json
python -m miniredis.benchmark --host 127.0.0.1 --port 6380 --mode quorum --operations 100 --value-size 32 --key-prefix fa2-quorum --seed 7 --output quorum-results.json
```

The JSON and table report attempted, successful, and failed writes; elapsed time; throughput; mean, p50, and p99 latency; and optional post-failover verification counts.

### M13: Lamport Logical Clocks

Replication, heartbeat, election, coordinator, and time-sync gRPC messages carry Lamport timestamps. Each node logs structured lines containing `lamport_event`.

Merge logs from three nodes into a deterministic presentation timeline:

```bash
python -m miniredis.timeline nodeA.log nodeB.log nodeC.log
```

Rows are ordered by Lamport timestamp, then node id as a display-only tie breaker. Equal timestamps do not imply a causal relationship.

### M14: Cristian Clock Synchronization and TTL

`miniredis-server` accepts:

```bash
--clock-skew-ms -250
```

The skew changes only the node's simulated wall clock, not the OS clock. Replicas use the gRPC `TimeService` on the primary to estimate offset and RTT using Cristian's algorithm. TTL-bearing writes replicate a common absolute expiry deadline, so replicas do not restart a fresh TTL when the replication message arrives.

Example local replica with skew:

```bash
python -m miniredis.server --role replica --port 6381 --grpc-port 7381 --node-id node-b --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380 --clock-skew-ms 500
```

Cristian synchronization is an estimate. Network delay, jitter, and primary changes can make the offset stale; replicas mark failed sync attempts as unsynchronized rather than pretending the estimate is perfect.

---

## M15 — Replica Staleness and Divergence Detection

### What Replication Progress Means

Every mini-redis node maintains a `ReplicationProgress` tracker that records:

- **Node ID and Role** — whether the node is currently `primary` or `replica`.
- **Election Epoch** — the leadership epoch under which the current sequence counter is valid. Sequence numbers across different epochs are not directly comparable.
- **Last Applied Sequence** — the highest *contiguous* sequence number that has been successfully applied to the local store. A sequence is "applied" only when the store mutation succeeds; queued or received-but-rejected sequences are not counted.
- **History Fingerprint** — a rolling SHA-256 digest over all applied write operations (epoch, sequence, op type, key, value, TTL/deadline). Identical operation histories produce identical fingerprints.
- **Gap Information** — whether a sequence gap or rejected replication message was ever detected, and which sequence numbers were expected vs. observed.

A **sequence gap** is permanent once detected. It is never automatically cleared by reconnection or network healing. Only an explicit node replacement with a verified consistent snapshot can clear a gap.

### Election Epoch and Applied Sequence Semantics

An election epoch is monotonically increasing. Each new primary that wins an election (Bully or Ring), and the primary that bootstraps a brand-new cluster, gets a new epoch (`max(previous + 1, time_ns())`). Epoch `0` means "this process has not learned any leadership yet" (fresh or restarted node). When a new epoch starts, the per-epoch sequence counter resets to 0. The history fingerprint is **not** reset — it accumulates across epoch transitions so divergence can be detected even when two nodes both start a new epoch at sequence 0.

### History Fingerprint Purpose and Limitations

The fingerprint is a hash chain: each applied write computes `sha256(previous_digest || canonical_write)`. Because the whole history is summarized by the hex digest, a rejoining replica can adopt the leader's digest from a snapshot and keep producing identical digests for subsequent writes.

The fingerprint detects the **DIVERGENT_HISTORY** condition: two nodes that have the same epoch and same sequence number, but applied different writes along the way. This can happen after a split-brain scenario where writes were made on an isolated minority node that later reconnected.

**Limitations:**
- The fingerprint is a diagnostic tool, not a consensus or durability guarantee.
- It does not provide Byzantine fault tolerance.
- Two nodes that have genuinely identical write histories produce identical fingerprints, but the converse is not guaranteed for all possible hash collisions (SHA-256 collision resistance is assumed sufficient for distributed diagnostics).
- The Lamport timestamp is intentionally excluded from the fingerprint because it differs per node for the same write.

### The `STALENESS` Command

Run on the **primary node** to get a cluster-wide replication consistency report:

```text
STALENESS
```

The command takes no arguments. It queries every configured replica via a `GetProgress` gRPC RPC, compares each replica's progress against the primary's current snapshot, and returns a JSON bulk-string report.

**Example response:**

```json
{
  "primary_node_id": "node-a",
  "primary_epoch": 1728634891234567890,
  "primary_last_applied_sequence": 5,
  "nodes": [
    {
      "node_id": "node-a",
      "role": "primary",
      "epoch": 1728634891234567890,
      "last_applied_sequence": 5,
      "history_digest": "3b4c5d...",
      "status": "LOCAL_PRIMARY",
      "lag": 0
    },
    {
      "node_id": "node-b",
      "role": "replica",
      "epoch": 1728634891234567890,
      "last_applied_sequence": 5,
      "history_digest": "3b4c5d...",
      "status": "IN_SYNC",
      "lag": 0,
      "message": "Replica is fully synchronised."
    },
    {
      "node_id": "127.0.0.1:7382",
      "status": "UNREACHABLE",
      "message": "GetProgress RPC failed or timed out"
    }
  ]
}
```

### Status Values

| Status | Meaning |
|---|---|
| `LOCAL_PRIMARY` | This entry is the primary node itself. |
| `IN_SYNC` | Same epoch, same contiguous applied sequence, no gap, matching fingerprints. |
| `LAGGING` | Same epoch, replica is behind the primary by `lag` writes; no evidence of divergence. |
| `STALE_EPOCH` | Replica's epoch is older than the primary's. Sequences are not comparable across epochs. |
| `FUTURE_EPOCH` | Replica reports a newer epoch than the queried node — the queried node may not be the current leader. |
| `DIVERGENT_GAP` | A sequence gap has been detected on the replica; consistency cannot be assumed. |
| `DIVERGENT_HISTORY` | Same epoch and sequence, but the history fingerprints differ — writes diverged. |
| `UNREACHABLE` | The `GetProgress` RPC failed or timed out. |
| `STALE_VIEW` | The primary epoch changed while collecting responses; the report may be incoherent. |

### Protobuf Regeneration (Windows)

The replication protobuf definitions live in `src/miniredis/replication/proto/replication.proto`. Generated stubs are **not** committed. After editing the `.proto` file, regenerate stubs with:

```powershell
# From the repository root
python -m grpc_tools.protoc `
    -I src `
    --python_out=src `
    --grpc_python_out=src `
    src/miniredis/election/proto/election.proto `
    src/miniredis/health/proto/health.proto `
    src/miniredis/mutex/proto/mutex.proto `
    src/miniredis/replication/proto/replication.proto `
    src/miniredis/time_sync/proto/time.proto
```

`grpcio-tools` is declared as a project dependency and is installed by `pip install -e .` or `pip install -e ".[dev]"`.

### Unit and Integration Test Commands

Run M15 tests:

```powershell
python -m pytest -q tests/test_progress.py tests/test_staleness.py
```

Run M16 & M18 mutual exclusion and ring election tests:

```powershell
python -m pytest -q tests/test_mutex.py tests/test_mutex_integration.py tests/test_ring_election.py tests/test_election_comparison.py
```

Run replication and election regressions:

```powershell
python -m pytest -q tests/test_replication.py tests/test_election.py
```

Run safe failover / rejoin regressions (in-process three-node clusters on dynamic ports):

```powershell
python -m pytest -q tests/test_leader_recovery.py tests/test_rejoin.py
```

Run the Docker failover-and-rejoin integration test (opt-in; builds images in a throwaway Compose project):

```powershell
$env:MINIREDIS_DOCKER_TESTS = "1"; python -m pytest -v tests/test_docker_failover.py
```

Run the full suite:

```powershell
python -m pytest -q
```

Expected results:
- `test_progress.py` — 44 tests covering the `ReplicationProgress` tracker.
- `test_staleness.py` — 19 tests covering classification, RPC, and the `STALENESS` command.
- `test_mutex.py` & `test_mutex_integration.py` — 22 tests covering mutual exclusion and fencing tokens.
- `test_ring_election.py` & `test_election_comparison.py` — 6 tests covering ring election and algorithm comparison.
- `test_deployment.py` — 5 tests validating Docker and Terraform deployment configurations.
- `test_leader_recovery.py` — 26 tests covering safe bootstrap, failover, old-primary rejoin (Bully and Ring, async and quorum), epoch fencing, symmetric and asymmetric partitions, replica repair (gap, stalled lag, divergent digest), write bursts and concurrent startup.
- `test_docker_failover.py` — 2 opt-in Docker tests (skipped unless `MINIREDIS_DOCKER_TESTS=1`).
- Full suite — **269 passed, 2 skipped** (all async tests share one session event loop; see `tests/conftest.py`).

### Three-Node Demonstration

#### 1. Start Three Nodes

Open three PowerShell terminals in the repository root.

**Terminal 1 — Replica B (start before primary):**
```powershell
python -m miniredis.server --role replica --port 6381 --grpc-port 7381 --node-id node-b --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380 --peer node-c,127.0.0.1:7382,127.0.0.1:6382
```

**Terminal 2 — Replica C:**
```powershell
python -m miniredis.server --role replica --port 6382 --grpc-port 7382 --node-id node-c --primary-address 127.0.0.1:7380 --primary-node-id node-a --peer node-a,127.0.0.1:7380,127.0.0.1:6380 --peer node-b,127.0.0.1:7381,127.0.0.1:6381
```

**Terminal 3 — Primary A:**
```powershell
python -m miniredis.server --role primary --port 6380 --grpc-port 7380 --node-id node-a --replica-of 127.0.0.1:7381 --replica-of 127.0.0.1:7382 --peer node-b,127.0.0.1:7381,127.0.0.1:6381 --peer node-c,127.0.0.1:7382,127.0.0.1:6382
```

#### 2. Perform Writes and Inspect Staleness

**Terminal 4 — CLI:**
```powershell
python -m miniredis.cli_client --port 6380
```

```text
mini-redis> SET k1 v1
OK
mini-redis> SET k2 v2
OK
mini-redis> SET k3 v3
OK
mini-redis> STALENESS
```

**Expected output (all nodes synchronised):**
```json
{
  "primary_node_id": "node-a",
  "primary_epoch": ...,
  "primary_last_applied_sequence": 3,
  "nodes": [
    { "node_id": "node-a", "status": "LOCAL_PRIMARY", "lag": 0 },
    { "node_id": "node-b", "status": "IN_SYNC", "lag": 0 },
    { "node_id": "node-c", "status": "IN_SYNC", "lag": 0 }
  ]
}
```

#### 3. Simulate Replica Isolation

Stop Replica C (Ctrl+C in Terminal 2). Perform more writes:

```text
mini-redis> SET k4 v4
OK
mini-redis> SET k5 v5
OK
mini-redis> STALENESS
```

**Expected output:**
```json
{
  "primary_node_id": "node-a",
  "primary_last_applied_sequence": 5,
  "nodes": [
    { "node_id": "node-a", "status": "LOCAL_PRIMARY", "lag": 0 },
    { "node_id": "node-b", "status": "IN_SYNC", "lag": 0 },
    { "node_id": "127.0.0.1:7382", "status": "UNREACHABLE", "message": "GetProgress RPC failed or timed out" }
  ]
}
```

Replica C is `UNREACHABLE` — it does not prevent the report from being generated for node-b.

#### 4. Restart Replica C and Heal

Restart Replica C (Terminal 2 with the same command). It will reconnect. Run `STALENESS` again.

The restarted node-c first discovers the current leader and pulls a snapshot (see [Safe Restart and Leader Recovery](#safe-restart-and-leader-recovery)), so it normally reports `IN_SYNC` within a second or two. While it is still catching up — or if snapshot transfer is failing — `STALENESS` reports its real progress, for example `LAGGING` or `DIVERGENT_GAP` as below; it is never reported `IN_SYNC` before its epoch, sequence and digest match the primary.

```text
mini-redis> STALENESS
```

If node-c restarted cleanly and missed sequences were not replicated (gap):
```json
{
  "nodes": [
    { "node_id": "node-a", "status": "LOCAL_PRIMARY" },
    { "node_id": "node-b", "status": "IN_SYNC", "lag": 0 },
    { "node_id": "node-c", "status": "LAGGING", "lag": 2 }
  ]
}
```

If the replication gap is explicitly detected by the replica (non-contiguous sequence arrives):
```json
{
  "nodes": [
    { "node_id": "node-c", "status": "DIVERGENT_GAP",
      "gap_expected_sequence": 4, "gap_observed_sequence": 4 }
  ]
}
```

### What M15 Does Not Do

- **No repair by M15 itself** — M15 only reports inconsistencies. Repair is done by the leadership reconciler (see [Safe Restart and Leader Recovery](#safe-restart-and-leader-recovery)): a replica that detects a gap, a divergent digest or stalled lag re-synchronizes from a snapshot of the confirmed primary. Network reconnection alone still never clears a gap — only adopting the leader's snapshot does.
- **No log replay** — missed writes are not replayed one by one; the replica adopts the leader's full state, sequence and digest.
- **No Byzantine fault tolerance** — The history fingerprint is a diagnostic tool, not a cryptographic proof of data integrity.

---

## M16 — Distributed Mutual Exclusion

M16 introduces distributed lock management with fencing tokens to prevent split-brain writes and race conditions across distributed clients.

### Algorithms Supported

`miniredis-server` accepts:

```bash
--mutex-algo ricart-agrawala  # Default: fully distributed Ricart-Agrawala
--mutex-algo centralized      # Baseline: primary-coordinated lock manager
```

1. **Ricart-Agrawala (`ricart-agrawala`)**:
   - Fully distributed, permission-based mutual exclusion algorithm.
   - Uses Lamport logical clocks to totally order lock requests: `(timestamp, node_id)`.
   - Node requests a lock by broadcasting `LockRequest` via gRPC to all cluster peers.
   - Peers reply immediately if not requesting or holding the lock, or if their own request has a higher `(timestamp, node_id)` tuple. Otherwise, replies are deferred until release.
   - The critical section is entered once replies/grants are collected from all peers.
   - Message complexity: $2(N - 1)$ messages per critical section entry.

2. **Centralized Baseline (`centralized`)**:
   - Primary acts as coordinator for lock grants and queue management.
   - Replicas forward lock requests to the primary via gRPC.
   - Integrates with leader election: when a new primary is elected, lock ownership and state adapt dynamically.

### Fencing Tokens

To safeguard against stale clients operating past lease expiration or partitioned leaders:
- Each granted lock issues a monotonically increasing **fencing token** `(epoch, sequence)`.
- Key-value mutations validate the fencing token via the watermark mechanism.
- Stale or lower token writes are rejected with an error.

### Redis Client Commands

- `LOCK <resource_id>`: Acquire exclusive lock for `resource_id`. Returns the assigned fencing token string.
- `UNLOCK <resource_id> [token]`: Release the lock held by the caller. Returns `+OK`.
- `FENCED_SET <key> <value> <token> [EX seconds]`: Atomic set operation guarded by a fencing token. If the provided token is older than the storage watermark, the write is rejected.

```text
mini-redis> LOCK res:accounts
"0:1"
mini-redis> FENCED_SET acc:100 "active" "0:1"
OK
mini-redis> UNLOCK res:accounts 0:1
OK
```

---

## M18 — Ring-Based Leader Election

M18 implements ring-based distributed leader election (Chang-Roberts / token-ring style) alongside election performance metrics and comparison against Bully election.

### Algorithm Selection

`miniredis-server` accepts:

```bash
--election-algo bully  # Default: Bully election (Garcia-Molina)
--election-algo ring   # M18: Ring-based election
```

### How Ring Election Works

1. **Logical Ring Topology**:
   - Nodes are arranged in a deterministic circular ring ordered by node identifiers.
   - Successor and predecessor links are computed dynamically from cluster peer configurations.

2. **Election Process**:
   - When a node detects failure of the primary, it creates an election message `(epoch, candidate_list)` containing its own identifier and forwards it to its immediate successor in the ring.
   - If an immediate successor is unreachable or declared dead, the message skips over it to the next reachable successor in the ring.
   - Each receiving node appends its identifier to the candidate list and forwards the message along the ring.
   - When the election message completes a traversal of the ring back to the initiator (or collects a majority of reachable cluster votes), the candidate with the highest identifier is declared the new leader.
   - A coordinator message is circulated around the ring announcing the new primary.

3. **Fault Tolerance**:
   - Successor skip routing ensures elections succeed even in the presence of node crashes or network partitions.
   - Requires majority quorum before confirming leadership to prevent split-brain.

### Algorithm Comparison & Metrics

Both algorithms collect real-time diagnostic metrics via `ElectionMetrics`:
- `total_messages`: Count of all election-related RPC messages sent.
- `candidate_messages`: Number of election query/forwarding messages.
- `coordinator_messages`: Number of coordinator announcement messages.
- `election_duration_ms`: Wall-clock latency required to resolve leader failover.

| Property | Bully Election (`--election-algo bully`) | Ring Election (`--election-algo ring`) |
|---|---|---|
| **Message Complexity (Best Case)** | $O(N)$ when highest node initiates | $O(N)$ |
| **Message Complexity (Worst Case)** | $O(N^2)$ when lowest node initiates | $O(N)$ (typically $2N$ messages including coordinator announcement) |
| **Turnaround Time** | Fast when highest node initiates (immediate) | Proportional to ring length ($N$ hops) |
| **Topology Requirement** | Full mesh connectivity between nodes | Circular ring overlay with skip links for fault tolerance |

---

## M22 — Docker Compose Deployment

M22 packages `mini-redis` into a container image and runs a 3-node cluster using Docker Compose.

### Image Build & Stubs

The [`Dockerfile`](Dockerfile) builds a container image using `python:3.11-slim`:
- Installs `grpcio` and `grpcio-tools`.
- Copies source files and compiles all `.proto` files (`election.proto`, `health.proto`, `mutex.proto`, `replication.proto`, `time.proto`) inside the image build.
- Installs `mini-redis` package.
- Sets entrypoint to `python -m miniredis.server`.

### Cluster Architecture

The Compose setup (`docker-compose.yml`) configures:
- **`node-a`**: Bootstrap primary of a brand-new cluster (`port 6380`, `grpc-port 6381`). Replicates to `node-b` and `node-c`. `--role primary` is only a bootstrap hint — see [Safe Restart and Leader Recovery](#safe-restart-and-leader-recovery).
- **`node-b`**: Replica (`port 6380`, `grpc-port 6381`, mapped to host port `6381`).
- **`node-c`**: Replica (`port 6380`, `grpc-port 6381`, mapped to host port `6382`).
- **Network**: Private Docker bridge network `miniredis-net`. Inter-node gRPC and health traffic uses container hostnames (`node-a`, `node-b`, `node-c`).
- **Healthchecks**: Periodically checks socket connectivity with Redis `PING`.

> [!NOTE]
> Each node's M20 ledger lives on its own named volume, so data survives container recreation; `docker compose down -v` deletes it.

### Quick Start & Validation Commands

1. **Configure environment (optional)**:
   ```bash
   cp .env.example .env
   ```

2. **Build and launch cluster**:
   ```bash
   docker compose up --build -d
   ```

3. **Verify running containers and health status**:
   ```bash
   docker compose ps
   ```

4. **Perform writes on primary (`node-a` on port 6380)**:
   ```bash
   python -m miniredis.cli_client --port 6380
   mini-redis> SET alpha 100
   +OK
   mini-redis> GET alpha
   "100"
   mini-redis> STALENESS
   # Returns cluster staleness report showing all nodes IN_SYNC
   ```

5. **Verify replication on replicas (`node-b` on port 6381, `node-c` on port 6382)**:
   ```bash
   python -m miniredis.cli_client --port 6381
   mini-redis> GET alpha
   "100"
   ```

6. **Test failover by stopping the primary**:
   ```bash
   docker compose stop node-a
   ```
   - `node-b` and `node-c` detect the failure through heartbeats and trigger leader election.
   - The winning node (`node-b` or `node-c` depending on election algorithm) promotes itself to primary and enables writes.
   - Quorum write protection is maintained: if another node fails leaving an isolated minority (1 of 3), writes are refused.

7. **Restart the old primary**:
   ```bash
   docker compose start node-a
   ```
   `node-a` discovers that `node-c` leads a newer epoch, rejoins as its replica, pulls a snapshot and refuses writes. `start` reuses the existing container, so this only applies if all three containers were created from an image that includes the safe-rejoin code; containers built before it must be recreated (losing their in-memory data) first. Check from the current primary (only the primary answers `STALENESS`):
   ```bash
   python -m miniredis.cli_client --port 6382
   mini-redis> STALENESS
   # node-a and node-b: IN_SYNC, same epoch / last_applied_sequence / history_digest as node-c
   ```

8. **Automated check of steps 2–7** (bounded polling, no fixed sleeps):
   ```bash
   python scripts/verify_failover_recovery.py                     # bully and ring, throwaway projects
   python scripts/verify_failover_recovery.py --algorithms ring --keep
   ```
   Each algorithm runs in its own new Compose project (`mrverify-<algo>-<random>`) with its own container names, network, free host ports and images, so a running cluster is never touched; afterwards only that project's resources are removed. It samples all nodes during failover and rejoin and fails if two ever answer as primary, and prints container state, `STALENESS` and recent logs on any timeout. Running against the existing cluster requires the explicit `--use-existing-cluster` flag (it stops and starts `node-a`). The same check runs as an opt-in test: `MINIREDIS_DOCKER_TESTS=1 python -m pytest tests/test_docker_failover.py -v`.

9. **Tear down cluster**:
   ```bash
   docker compose down -v
   ```

### Safe Restart and Leader Recovery

Nodes keep data, epoch and replication progress in memory only, so a restarted process knows nothing about the cluster. `--role` is therefore a **bootstrap hint**, not an instruction:

1. Every node starts *joining*: role `replica`, epoch `0`, writes refused (`PING`/`GET` work, so health checks pass).
2. It asks every peer for its progress (`GetProgress`: role, epoch, sequence, digest, followed leader).
3. **Established cluster** (any reachable peer reports epoch >= 1): it follows the *confirmed* leader — a peer that itself reports `role=primary` in the newest epoch — adopts that epoch and pulls a `SyncSnapshot` (keys, TTL deadlines, fencing watermarks, sequence, digest). Replicas naming a leader is not enough: after a fast restart they may still name the restarted node.
4. **Brand-new cluster** (no peer reports leadership): only the node configured `--role primary` may bootstrap, and only once a **majority is reachable**. An isolated node never promotes itself.
5. A background reconciler keeps confirming leadership: a primary that sees a newer epoch steps down and follows; a replica re-syncs after a sequence gap, divergent digest or stalled lag; an established cluster with no confirmed primary (e.g. peers still name the restarted node as leader) starts an election through the configured algorithm (Bully or Ring), which enforces majority.

Fencing is epoch-based and reuses the existing write gate: replicas reject writes from an epoch older than the newest one they accepted (including via a coordinator announcement), the rejection carries the newer epoch back, and the stale primary fences itself immediately before stepping down. A primary never applies replicated writes. Before a node starts leading after an election it catches up from the most advanced reachable peer, so a restarted node's empty history never replaces the cluster's data.

**Why epochs are not persisted:** the data an epoch describes is in memory. A persisted epoch on a node whose store was wiped would make it look up to date. Instead a restarted node always relearns epoch *and* state from the cluster.

**Limitations:**
- **Full cluster restart:** with M20 persistence each node replays its ledger and an election picks a leader from the persisted state; with `--no-persistence` all data is lost and the cluster bootstraps again.
- If a *majority* restarts at once while the only node holding data is unreachable, that majority can bootstrap a fresh cluster. When the old node reconnects, the higher-epoch side wins and the other side's writes are discarded — there is no merge.
- **No leader leases.** In an asymmetric partition (e.g. only the `node-a`↔`node-c` link fails) the old primary keeps a majority through `node-b` while `node-c` is elected, so for up to about one reconciler interval (1 s) two nodes are write-authorized — always in *different* epochs, never the same one. The stale one fences itself on its first rejected write or the next leadership survey.
- **`async` mode:** in that window, or before a deposed primary learns of the newer epoch, it can acknowledge client writes that are never replicated; they are discarded when it rejoins.
- **`quorum` mode:** once a majority has accepted the newer epoch, a stale primary cannot commit anything (replicas reject it). It is still not Raft: a quorum write acknowledged during the election itself (after the winner's catch-up, before the replicas accept the new epoch) can be lost.
- **Sustained write load:** asynchronous writes are sent as independent RPCs and can arrive out of order, so a rejoining replica may hit gaps and re-sync repeatedly; it is guaranteed to converge once writes pause, and `STALENESS` reports `LAGGING`/`DIVERGENT_GAP` until then.
- Exactly one node must be configured `--role primary`. Two misconfigured bootstrap primaries starting together can both bootstrap; the higher epoch wins once their coordinator announcements cross.
- Bully and Ring choose leaders by node id, not by data; the pre-leadership catch-up adopts the most advanced *reachable* peer's state.

---

## M17 — Chandy–Lamport Global Snapshot

Implementation: [`src/miniredis/snapshot.py`](src/miniredis/snapshot.py) (algorithm, coordinator), [`src/miniredis/links.py`](src/miniredis/links.py) (FIFO channels), RPCs `SnapshotMarker` / `CollectSnapshot` / `ReleaseSnapshot` in [`replication.proto`](src/miniredis/replication/proto/replication.proto).

**Channels.** Every ordered pair of nodes has one directed channel (3 nodes → 6 channels). Concurrent unary gRPC calls are *not* FIFO, so each node sends to each peer through one `PeerLink`: a single sender task that sends one RPC at a time and waits for its response (stop-and-wait), stamping `sender_id`, `link_incarnation` and an increasing `link_seq`. The receiver's `LinkGuard` rejects any message whose `link_seq` is not newer than the last one accepted from that sender (e.g. a late delivery of a request the sender gave up on). `ReplicateWrite` and markers share the link, so a marker always arrives after every write sent before it.

**Algorithm.** The initiator records its local state and enqueues a marker on every outgoing link. A node receiving its first marker records its state (that channel's state is empty), sends markers and starts recording all other incoming channels; a later marker on a channel stops recording it. Recording and marker enqueueing happen in one synchronous step under the node's *commit lock* — the lock the primary holds while it sequences, sends, logs and applies a write — so the recorded state matches exactly what was sent. A node's part is complete when markers arrived on all incoming channels; the coordinator then collects every node's report.

**Recorded state per node:** keys (value + absolute expiry), role, election epoch, replication epoch / last applied sequence / history digest, ledger head, Lamport time. **Channel state:** the `ReplicateWrite` messages applied after the receiver recorded and before that channel's marker.

**Result.** `COMPLETED` only if every node completed, all n·(n-1) channels received their marker, and all nodes recorded the same leadership epoch (election traffic is not a recorded channel, so a leadership change mid-snapshot gives `FAILED`). Snapshot installs on recovering replicas are deferred while a snapshot is recording.

| Case | Behaviour |
|---|---|
| Duplicate marker on a channel | Ignored (`duplicate`), counted in `duplicate_markers`; state is recorded once per id |
| Concurrent `SNAPSHOT`s | Independent (different ids), at most 4 active per node |
| Invalid / unknown id | `-ERR invalid snapshot id` / `-ERR unknown snapshot id` |
| Node down (marker undeliverable) | Fails fast: `-ERR SNAPSHOT <id> FAILED: ...` |
| Lost marker | `FAILED` after `--snapshot-timeout-ms` (default 5000), naming the missing channel |
| Cleanup | Coordinator releases every node's state; late markers for released ids are ignored; stale state is garbage-collected |

**Demo** (3-node cluster, e.g. Docker Compose):
```bash
python -m miniredis.cli_client --port 6380
mini-redis> SET a 1
mini-redis> SNAPSHOT
# JSON: snapshot_id, status COMPLETED, participants, per-node epoch/last_applied_sequence/keys,
# 6 channels each with marker_received=true and its in-flight messages
mini-redis> SNAPSHOT GET snap-node-a-<id>
```
A consistent cut means: for every replica, *recorded state + messages on primary→replica* equals the primary's recorded state, and no replica holds a write the primary had not sent. `tests/test_snapshot.py` checks exactly this, including with in-flight writes (slow primary link, snapshot started on a replica) and with a concurrent writer.

## M20 — Persistent Hash-Chained Write Log

Implementation: [`src/miniredis/ledger.py`](src/miniredis/ledger.py) (format, chain, fsync policies, replay, verification), commit path in [`dispatcher.py`](src/miniredis/dispatcher.py), replica path and snapshot installs in [`replicator.py`](src/miniredis/replication/replicator.py), startup replay in [`node.py`](src/miniredis/node.py).

**Record** (one JSON line, `<data-dir>/ledger.jsonl`): `v`, `kind` (`op` | `checkpoint`), `epoch`, `seq`, `op` (`SET`/`DEL`/`EXPIRE`), base64 `key`/`value`, `ttl`, absolute `deadline_ms`, fencing `token`, `prev`, `hash`. `hash = sha256(canonical JSON without "hash")`; the first record links to the fixed genesis hash `sha256("miniredis-ledger:v1:genesis")`. No node-specific field is hashed, so replicas applying the same writes build byte-identical `op` records and the same chain head. A replica that catches up by snapshot appends a `checkpoint` (full state) that *adopts* the leader's chain head, so later hashes match the leader's.

**Write path.** Primary: under the commit lock, the write is replicated (async or quorum), appended (write-ahead), applied, recorded in progress; the client ack waits for the durability rule. Replica: appended and applied in one synchronous step after the epoch/sequence checks (duplicates and gaps are rejected before logging, so a retried RPC is never logged twice); the replication ack waits for durability. Replay never appends.

| `--fsync` | Written | fsync | Ack returned | Lost on process crash | Lost on OS crash / power loss |
|---|---|---|---|---|---|
| `always` (default) | in the append | in the append | after fsync | nothing acknowledged | nothing acknowledged |
| `batch` | in the append | group fsync every `--fsync-interval-ms` (2) | after the fsync covering it | nothing acknowledged | nothing acknowledged |
| `interval` | in the append (flushed to OS) | background, every `--fsync-interval-ms` (1000) | immediately | nothing acknowledged | up to one interval of acknowledged writes |

**Startup.** The ledger is locked (one process per directory), validated record by record (hash, `prev` link, version, epoch/sequence order) and replayed; absolute deadlines mean an expired key stays expired. An incomplete last line (interrupted append) is moved to `ledger.jsonl.torn-<ms>` and truncated. Any invalid *complete* record makes the node exit with code 2 and leave the file untouched (fail closed). After replay the node still joins through leader discovery; with all nodes restarted, an election picks a leader from the persisted state.

**Storage.** Local runs default to `./data/<node-id>` (gitignored); `--data-dir` overrides, `--no-persistence` disables. Docker Compose mounts one named volume per node (`node-a-data`, `node-b-data`, `node-c-data` → `/data`). On AWS the service's working directory is `/opt/mini_redis`, so ledgers live in `/opt/mini_redis/data/<node-id>` on the instance's root volume (survives service restarts and instance stop/start). `user_data.sh` was deliberately not changed, to avoid Terraform drift on the running instances.

**Verify, tamper and replay demo** (temporary directory, never real data):
```powershell
python -m miniredis.server --node-id demo --port 7390 --grpc-port 7391 --data-dir $env:TEMP\mr-demo
# second terminal
python -m miniredis.cli_client --port 7390
mini-redis> SET a 1
mini-redis> SET b 2 EX 30
mini-redis> DEL a
mini-redis> VERIFY        # {"ok": true, "records": 3, "last_sequence": 3, "head_hash": ...}
# stop the server (Ctrl+C), start it again with the same command:
mini-redis> GET b         # "2" (replayed; TTL deadline unchanged)
mini-redis> GET a         # (nil)
# tamper: edit a value field in $env:TEMP\mr-demo\ledger.jsonl, then
mini-redis> VERIFY        # -ERR VERIFY FAILED: record 2: hash does not match record content (modified record)
# restarting now exits with code 2: "Refusing to start: ledger error ..."
```

**Limitations.** Under `always`/`batch`, a write whose fsync failed is reported as failed but may still be on disk and replayed (indeterminate outcome, as with any WAL); the ledger then refuses all further writes. Ledgers grow without compaction. `VERIFY` reads the whole file. A node that recovered by snapshot keeps the checkpoint, not the individual earlier records. FIFO links send one RPC at a time per peer, which lowers replication throughput under high write rates.

---

## M19 — Distributed Deadlock Detection (Edge Chasing)

Implementation: `RicartAgrawalaManager` in [`src/miniredis/mutex/manager.py`](src/miniredis/mutex/manager.py) (`waiting_for`, `_probe_loop`, `handle_probe`, `DeadlockError`), `Probe` RPC in [`mutex.proto`](src/miniredis/mutex/proto/mutex.proto). Tests: [`tests/test_deadlock.py`](tests/test_deadlock.py).

**Wait-for graph (distributed, never assembled anywhere).** A process is a node's lock client. Node P *waits for* Q while some resource is `WANTED` at P and Q's Ricart–Agrawala reply is still missing — Q deferred it because Q holds that resource (or wants it with higher priority). Each node only knows its own outgoing edges.

**Chandy–Misra–Haas probes.** While blocked, a node sends `probe(initiator=self, probe_id)` every 0.5 s to every node it waits for. A receiver that is itself blocked forwards the probe once to every node *it* waits for, appending itself to `path`; a receiver that is not blocked drops it. Duplicates (same initiator + probe id) are dropped, paths longer than the cluster are dropped, and the seen-set is bounded (4096 entries), so every probe terminates. If a probe returns to its initiator while the initiator is still blocked, `path` is a cycle. The **highest node id in the cycle** is the victim: its pending acquisitions raise `DeadlockError` (`LOCK` replies `-ERR lock acquisition failed: deadlock detected: wait-for cycle node-b -> node-a -> node-b`), its `WANTED` state is cleaned up and deferred replies are flushed. The victim's client must then release the locks it holds; the others proceed. Only pending acquisitions are aborted, so held locks, mutual exclusion and fencing tokens are unaffected.

**Failures.** A lost probe (dead node, partition) only delays detection: a cycle is reported only after a probe actually completed it, so failures cannot fabricate a deadlock; the lock timeout still bounds every wait. **Limitations (correctness):**
- *Phantom deadlocks.* A probe reports edges as they were when it passed each node. If a wait ends (a reply arrives) while the probe is still travelling, a cycle that no longer exists can be reported and the victim's pending acquisition is aborted unnecessarily. This costs liveness for that one request (the client gets `DeadlockError` and may retry); it never violates mutual exclusion or fencing, because only a *pending* acquisition is aborted and held locks are untouched.
- *Node granularity.* Wait-for edges are between nodes, not individual clients: two clients on the same node share one identity, and a victim aborts all of its node's pending acquisitions.
- *Detection, not prevention.* Deadlock is resolved only after a probe completes the cycle (probe interval 0.5 s); if probes are lost, the lock timeout still bounds the wait.
- *Centralized manager not covered.* With `--mutex-algo centralized` all lock state lives on the primary, so no probes are sent; a cycle there ends only by the lock timeout.

## M24 — Smart Distributed Client

Implementation: [`src/miniredis/smart_client.py`](src/miniredis/smart_client.py), discovery via the new `ROLE` command (JSON: `node_id`, `role`, `epoch`, `primary`, `last_applied_sequence`, `accepts_writes`). Tests: [`tests/test_smart_client.py`](tests/test_smart_client.py).

The client asks every configured node for `ROLE`, routes all commands to the node that reports `role=primary` with writes enabled and the highest epoch, and caches it. On failure it rediscovers and retries up to `max_attempts` with exponential backoff:
- **Reads** (`GET`) are retried after any failure.
- **Writes** (`SET`/`DEL`) are retried only when they definitely did not apply: connection refused, or the server answered *not primary*, *minority partition*, `READONLY`, or *quorum not reached, write not applied*. If a write was sent but no reply arrived, the server has no request-id deduplication, so the client raises `UncertainWriteError` and does **not** re-send.

Demo (writes continue across failover):
```powershell
docker compose up --build -d
python scripts/smart_client_demo.py          # terminal 1
docker compose stop node-a                   # terminal 2, while it runs
```
Writes pause for ~3–5 s during the election, then continue on `node-c`.

## M25 — HTTP Gateway and Dashboard

Implementation: [`src/miniredis/gateway.py`](src/miniredis/gateway.py) (asyncio only, no new dependency), built on the smart client; it never touches storage, replication or elections directly. Tests: [`tests/test_gateway.py`](tests/test_gateway.py).

| Endpoint | Success | Errors |
|---|---|---|
| `GET /kv/{key}` | 200 `{"key","value"}` | 404 missing key, 400 invalid key, 503 no primary |
| `PUT /kv/{key}` body `{"value": "<string>", "ttl": <optional positive int>}` | 200 `{"key","stored":true,"primary"}` | 400 invalid JSON/fields, 411 no Content-Length, 413 > 1 MiB, 503 no primary, 504 outcome unknown |
| `GET /status` | 200 primary, epoch, per-node role / health / epoch / applied sequence / lag | 503 (same JSON) when no primary |
| `GET /` | dashboard (refreshes `/status` every 2 s, logs primary changes, marks down nodes) | |

Keys are URL-decoded, 1–512 printable characters, no `/`. Run it:
```powershell
docker compose up --build -d          # includes the gateway service on port 8080
# or locally against any cluster:
python -m miniredis.gateway --nodes 127.0.0.1:6380,127.0.0.1:6381,127.0.0.1:6382 --port 8080
curl -X PUT http://127.0.0.1:8080/kv/hello -H "Content-Type: application/json" -d '{\"value\": \"world\"}'
curl http://127.0.0.1:8080/kv/hello
curl http://127.0.0.1:8080/status
```
### Control Center (dashboard)

`docker compose up --build -d`, then open **http://127.0.0.1:8080/** (laptop, tablet or phone). The page is [`src/miniredis/dashboard.html`](src/miniredis/dashboard.html), plain HTML/CSS/JS served by the gateway; everything it shows comes from real endpoints:

- **Cluster** — the three nodes from `/status` (refreshed every 2 s): role (★ PRIMARY / REPLICA / ✖ DOWN, not colour alone), epoch, applied sequence, lag, and a log of primary changes.
- **Key-Value** — PUT / GET a key through `/kv/{key}`; **Verify ledgers** calls `GET /verify` (M20 `VERIFY` on every node); **Take snapshot** calls `POST /snapshot` (a real M17 Chandy–Lamport snapshot; read-only).
- **Concepts** — one card per module (M13–M21, M24, M25): concept, mechanism, source files, what to demonstrate, limitations. Explanations only; the named tests are the executable proof.
- **Presentation** — 7 guided steps with Back/Next: architecture → write/read → replication → mutual exclusion & deadlock (`python -m pytest -v tests/test_deadlock.py`) → failover → dashboard follows the new leader → persistence & async/quorum.

The page never stops nodes or deletes data. For the failover step run `docker compose stop <current primary>` yourself (optionally with `python scripts/smart_client_demo.py` running); within a few seconds that node shows ✖ DOWN and another becomes ★ PRIMARY. `docker compose start <node>` brings it back as a replica. **Limitations:** one request per connection, no TLS or authentication, values are UTF-8 strings.

---

## M23 — AWS Multi-AZ EC2 Deployment (Terraform)

M23 automates the provisioning of a 3-node cluster across three Availability Zones in AWS using Terraform.

### Architecture

```text
               VPC (10.0.0.0/16)
  ┌─────────────────┬─────────────────┬─────────────────┐
  │  Subnet 1 (AZ-a)│  Subnet 2 (AZ-b)│  Subnet 3 (AZ-c)│
  │    10.0.1.0/24  │    10.0.2.0/24  │    10.0.3.0/24  │
  │                 │                 │                 │
  │   [ node-1 ]    │   [ node-2 ]    │   [ node-3 ]    │
  │   10.0.1.10     │   10.0.2.10     │   10.0.3.10     │
  │ (Initial Primary│   (Replica)     │   (Replica)     │
  └─────────────────┴─────────────────┴─────────────────┘
```

- **VPC & Subnets**: 1 custom VPC with 3 public subnets distributed across 3 distinct Availability Zones (`us-east-1a`, `us-east-1b`, `us-east-1c`).
- **Security Groups (Least Privilege)**:
  - Internal gRPC cluster traffic (`port 6381`) strictly allowed between cluster nodes (`self = true`).
  - Internal Redis TCP traffic (`port 6380`) allowed between cluster nodes (`self = true`).
  - Redis TCP (`6380`) client access restricted to the administrator/client CIDR (`var.client_cidr` or `var.admin_cidr`).
  - Administrative SSH (`22`) restricted strictly to the administrator CIDR block (`var.admin_cidr`). Wildcard `0.0.0.0/0` is actively rejected.
- **EC2 Instances & Cloud-Init**:
  - Deploys 3 **Ubuntu 24.04 LTS (Noble)** instances with static private IPs (`10.0.1.10`, `10.0.2.10`, `10.0.3.10`), which provides native **Python 3.12** matching the project's `>=3.11` runtime requirements.
  - `user_data.sh` verifies Python version, installs dependencies, compiles all 5 protobuf modules (`election`, `health`, `mutex`, `replication`, `time_sync`), verifies clean package imports, configures systemd service `mini-redis.service`, and starts the server with appropriate peer arguments.
- **Failover & High Availability**:
  - If any AZ or instance fails, the remaining nodes detect the primary loss via heartbeats, hold an election, and continue serving writes as long as a majority (2/3) exists.

### Prerequisites & Windows Setup

1. **Install Terraform on Windows**:
   ```powershell
   winget install Hashicorp.Terraform
   # Verify
   terraform -version
   ```

2. **Install AWS CLI on Windows**:
   ```powershell
   winget install Amazon.AWSCLI
   # Configure credentials
   aws configure
   ```

### Deployment Steps

1. **Navigate to Terraform directory**:
   ```bash
   cd infra/aws
   ```

2. **Configure variables**:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars:
   # Set admin_cidr to your IP (e.g. "203.0.113.10/32")
   # Set ssh_public_key if you wish to SSH into instances
   ```

3. **Initialize and validate Terraform**:
   ```bash
   terraform init
   terraform fmt -check
   terraform validate
   ```

4. **Review execution plan**:
   ```bash
   terraform plan
   ```

5. **Deploy the cluster (requires user authorization)**:
   ```bash
   terraform apply
   ```

6. **Retrieve cluster details**:
   ```bash
   terraform output
   ```

7. **Test connectivity and cluster state**:
   ```bash
   # Connect to node-1 using the public IP from terraform output
   python -m miniredis.cli_client --host <node-1-public-ip> --port 6380
   mini-redis> SET k1 v1
   +OK
   mini-redis> STALENESS
   ```

8. **Simulate AZ / Primary failover**:
   ```bash
   # Stop instance node-1 via AWS CLI or Console
   aws ec2 stop-instances --instance-ids <node-1-instance-id>
   ```
   Connect to `node-2` or `node-3` to observe leadership takeover and continued write availability.

9. **Teardown & Clean Up**:
   ```bash
   terraform destroy -auto-approve
   ```

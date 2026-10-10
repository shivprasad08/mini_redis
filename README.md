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

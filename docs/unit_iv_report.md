# Unit IV Deliverables: Final System Report & Theoretical Comparisons

---

## Section 1 — Design Comparison to Real-World Systems

### Design Choices Summary Table

| System Aspect | `mini-redis` Implementation | Real-World Production Parallel | Key Contrast & Rationale |
| :--- | :--- | :--- | :--- |
| **Client Wire Surface** | Hand-rolled streaming RESP-like TCP parser (`protocol.py`). | Redis (RESP2 / RESP3 protocol). | Demonstrates low-level socket framing without external protocol libraries; decouples client surface from internal RPCs. |
| **Internal Node RPC** | `gRPC` over HTTP/2 for replication, health, and election. | Redis Cluster Bus (custom binary gossip) / `etcd` (gRPC). | Pragmatic separation: custom wire protocol for client interaction vs. strongly-typed, schema-enforced RPC for internal node plumbing. |
| **Leader Election** | Bully Election Algorithm (node-ID priority ordering). | Raft (`etcd`, CockroachDB) / Redis Epoch Gossip Voting. | Bully algorithm chosen for implementation simplicity and deterministic priority under academic project constraints. |
| **Data Consistency** | Synchronous Majority Quorum gating (`majority_count()`) + Vector Clocks. | AWS DynamoDB ($W + R > N$ Quorums) & Cassandra. | Direct parallel to DynamoDB's configurable write quorums; prevents split-brain and uncommitted state divergence. |
| **Key Sharding** | Client-side Consistent Hash Ring with 150 virtual nodes per physical node. | Redis Cluster (16,384 fixed hash slots) / Cloudflare Edge. | Client-side routing with MD5 deterministic hashing provides minimal key disruption without proxy gateway overhead. |

---

### Architectural Prose & System Comparisons

Our design choices in `mini-redis` represent a deliberate hybrid architecture tailored for distributed systems education. For the client-facing network surface, we implemented a custom, hand-rolled RESP-like text protocol over raw TCP (`src/miniredis/protocol.py`), avoiding third-party protocol abstractions to fulfill the core requirement of socket-level protocol parsing. Conversely, for internal node-to-node communication—including primary-to-replica write replication (Module 6), heartbeat health checks (Module 9), and Bully leader election (Module 11)—we adopted `gRPC` (`grpcio`). This explicit split isolates client wire parsing from node coordination, using schema-driven RPCs for reliable internal control planes. Leader election is driven by the Bully algorithm, leveraging deterministic node-ID priority ranking for algorithmic clarity. To guarantee safety during network anomalies, both write admission (Module 8) and election eligibility (Module 11) share a single majority evaluation primitive (`majority_count()`), enforcing strict majority quorums ($>\lfloor N/2 \rfloor$) to prevent split-brain states.

When contrasted with production distributed systems, these design decisions reveal clear trade-offs:
* **Redis Cluster**: Production Redis Cluster relies on 16,384 fixed hash slots for data sharding and a binary gossip protocol over a dedicated cluster port for failure detection and failover. Rather than using Raft or Bully algorithms, Redis Cluster uses epoch-based vote negotiation among master nodes. In contrast, `mini-redis` implements client-side consistent hashing using an MD5-based ring with 150 virtual nodes per physical node (`src/miniredis/sharding/hashring.py`), offering smooth key distribution without cluster-wide slot migration state machines.
* **Cloudflare Edge**: Edge computing architectures at Cloudflare prioritize high-availability and low latency through eventual consistency across global edge points, reserving strong consensus engines like `etcd` (Raft) exclusively for centralized metadata coordination.
* **AWS DynamoDB**: The closest real-world parallel to `mini-redis` is AWS DynamoDB. DynamoDB relies on configurable read/write quorums ($W + R > N$) alongside vector clocks to detect concurrent write conflicts. Our Module 7 vector clock sequence tracking combined with Module 8 majority-quorum write gating mirrors DynamoDB's core consistency thesis: writes are only acknowledged to clients once a quorum of replicas confirms storage, ensuring durability across node crashes.

### Deliberate Simplifications & Academic Scope

To maintain a tractable project scope, `mini-redis` intentionally simplifies several production features. Cluster membership is fixed at startup rather than supporting live dynamic resharding or automatic slot rebalancing. Furthermore, node communication assumes crash-fault-tolerant (CFT) operation without TLS encryption, authentication, or chaos-tested Byzantine fault recovery. These trade-offs are deliberate: they eliminate operational infrastructure overhead while preserving full fidelity in demonstrating fundamental distributed systems concepts—specifically streaming binary-safe protocol parsing, consensus-driven leader failover, vector clock causality, and split-brain prevention.

---

## Section 2 — Connection to Blockchain / Distributed Ledger Consensus

### Conceptual Parallel to Quorum Agreement

The core consensus mechanism in `mini-redis` relies on majority-quorum validation (`majority_count()`) for both write admission (Module 8) and leader election (Module 11). Conceptually, this shares the fundamental principle underlying blockchain and distributed ledger consensus: state transitions are valid only when agreed upon by a strict majority of network participants. In `mini-redis`, a primary node cannot commit a quorum-mode write or claim leadership in isolation; it requires explicit acknowledgment from more than half of the cluster. This prevents a minority partition from unilaterally executing conflicting state changes, mirroring how blockchain networks prevent double-spending and invalid block commits by requiring majority agreement across validator nodes.

### Comparison to Practical Byzantine Fault Tolerance (PBFT)

A key theoretical benchmark in distributed consensus is Practical Byzantine Fault Tolerance (PBFT). While `mini-redis` enforces a simple majority threshold ($>1/2$), PBFT requires a supermajority threshold of more than two-thirds ($>2/3$, or $N \ge 3f + 1$ to tolerate $f$ faults). This difference stems from the underlying fault model:
* `mini-redis` operates under a **Crash-Fault-Tolerant (CFT)** model, assuming nodes are either operational or offline, but never malicious.
* PBFT operates under a **Byzantine-Fault-Tolerant (BFT)** model, designed to maintain consensus even when nodes actively transmit corrupt, conflicting, or fraudulent messages (such as sending different votes to different peers). 

Because `mini-redis` guarantees honest node behavior, a simple majority ($>50\%$) suffices to guarantee an intersection of at least one healthy node between any two quorums.

### Contrast with Nakamoto Consensus

In contrast to both PBFT and `mini-redis`, Bitcoin's Nakamoto consensus abandons explicit RPC vote counting and fixed participant registries. Instead, it utilizes Proof-of-Work (PoW) as a probabilistic mechanism for majority agreement, where the longest (or highest difficulty) chain represents the consensus of the majority of computational hash power. While `mini-redis` uses deterministic gRPC voting rounds among known static nodes, Nakamoto consensus achieves permissionless agreement across unknown, dynamic participants. Despite their structural differences, both mechanisms serve the identical fundamental objective: preventing an uncoordinated or minority subset of nodes from altering canonical state.

### Scope Distinction

`mini-redis` deliberately omits Byzantine fault tolerance, as crash-fault tolerance is fully sufficient to illustrate core distributed database concepts such as failover, vector clocks, and quorum gating. Highlighting the distinctions between CFT, PBFT, and Nakamoto consensus contextualizes `mini-redis` within the broader taxonomy of distributed agreement protocols, demonstrating how quorum thresholds adapt to varying threat models and network assumptions.

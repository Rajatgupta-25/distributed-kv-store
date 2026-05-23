# Distributed Key-Value Store

A distributed KV store built in Java + Spring Boot, implementing **CP (Consistency + Partition Tolerance)** semantics across a 3-node cluster. Built as a take-home assessment demonstrating distributed systems design.

---

## Architecture Summary

| Concern | Solution |
|---|---|
| Replication | Full replication — all 3 nodes store all data |
| Coordinator election | `hash(key) % 3` — deterministic, no central registry |
| Write consistency | Quorum writes (2/3 acks required before returning success) |
| Read consistency | Quorum reads (2 nodes must agree, highest timestamp wins) |
| Delete consistency | Tombstone-based deletion — propagates via quorum like a write |
| Crash recovery | WAL (Write-Ahead Log) + periodic snapshots |
| Node rejoin | WAL offset sync from most up-to-date peer |
| Temporary node failure | Hinted handoff — writes queued and delivered on recovery |
| Slow node | Replication timeout (500ms) + hinted handoff |
| Idempotency | Client-supplied `X-Write-Id` (UUID) deduplicates retries |
| CAP choice | CP — rejects writes/reads rather than risk inconsistency |

---

## Project Structure

```
src/main/java/com/kvstore/
├── KvStoreApplication.java          # Spring Boot entry point (@EnableScheduling)
├── config/
│   ├── KvStoreProperties.java       # Typed config (node IDs, ports, timeouts)
│   └── AppConfig.java               # RestTemplate with per-node timeout
├── model/
│   ├── VersionedValue.java          # Value + WAL offset timestamp (quorum comparison)
│   ├── WalEntry.java                # Single WAL log entry (PUT or DELETE)
│   └── HintedWrite.java             # Pending write for a down node
├── store/
│   └── InMemoryStore.java           # ConcurrentHashMap — the actual in-memory data
├── wal/
│   └── WalManager.java              # Append-only WAL file with fsync before ACK
├── snapshot/
│   └── SnapshotManager.java         # Periodic snapshots + WAL truncation
├── replication/
│   ├── ReplicationClient.java       # HTTP calls to peer nodes (RestTemplate)
│   ├── HintedHandoffStore.java      # In-memory queue of writes for down nodes
│   └── HealthMonitor.java           # Heartbeat scheduler + hint drain on recovery
├── service/
│   ├── KvService.java               # Core logic: put/get/delete/quorum/coordinator
│   └── RecoveryService.java         # Startup: snapshot → WAL replay → peer sync
├── controller/
│   ├── KvController.java            # Public API: GET/PUT/DELETE /kv/{key}
│   ├── InternalController.java      # Internal API: replication, WAL sync
│   └── DebugController.java         # Debug API: simulate slow nodes
└── filter/
    └── SlowNodeFilter.java          # Injects artificial delay (for testing)
```

---

## CAP Theorem Choice: CP

We choose **Consistency + Partition Tolerance** over Availability.

- If quorum (2/3 nodes) is not reachable → system returns `503` rather than risk stale or divergent data
- A network partition where only 1 node is reachable → writes and reads are rejected
- This means temporary unavailability is possible, but data is **never incorrect**

---

## Key Design Decisions

### Coordinator Election
Every write for a key is routed through a single coordinator: `hash(key) % 3 → node index`. All nodes run the same formula so they agree without communication. If the coordinator is down, the receiving node tries fallback nodes in order (node1 → node2 → node3).

### Quorum Writes (2/3)
The coordinator writes to its own WAL + memory, then replicates to peers. Once 2/3 nodes acknowledge, success is returned to the client. If a peer is unreachable, a **hinted handoff** is stored and delivered when the peer recovers.

### Quorum Reads with Read Repair
Reads ask 2 nodes and return the value with the highest WAL offset timestamp. If one node has stale data, it is updated in-place (read repair) — keeping replicas converging without a separate background process.

### Tombstone Deletion
Deletion writes a tombstone (`null` value + current WAL offset as timestamp) rather than physically removing the key. This ensures:
- Peers receive the deletion via replication — they don't just "miss" the key
- A lagging node with a stale value cannot resurrect a deleted key (tombstone timestamp > stale value timestamp → tombstone wins)
- WAL replay on restart correctly re-applies the deletion

### WAL + Snapshot Persistence
- Every write/delete is appended to `wal.log` and **fsynced to disk before ACK**
- Snapshots taken every 5 minutes (configurable) — serialize full in-memory state
- On restart: load snapshot → replay only WAL entries after snapshot offset
- WAL truncated after each successful snapshot (bounded disk usage)
- On node rejoin: pull missed WAL entries from the most up-to-date peer

### Hinted Handoff
If a peer is down or slow (times out) during a write, the write is stored as a hint. When the peer recovers (detected by heartbeat every 5 seconds), the hint queue is drained automatically.

---

## Docker

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Start the cluster with Docker Compose

```bash
# From the distributed-kv-store/ directory

# Build the image and start all 3 nodes
docker compose up --build

# Or run in background (detached)
docker compose up --build -d
```

All 3 nodes start on the same Docker bridge network (`kvstore-net`). Container names (`node1`, `node2`, `node3`) act as hostnames — Docker DNS resolves them automatically so nodes can reach each other.

### Check cluster health

```bash
docker compose ps

# Or check individual node health
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/actuator/health
```

### Use the cluster (same curl commands as local)

```bash
# Write
curl -X PUT http://localhost:8081/kv/user:1 \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Alice"}}'

# Read from any node
curl http://localhost:8082/kv/user:1

# Delete
curl -X DELETE http://localhost:8083/kv/user:1
```

### Simulate node failure in Docker

```bash
# Stop node2 (simulates crash)
docker compose stop node2

# Write still works — node1 + node3 = quorum
curl -X PUT http://localhost:8081/kv/user:2 \
  -H "Content-Type: application/json" \
  -d '{"value": "Bob"}'

# Restart node2 — it recovers from its volume and syncs missed writes
docker compose start node2
```

### View logs

```bash
# All nodes
docker compose logs -f

# Single node
docker compose logs -f node1
```

### Stop the cluster

```bash
# Stop containers (data volumes preserved)
docker compose down

# Stop and delete all data volumes (full reset)
docker compose down -v
```

### Where is data stored in Docker?

Data is stored in named Docker volumes (not inside the container):

```
node1-data  →  /data/wal.log, /data/snapshot.json  (inside node1 container)
node2-data  →  /data/wal.log, /data/snapshot.json  (inside node2 container)
node3-data  →  /data/wal.log, /data/snapshot.json  (inside node3 container)
```

Volumes persist across `docker compose down` and `docker compose up` — data survives container restarts. Only `docker compose down -v` deletes them.

To inspect a volume:
```bash
docker run --rm -v distributed-kv-store_node1-data:/data alpine ls -la /data
docker run --rm -v distributed-kv-store_node1-data:/data alpine cat /data/wal.log
```

---

## Build

```bash
# From the project root (distributed-kv-store/ directory)
mvn clean package -DskipTests

# Or if Maven is not installed globally:
/tmp/apache-maven-3.9.6/bin/mvn clean package -DskipTests -f distributed-kv-store/pom.xml
```

---

## Running the Cluster (Local — without Docker)

Open **3 separate terminals** and run one command per terminal.

```bash
# Terminal 1 — Node 1
java -jar target/distributed-kv-store-1.0.0.jar \
  --server.port=8081 \
  --kvstore.node-id=node1 \
  --kvstore.data-dir=/tmp/kvdata/node1 \
  --kvstore.all-nodes=node1|localhost:8081,node2|localhost:8082,node3|localhost:8083

# Terminal 2 — Node 2
java -jar target/distributed-kv-store-1.0.0.jar \
  --server.port=8082 \
  --kvstore.node-id=node2 \
  --kvstore.data-dir=/tmp/kvdata/node2 \
  --kvstore.all-nodes=node1|localhost:8081,node2|localhost:8082,node3|localhost:8083

# Terminal 3 — Node 3
java -jar target/distributed-kv-store-1.0.0.jar \
  --server.port=8083 \
  --kvstore.node-id=node3 \
  --kvstore.data-dir=/tmp/kvdata/node3 \
  --kvstore.all-nodes=node1|localhost:8081,node2|localhost:8082,node3|localhost:8083
```

> The `nodeId|localhost:port` format tells each node to use `localhost` as the host for peer communication. In Docker, this is not needed — container names resolve automatically.

Wait for `Started KvStoreApplication` in each terminal before sending requests.

> **Port conflict?** Run:
> ```bash
> lsof -ti :8081 -ti :8082 -ti :8083 | xargs kill -9 2>/dev/null
> ```

---

## API Reference

### Write a value
```bash
curl -X PUT http://localhost:8081/kv/user:1 \
  -H "Content-Type: application/json" \
  -d '{"value": {"name": "Alice", "email": "alice@example.com"}}'

# Response: { "key": "user:1", "writeId": "...", "status": "written" }
```

### Read a value
```bash
curl http://localhost:8081/kv/user:1

# Can read from any node — all nodes have the same data
curl http://localhost:8082/kv/user:1
curl http://localhost:8083/kv/user:1

# Response: { "key": "user:1", "value": { "name": "Alice", ... } }
# 404 if key not found, 503 if quorum not reachable
```

### Delete a value
```bash
curl -X DELETE http://localhost:8081/kv/user:1

# Response: { "key": "user:1", "writeId": "...", "status": "deleted" }
# 404 if key doesn't exist, 503 if quorum not reachable
```

### Idempotent write (safe to retry)
```bash
curl -X PUT http://localhost:8081/kv/user:1 \
  -H "Content-Type: application/json" \
  -H "X-Write-Id: my-unique-write-id-123" \
  -d '{"value": "Alice"}'

# Retrying with the same X-Write-Id returns success without re-applying the write
```

### Idempotent delete (safe to retry)
```bash
curl -X DELETE http://localhost:8081/kv/user:1 \
  -H "X-Write-Id: my-unique-delete-id-456"

# Retrying with the same X-Write-Id returns success without re-applying the delete
```

---

## Testing Failure Scenarios

### Fault tolerance — 1 node down
```bash
# Stop Terminal 2 (Ctrl+C to kill node2)

# Write still succeeds — node1 + node3 = quorum (2/3)
curl -X PUT http://localhost:8081/kv/user:2 \
  -H "Content-Type: application/json" \
  -d '{"value": "Bob"}'
# Returns 200 ✓

# Read still works
curl http://localhost:8081/kv/user:2
# Returns Bob ✓
```

### Quorum failure — 2 nodes down
```bash
# Stop Terminal 2 and Terminal 3

curl -X PUT http://localhost:8081/kv/user:3 \
  -H "Content-Type: application/json" \
  -d '{"value": "Charlie"}'
# Returns 503 — only 1/3 nodes reachable, below quorum (CP behavior) ✓
```

### Slow node simulation
```bash
# Make node2 respond slowly (3 second delay)
curl -X POST "http://localhost:8082/debug/slow?ms=3000"

# Write via node1 — succeeds because node2 times out after 500ms
# node1 + node3 = quorum, hint stored for node2
curl -X PUT http://localhost:8081/kv/foo \
  -H "Content-Type: application/json" \
  -d '{"value": "Alice"}'
# Returns 200 ✓

# Check node1 logs:
# [KvService] Peer node2 unreachable, stored hint for key=foo

# Reset node2 to normal speed
curl -X POST "http://localhost:8082/debug/slow?ms=0"

# After ~5 seconds, heartbeat fires and drains the hint to node2
# Check node1 logs:
# [HealthMonitor] Node node2 recovered, draining hint queue
# [HealthMonitor] Drained 1 hints to node=node2
```

### Delete lifecycle
```bash
# Write
curl -X PUT http://localhost:8081/kv/user:1 \
  -H "Content-Type: application/json" \
  -d '{"value": "Alice"}'

# Read — returns Alice
curl http://localhost:8081/kv/user:1

# Delete
curl -X DELETE http://localhost:8081/kv/user:1

# Read again — returns 404 (tombstone wins)
curl http://localhost:8081/kv/user:1
```

---

## Persistence — Where Are the Files?

WAL and snapshot files are stored under `--kvstore.data-dir` per node:

```
/tmp/kvdata/
├── node1/
│   ├── wal.log        ← append-only log of every write/delete operation
│   └── snapshot.json  ← periodic full state dump (appears after first snapshot)
├── node2/
│   ├── wal.log
│   └── snapshot.json
└── node3/
    ├── wal.log
    └── snapshot.json
```

**wal.log** — one JSON line per operation:
```json
{"offset":1,"operation":"PUT","key":"user:1","value":{"name":"Alice"},"writeId":"abc-123"}
{"offset":2,"operation":"DELETE","key":"user:1","value":null,"writeId":"def-456"}
```

**snapshot.json** — full state at a checkpoint:
```json
{
  "walOffset": 1000,
  "data": {
    "user:2": {"value": "Bob", "timestamp": 5}
  }
}
```

> Snapshot appears after the first snapshot trigger (every 5 minutes by default, or when WAL hits 1 million entries).

---

## Running Tests

```bash
mvn test

# Expected output:
# Tests run: 22, Failures: 0, Errors: 0, Skipped: 0
```

Test classes:
- `KvServiceTest` — core put/get/quorum/idempotency/read-repair logic (11 tests)
- `SlowNodeTest` — slow node timeout + hinted handoff behavior (4 tests)
- `DeleteTest` — tombstone deletion, propagation, idempotency (7 tests)

---

## Configuration Reference

All settings can be overridden via `--` args or environment variables:

| Property | Default | Description |
|---|---|---|
| `server.port` | `8081` | HTTP port for this node |
| `kvstore.node-id` | `node1` | Unique identifier for this node |
| `kvstore.all-nodes` | `node1:8081,node2:8082,node3:8083` | All cluster nodes |
| `kvstore.data-dir` | `./data` | Directory for WAL and snapshot files |
| `kvstore.snapshot-interval-ms` | `300000` | Snapshot frequency (5 minutes) |
| `kvstore.wal-max-entries` | `1000000` | WAL entries before forcing a snapshot |
| `kvstore.replication-timeout-ms` | `500` | Per-node replication timeout |
| `kvstore.hinted-handoff-interval-ms` | `5000` | Heartbeat + hint drain frequency |

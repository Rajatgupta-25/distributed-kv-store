# Distributed Key-Value Store

A distributed KV store built in Java + Spring Boot, implementing **CP (Consistency + Partition Tolerance)** semantics across a 3-node cluster.

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

I choose **Consistency + Partition Tolerance** over Availability.

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

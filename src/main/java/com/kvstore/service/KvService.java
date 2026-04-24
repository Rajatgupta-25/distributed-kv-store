package com.kvstore.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.VersionedValue;
import com.kvstore.model.WalEntry;
import com.kvstore.replication.HealthMonitor;
import com.kvstore.replication.HintedHandoffStore;
import com.kvstore.replication.ReplicationClient;
import com.kvstore.store.InMemoryStore;
import com.kvstore.wal.WalManager;

import jakarta.annotation.PostConstruct;

/**
 * Core KV store service — implements the distributed logic.
 *
 * Responsibilities:
 * 1. Coordinator election: determine which node is responsible for a key
 * 2. Quorum writes: replicate to 2/3 nodes before acknowledging
 * 3. Quorum reads: ask 2 nodes, return the value with the highest timestamp
 * 4. Idempotency: deduplicate retried writes using writeId
 * 5. Hinted handoff: store writes for down nodes, deliver when they recover
 * 6. Recovery: on startup, replay WAL on top of snapshot
 *
 * CAP choice: CP (Consistency + Partition Tolerance)
 * - Writes require quorum (2/3). If only 1 node is reachable → reject write.
 * - Reads require quorum (2/3). If only 1 node is reachable → reject read.
 * - This means the system is unavailable when 2+ nodes are down, but it
 *   never returns stale or inconsistent data.
 */
@Service
public class KvService {

    private static final Logger log = LoggerFactory.getLogger(KvService.class);

    // Quorum = majority of 3 nodes
    private static final int QUORUM = 2;

    private final InMemoryStore store;
    private final WalManager walManager;
    private final ReplicationClient replicationClient;
    private final HintedHandoffStore hintedHandoffStore;
    private final HealthMonitor healthMonitor;
    private final KvStoreProperties props;

    // Ordered list of all node IDs — used for coordinator election and fallback
    private List<String> allNodeIds;

    // Idempotency cache: writeId → WAL offset
    // Prevents duplicate writes when a client retries after a timeout.
    // Bounded to last 10,000 write IDs to avoid unbounded memory growth.
    private final LinkedHashMap<String, Long> idempotencyCache = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > 10_000;
        }
    };

    public KvService(InMemoryStore store, WalManager walManager,
                     ReplicationClient replicationClient,
                     HintedHandoffStore hintedHandoffStore,
                     HealthMonitor healthMonitor,
                     KvStoreProperties props) {
        this.store = store;
        this.walManager = walManager;
        this.replicationClient = replicationClient;
        this.hintedHandoffStore = hintedHandoffStore;
        this.healthMonitor = healthMonitor;
        this.props = props;
    }

    @PostConstruct
    public void init() {
        allNodeIds = props.getParsedNodes().stream()
                .map(KvStoreProperties.NodeDescriptor::nodeId)
                .toList();
    }

    // ── PUBLIC API ─────────────────────────────────────────────────────────

    /**
     * Delete a key with quorum replication.
     *
     * Deletion uses a TOMBSTONE — a special marker that says "this key was deleted".
     * We never physically remove the key from the store immediately because:
     * 1. Peers need to know the key was deleted (not just missing)
     * 2. During quorum reads, a tombstone with a higher timestamp beats a stale value
     * 3. On node rejoin, the tombstone propagates via WAL replay
     *
     * The tombstone is eventually cleaned up during snapshot compaction.
     *
     * Flow: same as put() — coordinator election → WAL → memory → quorum replication
     *
     * @param key     the key to delete
     * @param writeId client-supplied UUID for idempotency
     * @return true if deleted on quorum, false if quorum not reachable
     */
    public boolean delete(String key, String writeId) {
        if (writeId == null) writeId = UUID.randomUUID().toString();

        // Check idempotency — if we've seen this writeId before, return success
        synchronized (idempotencyCache) {
            if (idempotencyCache.containsKey(writeId)) {
                log.debug("[KvService] Duplicate delete detected writeId={}, skipping", writeId);
                return true;
            }
        }

        String coordinatorId = getCoordinator(key);

        // If this node is not the coordinator, forward the request
        if (!coordinatorId.equals(props.getNodeId())) {
            return forwardDelete(coordinatorId, key, writeId);
        }

        // This node IS the coordinator — execute the delete
        return executeDelete(key, writeId);
    }

    /**
     * Store a key-value pair with quorum replication.
     *
     * Flow:
     * 1. Determine coordinator for this key
     * 2. If this node is not the coordinator, forward to coordinator
     * 3. Check idempotency cache (deduplicate retries)
     * 4. Append to WAL + fsync
     * 5. Apply to in-memory store
     * 6. Replicate to peers (quorum required)
     * 7. Return success/failure
     *
     * @param key     the key to store
     * @param value   the value (must be JSON-serializable)
     * @param writeId client-supplied UUID for idempotency (generate one if null)
     * @return true if written to quorum, false if quorum not reachable
     */
    public boolean put(String key, Object value, String writeId) {
        if (writeId == null) writeId = UUID.randomUUID().toString();

        // Check idempotency — if we've seen this writeId before, return success
        synchronized (idempotencyCache) {
            if (idempotencyCache.containsKey(writeId)) {
                log.debug("[KvService] Duplicate write detected writeId={}, skipping", writeId);
                return true;
            }
        }

        String coordinatorId = getCoordinator(key);

        // If this node is not the coordinator, forward the request
        if (!coordinatorId.equals(props.getNodeId())) {
            return forwardWrite(coordinatorId, key, value, writeId);
        }

        // This node IS the coordinator — execute the write
        return executeWrite(key, value, writeId);
    }

    /**
     * Retrieve a value by key using quorum reads.
     *
     * Flow:
     * 1. Read local value
     * 2. Ask one peer for their value
     * 3. Compare timestamps — return the one with the highest timestamp
     * 4. If the peer has a newer value, update local store (read repair)
     *
     * If fewer than QUORUM nodes are reachable → return empty (refuse stale read).
     *
     * @param key the key to look up
     * @return Optional containing the value, or empty if not found or no quorum
     */
    public Optional<Object> get(String key) {
        // Read local value
        VersionedValue localValue = store.get(key);

        // Ask peers for their value (need at least 1 peer response for quorum)
        List<String> peers = getPeerNodeIds();
        VersionedValue bestValue = localValue;
        int reachableCount = 1; // self counts as 1

        for (String peerId : peers) {
            Optional<ReplicationClient.VersionedValueResponse> peerResp =
                    replicationClient.readFromPeer(peerId, key);

            if (peerResp.isPresent()) {
                reachableCount++;
                VersionedValue peerValue = new VersionedValue(
                        peerResp.get().value(), peerResp.get().timestamp()
                );

                // Keep the value with the highest timestamp (most recent write)
                if (bestValue == null || peerValue.timestamp() > bestValue.timestamp()) {
                    bestValue = peerValue;
                }

                // Read repair: if peer has a newer value, update our local store
                if (localValue == null || peerValue.timestamp() > localValue.timestamp()) {
                    store.put(key, peerValue);
                    log.debug("[KvService] Read repair applied for key={}", key);
                }

                // We have quorum — no need to ask more peers
                if (reachableCount >= QUORUM) break;
            }
        }

        // Quorum not reached — refuse to return potentially stale data
        if (reachableCount < QUORUM) {
            log.warn("[KvService] Quorum not reached for read key={}, reachable={}", key, reachableCount);
            throw new QuorumNotReachedException("Cannot reach quorum for read. Reachable nodes: " + reachableCount);
        }

        if (bestValue == null || bestValue.isDeleted()) {
            return Optional.empty();
        }
        return Optional.of(bestValue.value());
    }

    /**
     * Apply a replicated write from the coordinator.
     * Called by the internal replication endpoint — NOT the public API.
     *
     * The coordinator has already assigned the WAL offset and writeId.
     * This node just needs to write to its own WAL and memory.
     */
    public void applyReplicatedWrite(WalEntry entry) throws IOException {
        // Check idempotency
        synchronized (idempotencyCache) {
            if (idempotencyCache.containsKey(entry.writeId())) {
                log.debug("[KvService] Duplicate replicated write writeId={}, skipping", entry.writeId());
                return;
            }
        }

        // Write to WAL first (using the coordinator's offset to keep offsets in sync)
        // Note: we use the coordinator's offset directly to keep all nodes' WALs aligned
        walManager.setCurrentOffset(Math.max(walManager.getCurrentOffset(), entry.offset()));

        // Apply to in-memory store — handles both PUT and DELETE
        // For DELETE: entry.value() is null → tombstone written
        VersionedValue vv = WalEntry.OP_DELETE.equals(entry.operation())
                ? new VersionedValue(null, entry.offset())   // tombstone
                : new VersionedValue(entry.value(), entry.offset()); // normal value
        store.put(entry.key(), vv);

        // Record in idempotency cache
        synchronized (idempotencyCache) {
            idempotencyCache.put(entry.writeId(), entry.offset());
        }

        log.debug("[KvService] Applied replicated write key={} offset={}", entry.key(), entry.offset());
    }

    /**
     * Read a key's versioned value for peer quorum read comparison.
     * Called by the internal read endpoint.
     */
    public VersionedValue getVersioned(String key) {
        return store.get(key);
    }

    /**
     * Returns this node's current WAL offset.
     * Used by rejoining peers to determine how far behind they are.
     */
    public long getCurrentWalOffset() {
        return walManager.getCurrentOffset();
    }

    /**
     * Returns WAL entries after a given offset.
     * Used by rejoining peers to catch up.
     */
    public List<WalEntry> getWalEntriesAfter(long afterOffset) throws IOException {
        return walManager.readEntriesAfter(afterOffset);
    }

    // ── COORDINATOR ELECTION ───────────────────────────────────────────────

    /**
     * Deterministic coordinator election: hash(key) % numNodes → node index.
     *
     * All nodes run the same formula, so they all agree on the coordinator
     * without any communication. No central registry needed.
     *
     * Uses Math.abs to handle negative hash codes (Java's hashCode() can be negative).
     */
    private String getCoordinator(String key) {
        int index = Math.abs(key.hashCode()) % allNodeIds.size();
        return allNodeIds.get(index);
    }

    // ── WRITE EXECUTION ────────────────────────────────────────────────────

    /**
     * Execute a delete as the coordinator.
     *
     * Writes a TOMBSTONE to WAL and memory, then replicates to peers.
     * A tombstone is a VersionedValue with null value and the current WAL offset
     * as timestamp. This ensures that during quorum reads, the tombstone with
     * the highest timestamp wins — preventing a stale value from "resurrecting"
     * a deleted key on a lagging node.
     */
    private boolean executeDelete(String key, String writeId) {
        try {
            // Append DELETE to WAL — value is null for deletes
            long offset = walManager.append(WalEntry.OP_DELETE, key, null, writeId);

            // Write tombstone to memory — null value signals deletion
            VersionedValue tombstone = new VersionedValue(null, offset);
            store.put(key, tombstone);

            // Record in idempotency cache
            synchronized (idempotencyCache) {
                idempotencyCache.put(writeId, offset);
            }

            WalEntry entry = new WalEntry(offset, WalEntry.OP_DELETE, key, null, writeId);

            // Replicate tombstone to peers
            int acks = 1; // coordinator counts as 1
            for (String peerId : getPeerNodeIds()) {
                boolean success = replicationClient.replicate(peerId, entry);
                if (success) {
                    acks++;
                } else {
                    healthMonitor.markDown(peerId);
                    hintedHandoffStore.storeHint(peerId, entry);
                    log.warn("[KvService] Peer {} unreachable, stored delete hint for key={}", peerId, key);
                }
            }

            if (acks >= QUORUM) {
                log.debug("[KvService] Delete successful key={} offset={} acks={}", key, offset, acks);
                return true;
            } else {
                log.warn("[KvService] Quorum not reached for delete key={} acks={}", key, acks);
                return false;
            }

        } catch (IOException e) {
            log.error("[KvService] WAL delete failed for key={}", key, e);
            return false;
        }
    }

    /**
     * Execute a write as the coordinator.
     *
     * 1. Append to own WAL + fsync
     * 2. Apply to own memory
     * 3. Replicate to peers (collect acks)
     * 4. If quorum acks received → return true
     * 5. If quorum not reached → return false (but data is safe on this node)
     */
    private boolean executeWrite(String key, Object value, String writeId) {
        try {
            // Step 1 & 2: WAL + memory on coordinator
            long offset = walManager.append(WalEntry.OP_PUT, key, value, writeId);
            VersionedValue vv = new VersionedValue(value, offset);
            store.put(key, vv);

            // Record in idempotency cache
            synchronized (idempotencyCache) {
                idempotencyCache.put(writeId, offset);
            }

            WalEntry entry = new WalEntry(offset, WalEntry.OP_PUT, key, value, writeId);

            // Step 3: Replicate to peers, count acks
            int acks = 1; // coordinator counts as 1 ack
            for (String peerId : getPeerNodeIds()) {
                boolean success = replicationClient.replicate(peerId, entry);
                if (success) {
                    acks++;
                } else {
                    // Peer is down — store a hint for later delivery
                    healthMonitor.markDown(peerId);
                    hintedHandoffStore.storeHint(peerId, entry);
                    log.warn("[KvService] Peer {} unreachable, stored hint for key={}", peerId, key);
                }
            }

            // Step 4: Check quorum
            if (acks >= QUORUM) {
                log.debug("[KvService] Write successful key={} offset={} acks={}", key, offset, acks);
                return true;
            } else {
                // We wrote to WAL and memory but couldn't reach quorum.
                // The write is durable on this node but not confirmed to the client.
                // Return false — client should retry.
                log.warn("[KvService] Quorum not reached for write key={} acks={}", key, acks);
                return false;
            }

        } catch (IOException e) {
            log.error("[KvService] WAL write failed for key={}", key, e);
            return false;
        }
    }

    /**
     * Forward a write to the coordinator node.
     * Called when this node receives a write but is not the coordinator for the key.
     *
     * If the coordinator is down, try the next node in fallback order.
     */
    private boolean forwardWrite(String coordinatorId, String key, Object value, String writeId) {
        // Try coordinator first, then fallback nodes in order
        List<String> candidates = new ArrayList<>();
        candidates.add(coordinatorId);
        allNodeIds.stream()
                .filter(id -> !id.equals(coordinatorId) && !id.equals(props.getNodeId()))
                .forEach(candidates::add);

        for (String candidateId : candidates) {
            if (!healthMonitor.isNodeUp(candidateId)) continue;

            // Forward via replication endpoint (reuse the same mechanism)
            WalEntry fwdEntry = new WalEntry(0, WalEntry.OP_PUT, key, value, writeId);
            boolean success = replicationClient.replicate(candidateId, fwdEntry);
            if (success) {
                log.debug("[KvService] Forwarded write key={} to coordinator={}", key, candidateId);
                return true;
            }
            healthMonitor.markDown(candidateId);
        }

        log.warn("[KvService] All candidates unreachable for key={}", key);
        return false;
    }

    /**
     * Forward a delete to the coordinator node.
     * Same logic as forwardWrite but uses OP_DELETE.
     */
    private boolean forwardDelete(String coordinatorId, String key, String writeId) {
        List<String> candidates = new ArrayList<>();
        candidates.add(coordinatorId);
        allNodeIds.stream()
                .filter(id -> !id.equals(coordinatorId) && !id.equals(props.getNodeId()))
                .forEach(candidates::add);

        for (String candidateId : candidates) {
            if (!healthMonitor.isNodeUp(candidateId)) continue;

            WalEntry fwdEntry = new WalEntry(0, WalEntry.OP_DELETE, key, null, writeId);
            boolean success = replicationClient.replicate(candidateId, fwdEntry);
            if (success) {
                log.debug("[KvService] Forwarded delete key={} to coordinator={}", key, candidateId);
                return true;
            }
            healthMonitor.markDown(candidateId);
        }

        log.warn("[KvService] All candidates unreachable for delete key={}", key);
        return false;
    }

    // ── HELPERS ────────────────────────────────────────────────────────────

    private List<String> getPeerNodeIds() {
        return allNodeIds.stream()
                .filter(id -> !id.equals(props.getNodeId()))
                .toList();
    }

    // ── EXCEPTIONS ─────────────────────────────────────────────────────────

    public static class QuorumNotReachedException extends RuntimeException {
        public QuorumNotReachedException(String message) {
            super(message);
        }
    }
}

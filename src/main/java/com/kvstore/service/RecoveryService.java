package com.kvstore.service;

import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.VersionedValue;
import com.kvstore.model.WalEntry;
import com.kvstore.replication.ReplicationClient;
import com.kvstore.snapshot.SnapshotManager;
import com.kvstore.store.InMemoryStore;
import com.kvstore.wal.WalManager;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Handles node startup recovery.
 *
 * Recovery sequence on node restart:
 *
 * Phase 1 — Local recovery (from own disk):
 *   1. Load snapshot.json → restore in-memory store to last checkpoint
 *   2. Replay WAL entries after snapshot offset → apply missed writes
 *   After phase 1, the node is at the state it was in when it crashed.
 *
 * Phase 2 — Peer sync (catch up on missed writes):
 *   3. Ask peers for their current WAL offset
 *   4. If peers are ahead → fetch and apply missed WAL entries
 *   After phase 2, the node is fully caught up with the cluster.
 *
 * Phase 3 — Ready:
 *   5. Node starts accepting reads and writes
 *
 * Why two phases?
 * Phase 1 handles the node's own crash (data it had before going down).
 * Phase 2 handles writes that happened while the node was down.
 * Without phase 2, the node would serve stale data for keys written during its downtime.
 */
@Service
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final SnapshotManager snapshotManager;
    private final WalManager walManager;
    private final InMemoryStore store;
    private final ReplicationClient replicationClient;
    private final KvStoreProperties props;

    public RecoveryService(SnapshotManager snapshotManager, WalManager walManager,
                           InMemoryStore store, ReplicationClient replicationClient,
                           KvStoreProperties props) {
        this.snapshotManager = snapshotManager;
        this.walManager = walManager;
        this.store = store;
        this.replicationClient = replicationClient;
        this.props = props;
    }

    @PostConstruct
    public void recover() {
        log.info("[Recovery] Starting recovery for node={}", props.getNodeId());
        try {
            // Phase 1: Local recovery
            long snapshotOffset = localRecovery();

            // Phase 2: Peer sync
            peerSync(snapshotOffset);

            log.info("[Recovery] Recovery complete. WAL offset={}, store size={}",
                    walManager.getCurrentOffset(), store.size());
        } catch (IOException e) {
            log.error("[Recovery] Recovery failed", e);
            throw new RuntimeException("Node recovery failed — cannot start safely", e);
        }
    }

    // ── PHASE 1: LOCAL RECOVERY ────────────────────────────────────────────

    /**
     * Restore state from local snapshot + WAL.
     * Returns the WAL offset after local recovery.
     */
    private long localRecovery() throws IOException {
        // Step 1: Load snapshot (fast — bulk restore)
        long snapshotOffset = snapshotManager.loadSnapshot();
        log.info("[Recovery] Snapshot loaded, offset={}", snapshotOffset);

        // Step 2: Replay WAL entries after snapshot offset
        List<WalEntry> walEntries = walManager.readEntriesAfter(snapshotOffset);
        log.info("[Recovery] Replaying {} WAL entries after offset={}", walEntries.size(), snapshotOffset);

        for (WalEntry entry : walEntries) {
            applyWalEntry(entry);
        }

        long localOffset = walManager.getCurrentOffset();
        log.info("[Recovery] Local recovery complete, offset={}", localOffset);
        return localOffset;
    }

    // ── PHASE 2: PEER SYNC ─────────────────────────────────────────────────

    /**
     * Sync with peers to catch up on writes that happened while this node was down.
     *
     * Strategy:
     * 1. Ask all peers for their current WAL offset
     * 2. Find the peer with the highest offset (most up-to-date)
     * 3. If that peer is ahead of us → fetch and apply their WAL entries
     * 4. If no peer is reachable → log warning, proceed with local state
     *    (the node will catch up via real-time replication once it starts serving)
     */
    private void peerSync(long localOffset) throws IOException {
        long bestPeerOffset = localOffset;
        String bestPeerId = null;

        // Find the most up-to-date peer
        for (KvStoreProperties.NodeDescriptor peer : props.getParsedNodes()) {
            if (peer.nodeId().equals(props.getNodeId())) continue;

            Optional<Long> peerOffset = replicationClient.getWalOffset(peer.nodeId());
            if (peerOffset.isPresent() && peerOffset.get() > bestPeerOffset) {
                bestPeerOffset = peerOffset.get();
                bestPeerId = peer.nodeId();
            }
        }

        if (bestPeerId == null) {
            log.warn("[Recovery] No peers reachable for sync. Node will catch up via replication.");
            return;
        }

        if (bestPeerOffset <= localOffset) {
            log.info("[Recovery] Already up to date (local={}, peer={})", localOffset, bestPeerOffset);
            return;
        }

        // Fetch missed entries from the best peer
        log.info("[Recovery] Syncing from peer={} (local offset={}, peer offset={})",
                bestPeerId, localOffset, bestPeerOffset);

        Optional<List<WalEntry>> missedEntries = replicationClient.getWalEntriesAfter(bestPeerId, localOffset);

        if (missedEntries.isEmpty()) {
            log.warn("[Recovery] Could not fetch WAL entries from peer={}", bestPeerId);
            return;
        }

        log.info("[Recovery] Applying {} missed entries from peer={}", missedEntries.get().size(), bestPeerId);
        for (WalEntry entry : missedEntries.get()) {
            applyWalEntry(entry);
        }

        log.info("[Recovery] Peer sync complete, offset={}", walManager.getCurrentOffset());
    }

    // ── HELPERS ────────────────────────────────────────────────────────────

    /**
     * Apply a single WAL entry to the in-memory store.
     * Used during both local recovery and peer sync.
     */
    private void applyWalEntry(WalEntry entry) {
        VersionedValue vv = new VersionedValue(entry.value(), entry.offset());
        store.put(entry.key(), vv);
        walManager.setCurrentOffset(Math.max(walManager.getCurrentOffset(), entry.offset()));
        log.debug("[Recovery] Applied WAL entry offset={} op={} key={}", entry.offset(), entry.operation(), entry.key());
    }
}

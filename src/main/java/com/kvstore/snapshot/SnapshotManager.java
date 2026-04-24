package com.kvstore.snapshot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.VersionedValue;
import com.kvstore.store.InMemoryStore;
import com.kvstore.wal.WalManager;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Map;

/**
 * Manages periodic snapshots of the in-memory store to disk.
 *
 * Why snapshots?
 * Without snapshots, every restart would replay the entire WAL from the
 * beginning — which could take minutes or hours for a long-running node.
 * Snapshots bound the replay time to "entries since last snapshot."
 *
 * Snapshot format (snapshot.json):
 * {
 *   "walOffset": 5000,          ← WAL offset at snapshot time (checkpoint)
 *   "data": { "key": {...}, ... } ← full copy of in-memory store
 * }
 *
 * Safety guarantee:
 * 1. Take a copy of the in-memory store (non-blocking, no write lock needed)
 * 2. Record the current WAL offset as the checkpoint
 * 3. Serialize to a temp file
 * 4. fsync the temp file
 * 5. Atomic rename temp → snapshot.json
 * 6. ONLY THEN truncate WAL up to the checkpoint offset
 *
 * If the node crashes between steps 3-5, the old snapshot is still intact
 * (atomic rename ensures we never have a partial snapshot file).
 * If the node crashes between steps 5-6, the WAL still has the entries —
 * they will be replayed on top of the snapshot (idempotent).
 */
@Component
public class SnapshotManager {

    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    private final InMemoryStore store;
    private final WalManager walManager;
    private final KvStoreProperties props;
    private final ObjectMapper objectMapper;

    private Path snapshotPath;
    private Path tempSnapshotPath;

    public SnapshotManager(InMemoryStore store, WalManager walManager,
                           KvStoreProperties props, ObjectMapper objectMapper) {
        this.store = store;
        this.walManager = walManager;
        this.props = props;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws IOException {
        Path dataDir = Path.of(props.getDataDir(), props.getNodeId());
        Files.createDirectories(dataDir);
        snapshotPath = dataDir.resolve("snapshot.json");
        tempSnapshotPath = dataDir.resolve("snapshot.json.tmp");
    }

    /**
     * Triggered periodically by Spring's scheduler.
     * Also called manually before backup or planned maintenance.
     *
     * fixedDelayString means: wait N ms AFTER the previous snapshot completes
     * before starting the next one. This prevents overlapping snapshots.
     */
    @Scheduled(fixedDelayString = "${kvstore.snapshot-interval-ms:300000}")
    public void takeSnapshot() {
        try {
            doSnapshot();
        } catch (IOException e) {
            // Log but don't crash — WAL is still intact, recovery is still possible
            log.error("[Snapshot] Failed to take snapshot", e);
        }
    }

    /**
     * Core snapshot logic. Can be called directly for on-demand snapshots.
     *
     * Returns the WAL offset at which the snapshot was taken.
     */
    public long doSnapshot() throws IOException {
        // Step 1: Record WAL offset BEFORE taking the data copy.
        // Any writes that arrive after this point will be in the WAL
        // and will be replayed on top of this snapshot during recovery.
        long snapshotOffset = walManager.getCurrentOffset();

        // Step 2: Take a non-blocking copy of the in-memory store.
        // InMemoryStore.snapshot() returns a HashMap copy — no write lock needed.
        // Writes that arrive during serialization go to WAL and memory normally;
        // they will be replayed on top of this snapshot.
        Map<String, VersionedValue> dataCopy = store.snapshot();

        // Step 3: Build the snapshot document
        SnapshotDocument doc = new SnapshotDocument(snapshotOffset, dataCopy);

        // Step 4: Write to temp file first (never write directly to snapshot.json)
        objectMapper.writeValue(tempSnapshotPath.toFile(), doc);

        // Step 5: fsync the temp file — ensure it's physically on disk
        try (var channel = FileChannel.open(tempSnapshotPath, StandardOpenOption.WRITE)) {
            channel.force(true);
        }

        // Step 6: Atomic rename temp → snapshot.json
        // If the node crashes here, either the old or new snapshot exists — never partial.
        Files.move(tempSnapshotPath, snapshotPath,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);

        log.info("[Snapshot] Saved snapshot at WAL offset={}, keys={}", snapshotOffset, dataCopy.size());

        // Step 7: NOW safe to truncate WAL up to snapshotOffset
        walManager.truncateUpTo(snapshotOffset);

        return snapshotOffset;
    }

    /**
     * Load the latest snapshot from disk.
     * Called during node startup before WAL replay.
     *
     * Returns the WAL offset of the snapshot (0 if no snapshot exists).
     */
    public long loadSnapshot() throws IOException {
        if (!Files.exists(snapshotPath)) {
            log.info("[Snapshot] No snapshot found, starting fresh");
            return 0;
        }

        SnapshotDocument doc = objectMapper.readValue(snapshotPath.toFile(), SnapshotDocument.class);
        store.loadFromSnapshot(doc.data());
        walManager.setCurrentOffset(doc.walOffset());

        log.info("[Snapshot] Loaded snapshot at WAL offset={}, keys={}", doc.walOffset(), doc.data().size());
        return doc.walOffset();
    }

    /**
     * Returns true if a snapshot file exists on disk.
     */
    public boolean snapshotExists() {
        return Files.exists(snapshotPath);
    }

    /**
     * The on-disk snapshot document structure.
     */
    public record SnapshotDocument(long walOffset, Map<String, VersionedValue> data) {}
}

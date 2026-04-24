package com.kvstore.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.WalEntry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the Write-Ahead Log (WAL) for crash recovery.
 *
 * WAL guarantees:
 * 1. Every write is appended to the WAL file and fsynced BEFORE being applied
 *    to the in-memory store. This ensures that if the node crashes after
 *    acknowledging a write, the write can be recovered on restart.
 *
 * 2. The WAL is append-only. Entries are never modified. Truncation happens
 *    only after a snapshot has been safely fsynced to disk (see SnapshotManager).
 *
 * 3. Each entry has a monotonically increasing offset (logical clock). This
 *    offset is used to:
 *    - Determine what a restarting node missed (peer sync)
 *    - Record the snapshot checkpoint (snapshot_offset)
 *    - Deduplicate idempotent retries (writeId)
 *
 * File format: one JSON line per entry (newline-delimited JSON / NDJSON).
 * Simple to append, simple to replay line by line.
 */
@Component
public class WalManager {

    private static final Logger log = LoggerFactory.getLogger(WalManager.class);

    private final KvStoreProperties props;
    private final ObjectMapper objectMapper;

    // Monotonically increasing offset — the logical clock for this node.
    private final AtomicLong currentOffset = new AtomicLong(0);

    // Lock to serialize WAL appends. Multiple threads may call append() concurrently.
    private final ReentrantLock appendLock = new ReentrantLock();

    private Path walFilePath;
    private BufferedWriter walWriter;
    private FileChannel walChannel; // used for fsync

    public WalManager(KvStoreProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() throws IOException {
        Path dataDir = Path.of(props.getDataDir(), props.getNodeId());
        Files.createDirectories(dataDir);
        walFilePath = dataDir.resolve("wal.log");

        // Open in append mode — never overwrite existing entries
        FileOutputStream fos = new FileOutputStream(walFilePath.toFile(), true);
        walChannel = fos.getChannel();
        walWriter = new BufferedWriter(new OutputStreamWriter(fos));

        log.info("[WAL] Initialized at {}", walFilePath);
    }

    /**
     * Append a new entry to the WAL and fsync to disk.
     *
     * This MUST be called before applying the write to the in-memory store.
     * Returns the assigned WAL offset for this entry.
     *
     * Thread safety: serialized by appendLock so offsets are strictly ordered.
     */
    public long append(String operation, String key, Object value, String writeId) throws IOException {
        appendLock.lock();
        try {
            long offset = currentOffset.incrementAndGet();
            WalEntry entry = new WalEntry(offset, operation, key, value, writeId);
            String line = objectMapper.writeValueAsString(entry);

            walWriter.write(line);
            walWriter.newLine();
            walWriter.flush();

            // fsync: force OS buffer to physical disk before returning.
            // Without this, a crash after write() but before physical disk write
            // would lose the entry even though we think it's persisted.
            walChannel.force(true);

            log.debug("[WAL] Appended offset={} op={} key={}", offset, operation, key);
            return offset;
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Read all WAL entries with offset > afterOffset.
     * Used during:
     * - Node restart: replay entries after the snapshot checkpoint
     * - Peer sync: stream missed entries to a rejoining node
     */
    public List<WalEntry> readEntriesAfter(long afterOffset) throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        if (!Files.exists(walFilePath)) return entries;

        try (BufferedReader reader = Files.newBufferedReader(walFilePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                WalEntry entry = objectMapper.readValue(line, WalEntry.class);
                if (entry.offset() > afterOffset) {
                    entries.add(entry);
                }
            }
        }
        return entries;
    }

    /**
     * Truncate WAL entries with offset <= upToOffset.
     *
     * SAFETY: Only call this AFTER the snapshot has been fsynced to disk.
     * If called before, a crash between truncation and snapshot completion
     * would result in data loss.
     *
     * Implementation: rewrite the WAL file keeping only entries after upToOffset.
     * This is safe because we hold the appendLock during the rewrite.
     */
    public void truncateUpTo(long upToOffset) throws IOException {
        appendLock.lock();
        try {
            List<WalEntry> toKeep = readEntriesAfter(upToOffset);

            // Close current writer
            walWriter.close();
            walChannel.close();

            // Rewrite WAL with only the entries we want to keep
            Path tempPath = walFilePath.resolveSibling("wal.log.tmp");
            try (BufferedWriter writer = Files.newBufferedWriter(tempPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                for (WalEntry entry : toKeep) {
                    writer.write(objectMapper.writeValueAsString(entry));
                    writer.newLine();
                }
                writer.flush();
            }

            // Atomic rename — either old or new file exists, never a partial state
            Files.move(tempPath, walFilePath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // Reopen writer in append mode
            FileOutputStream fos = new FileOutputStream(walFilePath.toFile(), true);
            walChannel = fos.getChannel();
            walWriter = new BufferedWriter(new OutputStreamWriter(fos));

            log.info("[WAL] Truncated entries up to offset={}, kept {} entries", upToOffset, toKeep.size());
        } finally {
            appendLock.unlock();
        }
    }

    /**
     * Returns the current highest WAL offset on this node.
     * Used by rejoining peers to determine how far behind they are.
     */
    public long getCurrentOffset() {
        return currentOffset.get();
    }

    /**
     * Set the current offset — called during recovery after replaying WAL.
     */
    public void setCurrentOffset(long offset) {
        currentOffset.set(offset);
    }
}

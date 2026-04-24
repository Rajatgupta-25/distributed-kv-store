package com.kvstore.controller;

import com.kvstore.model.VersionedValue;
import com.kvstore.model.WalEntry;
import com.kvstore.service.KvService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Internal REST endpoints for inter-node communication.
 *
 * These endpoints are NOT part of the public API — they are called only
 * by peer nodes (via ReplicationClient). In production, these would be
 * protected by network-level access control (VPC, firewall rules).
 *
 * Endpoints:
 *   PUT  /internal/replicate          → receive a replicated write from coordinator
 *   GET  /internal/read/{key}         → read a versioned value (for quorum reads)
 *   GET  /internal/wal/offset         → get current WAL offset (for peer sync)
 *   GET  /internal/wal/entries        → get WAL entries after offset (for peer sync)
 */
@RestController
@RequestMapping("/internal")
public class InternalController {

    private static final Logger log = LoggerFactory.getLogger(InternalController.class);

    private final KvService kvService;

    public InternalController(KvService kvService) {
        this.kvService = kvService;
    }

    /**
     * PUT /internal/replicate
     *
     * Receive a replicated write from the coordinator.
     * Applies the write to this node's WAL and in-memory store.
     */
    @PutMapping("/replicate")
    public ResponseEntity<?> replicate(@RequestBody WalEntry entry) {
        try {
            kvService.applyReplicatedWrite(entry);
            return ResponseEntity.ok(Map.of("status", "ack", "offset", entry.offset()));
        } catch (IOException e) {
            log.error("[Internal] Failed to apply replicated write offset={}", entry.offset(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to apply write: " + e.getMessage()));
        }
    }

    /**
     * GET /internal/read/{key}
     *
     * Return the versioned value for a key (value + timestamp).
     * Used by the coordinator during quorum reads to compare versions.
     */
    @GetMapping("/read/{key}")
    public ResponseEntity<?> read(@PathVariable String key) {
        VersionedValue vv = kvService.getVersioned(key);
        if (vv == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of(
                "value", vv.value() != null ? vv.value() : "",
                "timestamp", vv.timestamp()
        ));
    }

    /**
     * GET /internal/wal/offset
     *
     * Return this node's current WAL offset.
     * Used by rejoining nodes to determine how far behind they are.
     */
    @GetMapping("/wal/offset")
    public ResponseEntity<Long> walOffset() {
        return ResponseEntity.ok(kvService.getCurrentWalOffset());
    }

    /**
     * GET /internal/wal/entries?afterOffset=N
     *
     * Return WAL entries after the given offset.
     * Used by rejoining nodes to catch up on missed writes.
     *
     * Note: In production, this would be paginated and streamed for large gaps.
     * For this demo, we return all entries at once.
     */
    @GetMapping("/wal/entries")
    public ResponseEntity<?> walEntries(@RequestParam long afterOffset) {
        try {
            List<WalEntry> entries = kvService.getWalEntriesAfter(afterOffset);
            return ResponseEntity.ok(entries);
        } catch (IOException e) {
            log.error("[Internal] Failed to read WAL entries after offset={}", afterOffset, e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to read WAL: " + e.getMessage()));
        }
    }
}

package com.kvstore.controller;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kvstore.service.KvService;

/**
 * Public-facing REST API for the KV store.
 *
 * Endpoints:
 *   GET  /kv/{key}           → get a value
 *   PUT  /kv/{key}           → store a value
 *
 * Any node can receive any request. If this node is not the coordinator
 * for the key, KvService will forward the request to the correct coordinator.
 *
 * The X-Write-Id header allows clients to supply their own idempotency token.
 * If not provided, a random UUID is generated. Clients should supply the same
 * X-Write-Id on retries to avoid duplicate writes.
 */
@RestController
@RequestMapping("/kv")
public class KvController {

    private static final Logger log = LoggerFactory.getLogger(KvController.class);

    private final KvService kvService;

    public KvController(KvService kvService) {
        this.kvService = kvService;
    }

    /**
     * GET /kv/{key}
     *
     * Returns the value for the key, or 404 if not found.
     * Returns 503 if quorum cannot be reached (not enough nodes available).
     */
    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable String key) {
        try {
            Optional<Object> value = kvService.get(key);
            if (value.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(Map.of("key", key, "value", value.get()));
        } catch (KvService.QuorumNotReachedException e) {
            log.warn("[API] Quorum not reached for GET key={}", key);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * PUT /kv/{key}
     *
     * Stores a key-value pair with quorum replication.
     * Request body: { "value": <any JSON> }
     * Optional header: X-Write-Id (UUID for idempotency)
     *
     * Returns 200 on success, 503 if quorum not reachable.
     */
    @PutMapping("/{key}")
    public ResponseEntity<?> put(
            @PathVariable String key,
            @RequestBody Map<String, Object> body,
            @RequestHeader(value = "X-Write-Id", required = false) String writeId) {

        Object value = body.get("value");
        if (value == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing 'value' in request body"));
        }

        // Generate writeId if client didn't provide one
        if (writeId == null || writeId.isBlank()) {
            writeId = UUID.randomUUID().toString();
        }

        boolean success = kvService.put(key, value, writeId);
        if (success) {
            return ResponseEntity.ok(Map.of(
                    "key", key,
                    "writeId", writeId,
                    "status", "written"
            ));
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Could not reach quorum. Write may not be durable."));
        }
    }

    /**
     * DELETE /kv/{key}
     *
     * Deletes a key using a tombstone — replicates the deletion to quorum.
     * Optional header: X-Write-Id (UUID for idempotency on retries)
     *
     * Returns 200 on success, 404 if key doesn't exist, 503 if no quorum.
     *
     * Design note: We use tombstones (not physical deletion) so that:
     * - Peers know the key was intentionally deleted (not just missing)
     * - A lagging node can't "resurrect" a deleted key with a stale value
     * - The deletion propagates correctly via WAL replay on node rejoin
     */
    @DeleteMapping("/{key}")
    public ResponseEntity<?> delete(
            @PathVariable String key,
            @RequestHeader(value = "X-Write-Id", required = false) String writeId) {

        // Check key exists before deleting
        try {
            Optional<Object> existing = kvService.get(key);
            if (existing.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
        } catch (KvService.QuorumNotReachedException e) {
            // Can't confirm existence — still attempt delete for safety
            log.warn("[API] Quorum not reached checking existence for DELETE key={}", key);
        }

        if (writeId == null || writeId.isBlank()) {
            writeId = UUID.randomUUID().toString();
        }

        boolean success = kvService.delete(key, writeId);
        if (success) {
            return ResponseEntity.ok(Map.of(
                    "key", key,
                    "writeId", writeId,
                    "status", "deleted"
            ));
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Could not reach quorum. Delete may not be durable."));
        }
    }
}

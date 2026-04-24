package com.kvstore.model;

/**
 * A single entry in the Write-Ahead Log.
 *
 * Each entry captures:
 *  - offset:    monotonically increasing sequence number (the logical clock)
 *  - operation: PUT or DELETE
 *  - key:       the key being written
 *  - value:     the value (null for DELETE)
 *  - writeId:   client-supplied idempotency token (UUID) to deduplicate retries
 *
 * The WAL is append-only. Entries are never modified after being written.
 * Truncation happens only after a snapshot has been safely fsynced to disk.
 */
public record WalEntry(long offset, String operation, String key, Object value, String writeId) {

    public static final String OP_PUT    = "PUT";
    public static final String OP_DELETE = "DELETE";
}

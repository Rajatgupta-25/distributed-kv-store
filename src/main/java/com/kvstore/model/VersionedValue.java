package com.kvstore.model;

/**
 * A value stored in the KV store, paired with a logical timestamp.
 *
 * The timestamp (WAL offset at write time) is used during quorum reads
 * to resolve which node has the most recent version of a key.
 *
 * Design decision: We use a monotonically increasing WAL offset as the
 * logical clock rather than wall-clock time, because wall clocks can skew
 * across nodes (NTP drift, leap seconds). WAL offsets are strictly ordered
 * within a coordinator's write sequence.
 */
public record VersionedValue(Object value, long timestamp) {

    /** Sentinel representing a deleted key. */
    public static final VersionedValue TOMBSTONE = new VersionedValue(null, Long.MAX_VALUE);

    public boolean isDeleted() {
        return value == null;
    }
}

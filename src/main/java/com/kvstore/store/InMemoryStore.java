package com.kvstore.store;

import com.kvstore.model.VersionedValue;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory key-value store backed by a ConcurrentHashMap.
 *
 * Design decisions:
 * - ConcurrentHashMap provides segment-level locking, allowing concurrent
 *   reads and writes to different keys without a global lock.
 * - Each value is wrapped in VersionedValue (value + timestamp) so that
 *   quorum reads can compare versions across nodes and return the latest.
 * - This class has NO knowledge of replication or persistence — it is a
 *   pure in-memory data structure. WAL and replication are handled by
 *   WalManager and ReplicationClient respectively.
 */
@Component
public class InMemoryStore {

    // The primary data structure. Key → VersionedValue (value + WAL offset timestamp).
    private final ConcurrentHashMap<String, VersionedValue> store = new ConcurrentHashMap<>();

    /**
     * Store a key with its versioned value.
     * Called after WAL write is confirmed — never before.
     */
    public void put(String key, VersionedValue vv) {
        store.put(key, vv);
    }

    /**
     * Retrieve a versioned value by key.
     * Returns null if the key does not exist.
     */
    public VersionedValue get(String key) {
        return store.get(key);
    }

    /**
     * Returns true if the key exists and is not a tombstone (deleted).
     */
    public boolean containsKey(String key) {
        VersionedValue vv = store.get(key);
        return vv != null && !vv.isDeleted();
    }

    /**
     * Returns a shallow copy of the entire store for snapshot serialization.
     *
     * Design note: We take a copy rather than serializing the live map to
     * avoid holding a lock during the (potentially slow) disk write.
     * The copy is consistent at the moment it is taken; any writes that
     * arrive after this point will be captured in the WAL and replayed
     * on top of the snapshot during recovery.
     */
    public Map<String, VersionedValue> snapshot() {
        return new HashMap<>(store);
    }

    /**
     * Bulk-load data into the store during recovery (snapshot restore).
     * Replaces all existing data — only called on node startup.
     */
    public void loadFromSnapshot(Map<String, VersionedValue> data) {
        store.clear();
        store.putAll(data);
    }

    /**
     * Returns the number of keys currently in the store.
     */
    public int size() {
        return store.size();
    }
}

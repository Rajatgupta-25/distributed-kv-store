package com.kvstore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.VersionedValue;
import com.kvstore.model.WalEntry;
import com.kvstore.replication.HealthMonitor;
import com.kvstore.replication.HintedHandoffStore;
import com.kvstore.replication.ReplicationClient;
import com.kvstore.service.KvService;
import com.kvstore.store.InMemoryStore;
import com.kvstore.wal.WalManager;

/**
 *
 * Key design: deletion writes a tombstone (null value + timestamp) rather than
 * physically removing the key. This ensures:
 * 1. Peers receive the deletion via replication — they don't just "miss" the key
 * 2. A lagging node with a stale value can't resurrect a deleted key
 *    (tombstone timestamp > stale value timestamp → tombstone wins in quorum read)
 * 3. WAL replay on restart correctly re-applies the deletion
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DeleteTest {

    @Mock private WalManager walManager;
    @Mock private ReplicationClient replicationClient;
    @Mock private HintedHandoffStore hintedHandoffStore;
    @Mock private HealthMonitor healthMonitor;
    @Mock private KvStoreProperties props;

    private InMemoryStore store;
    private KvService kvService;

    // "foo" hashes to node1 (index 0) — this node is the coordinator
    private static final String KEY = "foo";

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryStore();

        when(props.getNodeId()).thenReturn("node1");
        when(props.getParsedNodes()).thenReturn(List.of(
                new KvStoreProperties.NodeDescriptor("node1", "localhost", 8081),
                new KvStoreProperties.NodeDescriptor("node2", "localhost", 8082),
                new KvStoreProperties.NodeDescriptor("node3", "localhost", 8083)
        ));

        kvService = new KvService(store, walManager, replicationClient,
                hintedHandoffStore, healthMonitor, props);
        kvService.init();

        when(walManager.append(anyString(), anyString(), any(), anyString())).thenReturn(10L);
    }

    /**
     * Basic delete: write a key, delete it, verify it's gone.
     */
    @Test
    void delete_removes_key_from_store() throws IOException {
        // Pre-populate the store with a value
        store.put(KEY, new VersionedValue("Alice", 5L));

        when(replicationClient.replicate(anyString(), any())).thenReturn(true);

        boolean result = kvService.delete(KEY, "delete-id-1");

        assertThat(result).isTrue();

        // Key should now be a tombstone (isDeleted = true)
        VersionedValue stored = store.get(KEY);
        assertThat(stored).isNotNull();
        assertThat(stored.isDeleted()).isTrue();
        assertThat(stored.value()).isNull();
    }

    /**
     * After delete, get() should return empty (not the old value).
     */
    @Test
    void get_returns_empty_after_delete() {
        // Pre-populate with a value
        store.put(KEY, new VersionedValue("Alice", 5L));

        // Peer also has the value
        when(replicationClient.readFromPeer(eq("node2"), eq(KEY)))
                .thenReturn(Optional.of(new ReplicationClient.VersionedValueResponse(null, 10L)));

        // After delete, local store has tombstone (timestamp 10 > 5)
        store.put(KEY, new VersionedValue(null, 10L));

        Optional<Object> result = kvService.get(KEY);

        assertThat(result).isEmpty();
    }

    /**
     * Tombstone wins over stale value during quorum read.
     *
     * Scenario: node2 is lagging and still has the old value.
     * Local node has the tombstone (higher timestamp).
     * Quorum read should return empty (tombstone wins).
     */
    @Test
    void tombstone_wins_over_stale_value_in_quorum_read() {
        // Local node has tombstone (timestamp 10 — from delete)
        store.put(KEY, new VersionedValue(null, 10L));

        // node2 is lagging — still has old value (timestamp 5)
        when(replicationClient.readFromPeer(eq("node2"), eq(KEY)))
                .thenReturn(Optional.of(
                        new ReplicationClient.VersionedValueResponse("Alice", 5L)));

        Optional<Object> result = kvService.get(KEY);

        // Tombstone (t=10) beats stale value (t=5) → key is deleted
        assertThat(result).isEmpty();
    }

    /**
     * Delete is replicated to peers as a tombstone WAL entry.
     */
    @Test
    void delete_replicates_tombstone_to_peers() throws IOException {
        store.put(KEY, new VersionedValue("Alice", 5L));
        when(replicationClient.replicate(anyString(), any())).thenReturn(true);

        kvService.delete(KEY, "delete-id-2");

        // Verify WAL was written with OP_DELETE
        verify(walManager).append(eq(WalEntry.OP_DELETE), eq(KEY), isNull(), eq("delete-id-2"));

        // Verify replication was sent to both peers
        verify(replicationClient).replicate(eq("node2"),
                argThat(e -> WalEntry.OP_DELETE.equals(e.operation()) && e.value() == null));
        verify(replicationClient).replicate(eq("node3"),
                argThat(e -> WalEntry.OP_DELETE.equals(e.operation()) && e.value() == null));
    }

    /**
     * Delete is idempotent — retrying with same writeId has no effect.
     */
    @Test
    void delete_is_idempotent() throws IOException {
        store.put(KEY, new VersionedValue("Alice", 5L));
        when(replicationClient.replicate(anyString(), any())).thenReturn(true);

        // First delete
        kvService.delete(KEY, "delete-id-3");
        // Retry with same writeId
        kvService.delete(KEY, "delete-id-3");

        // WAL should only be written once
        verify(walManager, times(1)).append(anyString(), anyString(), any(), anyString());
    }

    /**
     * Delete succeeds with one peer down (quorum = 2/3).
     * Hint stored for the down peer.
     */
    @Test
    void delete_succeeds_with_one_peer_down() throws IOException {
        store.put(KEY, new VersionedValue("Alice", 5L));

        when(replicationClient.replicate(eq("node2"), any())).thenReturn(false); // down
        when(replicationClient.replicate(eq("node3"), any())).thenReturn(true);  // up

        boolean result = kvService.delete(KEY, "delete-id-4");

        assertThat(result).isTrue();
        // Hint stored for node2 — tombstone will be delivered when it recovers
        verify(hintedHandoffStore).storeHint(eq("node2"),
                argThat(e -> WalEntry.OP_DELETE.equals(e.operation())));
    }

    /**
     * Applying a replicated DELETE entry writes a tombstone.
     */
    @Test
    void applyReplicatedDelete_writes_tombstone() throws IOException {
        // Pre-populate with a value
        store.put(KEY, new VersionedValue("Alice", 5L));

        // Coordinator sends a DELETE replication entry
        WalEntry deleteEntry = new WalEntry(10L, WalEntry.OP_DELETE, KEY, null, "rep-delete-id");
        kvService.applyReplicatedWrite(deleteEntry);

        // Store should now have a tombstone
        VersionedValue stored = store.get(KEY);
        assertThat(stored.isDeleted()).isTrue();
        assertThat(stored.timestamp()).isEqualTo(10L);
    }
}

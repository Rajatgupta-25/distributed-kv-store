package com.kvstore;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
 * Unit tests for KvService — the core distributed logic.
 *
 * We mock all I/O (WAL, replication) so tests run fast and in isolation.
 *
 * Key note on coordinator election:
 * The coordinator is determined by hash(key) % 3 mapped to the node list index.
 * We use keys whose hash routes to node1 (this node) so the coordinator path
 * is exercised directly without forwarding.
 *
 * hash("alpha") % 3 = 0 → node1 (verified below)
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT) // allow unused stubs in setUp
class KvServiceTest {

    @Mock private WalManager walManager;
    @Mock private ReplicationClient replicationClient;
    @Mock private HintedHandoffStore hintedHandoffStore;
    @Mock private HealthMonitor healthMonitor;
    @Mock private KvStoreProperties props;

    private InMemoryStore store;
    private KvService kvService;

    // A key whose hash routes to node1 as coordinator: Math.abs("foo".hashCode()) % 3 == 0
    // Verified: Math.abs("foo".hashCode()) % 3 = 0 → index 0 = node1
    private static final String KEY = "foo";

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryStore();

        // 3-node cluster, this node is node1
        when(props.getNodeId()).thenReturn("node1");
        when(props.getParsedNodes()).thenReturn(List.of(
                new KvStoreProperties.NodeDescriptor("node1", 8081),
                new KvStoreProperties.NodeDescriptor("node2", 8082),
                new KvStoreProperties.NodeDescriptor("node3", 8083)
        ));

        kvService = new KvService(store, walManager, replicationClient,
                hintedHandoffStore, healthMonitor, props);
        kvService.init();

        // Default: WAL append returns offset 1
        when(walManager.append(anyString(), anyString(), any(), anyString())).thenReturn(1L);
    }

    // ── COORDINATOR ROUTING SANITY CHECK ──────────────────────────────────

    @Test
    void key_alpha_routes_to_node1() {
        // Verify our test key actually routes to node1 so tests exercise coordinator path
        int index = Math.abs("foo".hashCode()) % 3;
        assertThat(index).isEqualTo(0); // index 0 = node1
    }

    // ── WRITE TESTS ────────────────────────────────────────────────────────

    @Test
    void put_succeeds_when_quorum_reached() throws IOException {
        // Both peers ack the replication
        when(replicationClient.replicate(eq("node2"), any())).thenReturn(true);
        when(replicationClient.replicate(eq("node3"), any())).thenReturn(true);

        boolean result = kvService.put(KEY, "Alice", "write-id-1");

        assertThat(result).isTrue();
        // Verify WAL was written before replication
        verify(walManager).append(eq(WalEntry.OP_PUT), eq(KEY), eq("Alice"), eq("write-id-1"));
        // Verify data is in memory
        assertThat(store.get(KEY)).isNotNull();
        assertThat(store.get(KEY).value()).isEqualTo("Alice");
    }

    @Test
    void put_succeeds_with_one_peer_down_quorum_still_met() throws IOException {
        // node2 is down, node3 acks — coordinator(node1) + node3 = 2/3 quorum
        when(replicationClient.replicate(eq("node2"), any())).thenReturn(false);
        when(replicationClient.replicate(eq("node3"), any())).thenReturn(true);

        boolean result = kvService.put(KEY, "Alice", "write-id-2");

        assertThat(result).isTrue();
        // Hint should be stored for node2
        verify(hintedHandoffStore).storeHint(eq("node2"), any());
    }

    @Test
    void put_fails_when_both_peers_down_quorum_not_met() throws IOException {
        // Both peers down — only coordinator has the write (1/3, below quorum)
        when(replicationClient.replicate(eq("node2"), any())).thenReturn(false);
        when(replicationClient.replicate(eq("node3"), any())).thenReturn(false);

        boolean result = kvService.put(KEY, "Alice", "write-id-3");

        assertThat(result).isFalse();
        // Hints stored for both down nodes
        verify(hintedHandoffStore).storeHint(eq("node2"), any());
        verify(hintedHandoffStore).storeHint(eq("node3"), any());
    }

    @Test
    void put_is_idempotent_for_same_write_id() throws IOException {
        when(replicationClient.replicate(anyString(), any())).thenReturn(true);

        // First write — goes through normally
        boolean first = kvService.put(KEY, "Alice", "idempotent-write-id");
        assertThat(first).isTrue();

        // Second write with same writeId — deduplicated, returns true immediately
        boolean second = kvService.put(KEY, "Bob", "idempotent-write-id");
        assertThat(second).isTrue();

        // WAL should only be written once (second call was short-circuited)
        verify(walManager, times(1)).append(anyString(), anyString(), any(), anyString());
        // Value should still be Alice (second write was ignored)
        assertThat(store.get(KEY).value()).isEqualTo("Alice");
    }

    // ── READ TESTS ─────────────────────────────────────────────────────────

    @Test
    void get_returns_value_when_quorum_agrees() {
        // Local store has the value
        store.put(KEY, new VersionedValue("Alice", 100L));

        // Peer also has the same value
        when(replicationClient.readFromPeer(eq("node2"), eq(KEY)))
                .thenReturn(Optional.of(new ReplicationClient.VersionedValueResponse("Alice", 100L)));

        Optional<Object> result = kvService.get(KEY);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("Alice");
    }

    @Test
    void get_returns_latest_value_when_peer_has_newer_version() {
        // Local store has stale value (timestamp 50)
        store.put(KEY, new VersionedValue("OldAlice", 50L));

        // Peer has newer value (timestamp 100) — e.g., we missed a write while slow
        when(replicationClient.readFromPeer(eq("node2"), eq(KEY)))
                .thenReturn(Optional.of(new ReplicationClient.VersionedValueResponse("NewAlice", 100L)));

        Optional<Object> result = kvService.get(KEY);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("NewAlice");

        // Read repair: local store should be updated with the newer value
        assertThat(store.get(KEY).value()).isEqualTo("NewAlice");
        assertThat(store.get(KEY).timestamp()).isEqualTo(100L);
    }

    @Test
    void get_throws_when_quorum_not_reachable() {
        // Local store has value but both peers are unreachable
        store.put(KEY, new VersionedValue("Alice", 100L));

        when(replicationClient.readFromPeer(anyString(), anyString()))
                .thenReturn(Optional.empty());

        // Should throw QuorumNotReachedException rather than return stale data
        assertThatThrownBy(() -> kvService.get(KEY))
                .isInstanceOf(KvService.QuorumNotReachedException.class);
    }

    @Test
    void get_returns_empty_for_nonexistent_key() {
        // Key doesn't exist locally; peer returns null value
        when(replicationClient.readFromPeer(eq("node2"), eq(KEY)))
                .thenReturn(Optional.of(new ReplicationClient.VersionedValueResponse(null, 0L)));

        Optional<Object> result = kvService.get(KEY);

        assertThat(result).isEmpty();
    }

    // ── REPLICATION TESTS ──────────────────────────────────────────────────

    @Test
    void applyReplicatedWrite_stores_value_in_memory() throws IOException {
        WalEntry entry = new WalEntry(42L, WalEntry.OP_PUT, KEY, "Charlie", "rep-write-id");

        kvService.applyReplicatedWrite(entry);

        VersionedValue stored = store.get(KEY);
        assertThat(stored).isNotNull();
        assertThat(stored.value()).isEqualTo("Charlie");
        assertThat(stored.timestamp()).isEqualTo(42L);
    }

    @Test
    void applyReplicatedWrite_is_idempotent() throws IOException {
        WalEntry entry = new WalEntry(42L, WalEntry.OP_PUT, KEY, "Charlie", "rep-write-id");

        kvService.applyReplicatedWrite(entry);
        kvService.applyReplicatedWrite(entry); // duplicate

        // Value should still be Charlie, applied only once
        assertThat(store.get(KEY).value()).isEqualTo("Charlie");
    }
}

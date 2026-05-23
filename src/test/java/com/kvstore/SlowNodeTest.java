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
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.WalEntry;
import com.kvstore.replication.HealthMonitor;
import com.kvstore.replication.HintedHandoffStore;
import com.kvstore.replication.ReplicationClient;
import com.kvstore.service.KvService;
import com.kvstore.store.InMemoryStore;
import com.kvstore.wal.WalManager;

/**
 * Tests for slow node behavior.
 *
 * A slow node is one that is alive but responds after the replication timeout.
 * From the coordinator's perspective, a timeout looks the same as a node being down —
 * RestTemplate throws ResourceAccessException in both cases.
 *
 * Expected behavior:
 * - Write still succeeds if quorum (2/3) is reachable within timeout
 * - Slow node gets a hinted handoff entry
 * - When slow node recovers, hint is delivered
 * - Read still works from the 2 fast nodes
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SlowNodeTest {

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

        when(walManager.append(anyString(), anyString(), any(), anyString())).thenReturn(1L);
    }

    /**
     * Scenario: node2 is slow (times out), node3 responds fine.
     *
     * Expected:
     * - Write succeeds (node1 + node3 = quorum)
     * - Hint stored for node2 (will be delivered when it recovers)
     * - Client gets success response — unaware of node2's slowness
     */
    @Test
    void write_succeeds_when_one_node_is_slow() throws IOException {
        // node2 times out (slow) — replication returns false
        when(replicationClient.replicate(eq("node2"), any())).thenReturn(false);
        // node3 responds fine
        when(replicationClient.replicate(eq("node3"), any())).thenReturn(true);

        boolean result = kvService.put(KEY, "Alice", "write-slow-1");

        // Write succeeds — node1 (coordinator) + node3 = 2/3 quorum
        assertThat(result).isTrue();

        // Hint stored for slow node2 — will be delivered when it catches up
        verify(hintedHandoffStore).storeHint(eq("node2"), any(WalEntry.class));

        // node3 was NOT hinted — it responded fine
        verify(hintedHandoffStore, never()).storeHint(eq("node3"), any());
    }

    /**
     * Scenario: node2 is slow, node3 is also slow.
     *
     * Expected:
     * - Write FAILS — only node1 (coordinator) responded, below quorum
     * - Hints stored for both slow nodes
     * - Client gets failure response (503)
     */
    @Test
    void write_fails_when_two_nodes_are_slow() throws IOException {
        // Both peers time out
        when(replicationClient.replicate(eq("node2"), any())).thenReturn(false);
        when(replicationClient.replicate(eq("node3"), any())).thenReturn(false);

        boolean result = kvService.put(KEY, "Alice", "write-slow-2");

        // Write fails — only 1/3 nodes responded (below quorum of 2)
        assertThat(result).isFalse();

        // Hints stored for both slow nodes
        verify(hintedHandoffStore).storeHint(eq("node2"), any(WalEntry.class));
        verify(hintedHandoffStore).storeHint(eq("node3"), any(WalEntry.class));
    }

    /**
     * Scenario: node2 is slow during a read.
     *
     * Expected:
     * - Read still succeeds using node1 (local) + node3
     * - Returns correct value
     */
    @Test
    void read_succeeds_when_one_node_is_slow() {
        // Local store has the value
        store.put(KEY, new com.kvstore.model.VersionedValue("Alice", 100L));

        // node2 times out (slow) — returns empty
        when(replicationClient.readFromPeer(eq("node2"), eq(KEY)))
                .thenReturn(Optional.empty());

        // node3 responds fine
        when(replicationClient.readFromPeer(eq("node3"), eq(KEY)))
                .thenReturn(Optional.of(
                        new ReplicationClient.VersionedValueResponse("Alice", 100L)));

        Optional<Object> result = kvService.get(KEY);

        // Read succeeds — local + node3 = quorum
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("Alice");
    }

    /**
     * Scenario: both peers are slow during a read.
     *
     * Expected:
     * - Read FAILS — cannot reach quorum
     * - Throws QuorumNotReachedException rather than returning stale data
     */
    @Test
    void read_fails_when_both_peers_are_slow() {
        store.put(KEY, new com.kvstore.model.VersionedValue("Alice", 100L));

        // Both peers time out
        when(replicationClient.readFromPeer(anyString(), anyString()))
                .thenReturn(Optional.empty());

        // Should refuse to return data rather than serve potentially stale value
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> kvService.get(KEY))
                .isInstanceOf(KvService.QuorumNotReachedException.class)
                .hasMessageContaining("quorum");
    }
}

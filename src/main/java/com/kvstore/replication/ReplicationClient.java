package com.kvstore.replication;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.WalEntry;

/**
 * Handles all outbound inter-node HTTP communication.
 *
 * This is the Java equivalent of send_to_node() from the assignment.
 * All network calls go through this class so failure handling is centralized.
 *
 * Returns Optional.empty() when a node is unreachable (timeout, connection refused).
 * The caller decides what to do — typically: store a hint and proceed with quorum.
 */
@Component
public class ReplicationClient {

    private static final Logger log = LoggerFactory.getLogger(ReplicationClient.class);

    private final RestTemplate restTemplate;

    // Map of nodeId → base URL, built once at startup
    private final Map<String, String> nodeUrls = new HashMap<>();

    public ReplicationClient(RestTemplate restTemplate, KvStoreProperties props) {
        this.restTemplate = restTemplate;
        props.getParsedNodes().forEach(n -> nodeUrls.put(n.nodeId(), n.baseUrl()));
    }

    /**
     * Replicate a write to a peer node.
     *
     * Sends a PUT /internal/replicate request with the WAL entry.
     * Returns true if the peer acknowledged, false if unreachable.
     *
     * Design: fire-and-forget is NOT used here. We wait for the ack
     * because quorum requires confirmation from 2/3 nodes before
     * returning success to the client.
     */
    public boolean replicate(String targetNodeId, WalEntry entry) {
        String url = nodeUrls.get(targetNodeId) + "/internal/replicate";
        try {
            ReplicateRequest req = new ReplicateRequest(
                    entry.offset(), entry.operation(), entry.key(), entry.value(), entry.writeId()
            );
            restTemplate.put(url, req);
            log.debug("[Replication] Replicated offset={} to {}", entry.offset(), targetNodeId);
            return true;
        } catch (ResourceAccessException e) {
            // Node is down or timed out — caller will store a hint
            log.warn("[Replication] Node {} unreachable: {}", targetNodeId, e.getMessage());
            return false;
        } catch (RestClientException e) {
            log.error("[Replication] Unexpected error replicating to {}: {}", targetNodeId, e.getMessage());
            return false;
        }
    }

    /**
     * Ask a peer node for its current WAL offset.
     * Used when a node restarts and needs to know how far behind it is.
     */
    public Optional<Long> getWalOffset(String targetNodeId) {
        String url = nodeUrls.get(targetNodeId) + "/internal/wal/offset";
        try {
            Long offset = restTemplate.getForObject(url, Long.class);
            return Optional.ofNullable(offset);
        } catch (RestClientException e) {
            log.warn("[Replication] Could not get WAL offset from {}: {}", targetNodeId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Fetch WAL entries from a peer node after a given offset.
     * Used during peer sync when a node rejoins after being down.
     */
    @SuppressWarnings("unchecked")
    public Optional<List<WalEntry>> getWalEntriesAfter(String targetNodeId, long afterOffset) {
        String url = nodeUrls.get(targetNodeId) + "/internal/wal/entries?afterOffset=" + afterOffset;
        try {
            List<WalEntry> entries = restTemplate.getForObject(url, List.class);
            return Optional.ofNullable(entries);
        } catch (RestClientException e) {
            log.warn("[Replication] Could not fetch WAL entries from {}: {}", targetNodeId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Read a key from a peer node for quorum read comparison.
     * Returns the VersionedValue (value + timestamp) or empty if unreachable.
     */
    public Optional<VersionedValueResponse> readFromPeer(String targetNodeId, String key) {
        String url = nodeUrls.get(targetNodeId) + "/internal/read/" + key;
        try {
            VersionedValueResponse resp = restTemplate.getForObject(url, VersionedValueResponse.class);
            return Optional.ofNullable(resp);
        } catch (RestClientException e) {
            log.warn("[Replication] Could not read key={} from {}: {}", key, targetNodeId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Check if a peer node is alive (used by health monitor).
     */
    public boolean isAlive(String targetNodeId) {
        String url = nodeUrls.get(targetNodeId) + "/actuator/health";
        try {
            restTemplate.getForObject(url, String.class);
            return true;
        } catch (RestClientException e) {
            return false;
        }
    }

    // ── DTOs for inter-node communication ─────────────────────────────────

    public record ReplicateRequest(long offset, String operation, String key, Object value, String writeId) {}

    public record VersionedValueResponse(Object value, long timestamp) {}
}

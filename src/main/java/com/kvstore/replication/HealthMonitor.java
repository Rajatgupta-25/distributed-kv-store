package com.kvstore.replication;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kvstore.config.KvStoreProperties;
import com.kvstore.model.HintedWrite;

/**
 * Monitors peer node health and drains hinted handoff queues when nodes recover.
 *
 * Two responsibilities:
 * 1. Track which peer nodes are currently reachable (used by KvService for quorum decisions)
 * 2. When a previously-down node comes back up, drain its hint queue
 *
 * Design: heartbeat-based detection. Every N seconds, ping all peers.
 * If a peer responds → mark as UP, drain hints.
 * If a peer doesn't respond → mark as DOWN.
 *
 * This is simpler than gossip-based failure detection (used by Cassandra)
 * but sufficient for a 3-node cluster.
 */
@Component
public class HealthMonitor {

    private static final Logger log = LoggerFactory.getLogger(HealthMonitor.class);

    private final ReplicationClient replicationClient;
    private final HintedHandoffStore hintedHandoffStore;
    private final KvStoreProperties props;

    // Current known status of each peer node
    // true = reachable, false = unreachable
    private final ConcurrentHashMap<String, Boolean> nodeStatus = new ConcurrentHashMap<>();

    public HealthMonitor(ReplicationClient replicationClient,
                         HintedHandoffStore hintedHandoffStore,
                         KvStoreProperties props) {
        this.replicationClient = replicationClient;
        this.hintedHandoffStore = hintedHandoffStore;
        this.props = props;

        // Initialize all peers as unknown (optimistically UP)
        props.getParsedNodes().forEach(n -> {
            if (!n.nodeId().equals(props.getNodeId())) {
                nodeStatus.put(n.nodeId(), true);
            }
        });
    }

    /**
     * Heartbeat check — runs every 5 seconds.
     * Pings all peer nodes and updates their status.
     * If a node just recovered, drain its hint queue.
     */
    @Scheduled(fixedDelayString = "${kvstore.hinted-handoff-interval-ms:5000}")
    public void heartbeat() {
        props.getParsedNodes().forEach(node -> {
            if (node.nodeId().equals(props.getNodeId())) return; // skip self

            boolean wasUp = nodeStatus.getOrDefault(node.nodeId(), true);
            boolean isUp = replicationClient.isAlive(node.nodeId());

            nodeStatus.put(node.nodeId(), isUp);

            if (!wasUp && isUp) {
                // Node just recovered — drain hint queue
                log.info("[HealthMonitor] Node {} recovered, draining hint queue", node.nodeId());
                drainHints(node.nodeId());
            } else if (wasUp && !isUp) {
                log.warn("[HealthMonitor] Node {} went down", node.nodeId());
            }
        });
    }

    /**
     * Drain all pending hints for a recovered node.
     * Sends each hinted write to the node in offset order.
     */
    private void drainHints(String targetNodeId) {
        Queue<HintedWrite> hints = hintedHandoffStore.getHints(targetNodeId);
        int delivered = 0;
        int failed = 0;

        for (HintedWrite hint : hints) {
            boolean success = replicationClient.replicate(targetNodeId, hint.walEntry());
            if (success) {
                hintedHandoffStore.removeHint(targetNodeId, hint);
                delivered++;
            } else {
                // Node went back down during drain — stop and retry next heartbeat
                log.warn("[HealthMonitor] Failed to drain hint for node={}, will retry", targetNodeId);
                failed++;
                break;
            }
        }

        log.info("[HealthMonitor] Drained {} hints to node={}, {} failed", delivered, targetNodeId, failed);
    }

    /**
     * Returns true if the given node is currently considered reachable.
     * Used by KvService to decide whether to attempt replication or store a hint.
     */
    public boolean isNodeUp(String nodeId) {
        return nodeStatus.getOrDefault(nodeId, true);
    }

    /**
     * Returns the count of currently reachable nodes (including self).
     */
    public int reachableNodeCount() {
        long upPeers = nodeStatus.values().stream().filter(Boolean::booleanValue).count();
        return (int) upPeers + 1; // +1 for self
    }

    /**
     * Mark a node as down immediately (called when a replication attempt fails).
     * This avoids waiting for the next heartbeat cycle to detect the failure.
     */
    public void markDown(String nodeId) {
        nodeStatus.put(nodeId, false);
    }

    /**
     * Returns a snapshot of all node statuses for diagnostics.
     */
    public Map<String, Boolean> getNodeStatuses() {
        return Map.copyOf(nodeStatus);
    }
}

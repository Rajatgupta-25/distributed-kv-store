package com.kvstore.config;

import java.util.Arrays;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Typed configuration properties bound from application.yml.
 * All cluster topology and tuning parameters live here.
 */
@Component
@ConfigurationProperties(prefix = "kvstore")
public class KvStoreProperties {

    private String nodeId;
    private String allNodes;
    private String dataDir;
    private long snapshotIntervalMs;
    private long walMaxEntries;
    private int replicationTimeoutMs;
    private long hintedHandoffIntervalMs;

    // ── Getters & Setters ──────────────────────────────────────────────────

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getAllNodes() { return allNodes; }
    public void setAllNodes(String allNodes) { this.allNodes = allNodes; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public long getSnapshotIntervalMs() { return snapshotIntervalMs; }
    public void setSnapshotIntervalMs(long snapshotIntervalMs) { this.snapshotIntervalMs = snapshotIntervalMs; }

    public long getWalMaxEntries() { return walMaxEntries; }
    public void setWalMaxEntries(long walMaxEntries) { this.walMaxEntries = walMaxEntries; }

    public int getReplicationTimeoutMs() { return replicationTimeoutMs; }
    public void setReplicationTimeoutMs(int replicationTimeoutMs) { this.replicationTimeoutMs = replicationTimeoutMs; }

    public long getHintedHandoffIntervalMs() { return hintedHandoffIntervalMs; }
    public void setHintedHandoffIntervalMs(long hintedHandoffIntervalMs) { this.hintedHandoffIntervalMs = hintedHandoffIntervalMs; }

    /**
     * Returns ordered list of node descriptors parsed from "nodeId:port,nodeId:port,..."
     * Order is deterministic and used for coordinator fallback priority.
     */
    public List<NodeDescriptor> getParsedNodes() {
        return Arrays.stream(allNodes.split(","))
                .map(String::trim)
                .map(entry -> {
                    String[] parts = entry.split(":");
                    return new NodeDescriptor(parts[0], Integer.parseInt(parts[1]));
                })
                .toList();
    }

    public record NodeDescriptor(String nodeId, int port) {
        public String baseUrl() {
            return "http://localhost:" + port;
        }
    }
}

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
     *
     * Format supports two forms:
     *   - "node1:8081"           → host defaults to nodeId (works in Docker via DNS)
     *   - "node1|localhost:8081" → explicit host (useful for local dev without Docker)
     *
     * In Docker, container names are resolvable as hostnames on the same network,
     * so "node1:8081" correctly routes to the node1 container on port 8081.
     * Locally (without Docker), all nodes run on localhost with different ports.
     */
    public List<NodeDescriptor> getParsedNodes() {
        return Arrays.stream(allNodes.split(","))
                .map(String::trim)
                .map(entry -> {
                    // Format: "nodeId:port" or "nodeId|host:port"
                    // "node1:8081"           → host = nodeId  (Docker: resolved by DNS)
                    // "node1|localhost:8081" → host = localhost (local dev without Docker)
                    if (entry.contains("|")) {
                        // explicit host: "node1|localhost:8081"
                        String nodeId = entry.split("\\|")[0];
                        String hostPort = entry.split("\\|")[1];
                        String host = hostPort.split(":")[0];
                        int port = Integer.parseInt(hostPort.split(":")[1]);
                        return new NodeDescriptor(nodeId, host, port);
                    } else {
                        // default: use nodeId as host
                        String[] parts = entry.split(":");
                        String nodeId = parts[0];
                        int port = Integer.parseInt(parts[1]);
                        return new NodeDescriptor(nodeId, nodeId, port);
                    }
                })
                .toList();
    }

    public record NodeDescriptor(String nodeId, String host, int port) {
        /**
         * Base URL for HTTP calls to this node.
         *
         * Docker:     host = nodeId (e.g., "node2") → resolved by Docker DNS
         * Local dev:  host = "localhost" when using "nodeId|localhost:port" format
         */
        public String baseUrl() {
            return "http://" + host + ":" + port;
        }
    }
}

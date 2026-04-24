package com.kvstore.replication;

import com.kvstore.model.HintedWrite;
import com.kvstore.model.WalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Stores writes that could not be delivered to a target node because it was down.
 *
 * Hinted Handoff pattern (used by Cassandra, DynamoDB):
 * When a write cannot be replicated to node X, instead of failing the write,
 * the coordinator stores a "hint" — a record saying "deliver this to node X
 * when it comes back online."
 *
 * This allows the system to:
 * 1. Accept writes even when a replica is temporarily down
 * 2. Guarantee the replica eventually receives the write (eventual consistency)
 * 3. Avoid the client having to retry
 *
 * Tradeoff: If the coordinator itself crashes before draining the hint queue,
 * the hints are lost. The rejoining node must then do a full WAL sync from
 * another peer. This is acceptable — hints are a best-effort optimization,
 * not the primary recovery mechanism.
 *
 * Note: In this implementation hints are in-memory only (lost on coordinator crash).
 * A production system would persist hints to disk as well.
 */
@Component
public class HintedHandoffStore {

    private static final Logger log = LoggerFactory.getLogger(HintedHandoffStore.class);

    // nodeId → queue of pending writes for that node
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<HintedWrite>> hints =
            new ConcurrentHashMap<>();

    /**
     * Store a hint for a write that could not be delivered to targetNodeId.
     */
    public void storeHint(String targetNodeId, WalEntry entry) {
        hints.computeIfAbsent(targetNodeId, k -> new ConcurrentLinkedQueue<>())
             .add(new HintedWrite(targetNodeId, entry));
        log.debug("[HintedHandoff] Stored hint for node={} offset={}", targetNodeId, entry.offset());
    }

    /**
     * Returns all pending hints for a given node.
     * Called when the health monitor detects the node has recovered.
     */
    public Queue<HintedWrite> getHints(String targetNodeId) {
        return hints.getOrDefault(targetNodeId, new ConcurrentLinkedQueue<>());
    }

    /**
     * Remove a successfully delivered hint.
     */
    public void removeHint(String targetNodeId, HintedWrite hint) {
        Queue<HintedWrite> queue = hints.get(targetNodeId);
        if (queue != null) {
            queue.remove(hint);
        }
    }

    /**
     * Clear all hints for a node (called after successful full sync).
     */
    public void clearHints(String targetNodeId) {
        hints.remove(targetNodeId);
        log.info("[HintedHandoff] Cleared all hints for node={}", targetNodeId);
    }

    /**
     * Returns true if there are pending hints for the given node.
     */
    public boolean hasHints(String targetNodeId) {
        Queue<HintedWrite> queue = hints.get(targetNodeId);
        return queue != null && !queue.isEmpty();
    }

    /**
     * Returns all node IDs that have pending hints.
     */
    public Set<String> nodesWithHints() {
        return hints.keySet();
    }
}

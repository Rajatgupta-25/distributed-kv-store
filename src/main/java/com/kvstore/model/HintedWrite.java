package com.kvstore.model;

/**
 * A write that could not be delivered to a target node because it was down.
 *
 * Hinted handoff: the coordinator stores the write locally with a hint
 * indicating which node should eventually receive it. When that node
 * recovers, the hint queue is drained and the write is forwarded.
 *
 * This allows the system to remain available (accept writes) even when
 * the target replica is temporarily unreachable, without sacrificing
 * durability — the write is safely stored on the coordinator's disk.
 */
public record HintedWrite(String targetNodeId, WalEntry walEntry) {}

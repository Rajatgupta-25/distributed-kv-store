package com.kvstore.controller;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Debug endpoints for testing failure scenarios locally.
 *
 * NOT for production use — only active when running without a specific profile,
 * or you can restrict it with @Profile("!prod").
 *
 * Allows simulating:
 * - Slow node: adds artificial delay to all replication requests
 * - Node pause: simulates a GC pause or CPU spike
 *
 * Usage:
 *   POST /debug/slow?ms=2000   → make this node respond slowly (2 seconds)
 *   POST /debug/slow?ms=0      → reset to normal speed
 *   GET  /debug/status         → see current delay setting
 */
@RestController
@RequestMapping("/debug")
public class DebugController {

    private static final Logger log = LoggerFactory.getLogger(DebugController.class);

    // Shared delay value — set via endpoint, read by SlowNodeFilter
    public static final AtomicLong ARTIFICIAL_DELAY_MS = new AtomicLong(0);

    /**
     * Set artificial delay for all incoming requests on this node.
     * Simulates a slow node (high latency, not crashed).
     *
     * Example: POST /debug/slow?ms=2000
     */
    @PostMapping("/slow")
    public Map<String, Object> setSlow(@RequestParam long ms) {
        ARTIFICIAL_DELAY_MS.set(ms);
        log.warn("[Debug] Artificial delay set to {}ms on this node", ms);
        return Map.of("delayMs", ms, "status", ms > 0 ? "slow mode ON" : "slow mode OFF");
    }

    /**
     * GET /debug/status — see current delay and node info.
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "artificialDelayMs", ARTIFICIAL_DELAY_MS.get(),
                "slowModeActive", ARTIFICIAL_DELAY_MS.get() > 0
        );
    }
}

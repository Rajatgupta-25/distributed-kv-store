package com.kvstore.filter;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.kvstore.controller.DebugController;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Servlet filter that injects artificial delay into ALL incoming requests
 * when slow mode is enabled via POST /debug/slow?ms=N.
 *
 * This simulates a slow node — the node is alive and will eventually respond,
 * but takes longer than the coordinator's replication timeout.
 *
 * When the coordinator's timeout (500ms by default) fires before this node
 * responds, the coordinator treats this node as unreachable and:
 * 1. Stores a hinted handoff for this node
 * 2. Proceeds with quorum from the other two nodes
 * 3. Returns success to the client
 *
 * This lets you verify the timeout + hinted handoff path without actually
 * killing a node.
 */
@Component
public class SlowNodeFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(SlowNodeFilter.class);

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        long delayMs = DebugController.ARTIFICIAL_DELAY_MS.get();

        if (delayMs > 0) {
            HttpServletRequest req = (HttpServletRequest) request;
            log.warn("[SlowNode] Injecting {}ms delay for {} {}", delayMs, req.getMethod(), req.getRequestURI());
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        chain.doFilter(request, response);
    }
}

package com.kvstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Distributed KV Store node.
 *
 * Each node is an independent Spring Boot application.
 * Run 3 instances with different NODE_ID and NODE_PORT env vars to form a cluster.
 *
 * Example:
 *   NODE_ID=node1 NODE_PORT=8081 java -jar kv-store.jar
 *   NODE_ID=node2 NODE_PORT=8082 java -jar kv-store.jar
 *   NODE_ID=node3 NODE_PORT=8083 java -jar kv-store.jar
 */
@SpringBootApplication
@EnableScheduling
public class KvStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(KvStoreApplication.class, args);
    }
}

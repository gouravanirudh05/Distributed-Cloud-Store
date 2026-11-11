package com.objectstore.controller.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** All tunables under the "store" prefix in application.yml. */
@ConfigurationProperties(prefix = "store")
public record StoreProperties(
        int chunkSize,
        int replicationFactor,
        long heartbeatTimeoutMs,
        long heartbeatCheckIntervalMs,
        long reReplicationGraceMs,
        long maintenanceIntervalMs,
        long scrubIntervalMs,
        long workerConnectTimeoutMs,
        long workerReadTimeoutMs,
        long workerInventoryTimeoutMs,
        long staleOperationTimeoutMs,
        int eventRetention) {

    public StoreProperties {
        if (chunkSize <= 0) throw new IllegalArgumentException("store.chunk-size must be > 0");
        if (replicationFactor < 1) throw new IllegalArgumentException("store.replication-factor must be >= 1");
    }

    public Duration heartbeatTimeout() { return Duration.ofMillis(heartbeatTimeoutMs); }
    public Duration reReplicationGrace() { return Duration.ofMillis(reReplicationGraceMs); }
}

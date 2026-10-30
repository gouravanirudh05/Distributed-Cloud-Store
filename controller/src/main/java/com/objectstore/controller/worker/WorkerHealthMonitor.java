package com.objectstore.controller.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

@Component
public class WorkerHealthMonitor {

    private static final Logger log = LoggerFactory.getLogger(WorkerHealthMonitor.class);

    private final WorkerRepository workers;
    private final Duration timeout;

    public WorkerHealthMonitor(WorkerRepository workers,
                               @Value("${store.heartbeat-timeout-ms}") long timeoutMs) {
        this.workers = workers;
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    @Scheduled(fixedDelay = 2000)
    public void checkWorkers() {
        Instant cutoff = Instant.now().minus(timeout);
        for (WorkerNode node : workers.findByStatus(WorkerStatus.UP)) {
            if (node.getLastHeartbeat().isBefore(cutoff)) {
                node.setStatus(WorkerStatus.DOWN);
                workers.save(node);
                log.warn("Worker {} marked DOWN (no heartbeat since {})", node.getId(), node.getLastHeartbeat());
            }
        }
    }
}

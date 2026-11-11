package com.objectstore.controller.replication;

import com.objectstore.controller.config.StoreProperties;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.object.ObjectService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs all background repair work on ONE thread, so recovery checks, repairs, re-replication and scrubs
 * never race each other. Each tick:
 *   1. verify workers that are RECOVERING and promote them to UP
 *   2. repair replicas reported bad by downloads
 *   3. re-replicate under-replicated chunks / trim over-replicated ones
 *   4. periodic integrity scrub of all UP workers
 *   5. housekeeping (stale uploads/deletes, event log pruning)
 */
@Component
public class MaintenanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private final IntegrityScrubber scrubber;
    private final ReplicationManager replication;
    private final RepairQueue repairQueue;
    private final ObjectService objectService;
    private final EventLog events;
    private final StoreProperties props;

    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "maintenance");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean wakeQueued = new AtomicBoolean(false);
    private Instant lastScrub = Instant.now();
    private Instant lastHousekeeping = Instant.EPOCH;

    public MaintenanceScheduler(IntegrityScrubber scrubber, ReplicationManager replication, RepairQueue repairQueue,
                                ObjectService objectService, EventLog events, StoreProperties props) {
        this.scrubber = scrubber;
        this.replication = replication;
        this.repairQueue = repairQueue;
        this.objectService = objectService;
        this.events = events;
        this.props = props;
        repairQueue.setListener(this::triggerSoon);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        exec.execute(() -> {
            try {
                int n = objectService.cleanupStaleOperations(null);
                if (n > 0) log.info("Cleaned {} object(s) left mid-upload/mid-delete by a previous run", n);
            } catch (Exception e) {
                log.warn("Startup cleanup failed: {}", e.getMessage());
            }
        });
        exec.scheduleWithFixedDelay(this::safeTick, 2000, props.maintenanceIntervalMs(), TimeUnit.MILLISECONDS);
        log.info("Maintenance loop started (every {} ms, re-replication grace {} ms, scrub every {} ms)",
                props.maintenanceIntervalMs(), props.reReplicationGraceMs(), props.scrubIntervalMs());
    }

    @PreDestroy
    public void stop() {
        exec.shutdownNow();
    }

    /** Runs a tick shortly (used when a worker registers or a bad replica is reported). */
    public void triggerSoon() {
        if (wakeQueued.compareAndSet(false, true)) {
            exec.schedule(() -> {
                wakeQueued.set(false);
                safeTick();
            }, 200, TimeUnit.MILLISECONDS);
        }
    }

    /** Runs a full integrity scrub now (on the maintenance thread) and waits for the result. */
    public ScrubReport scrubNow() throws InterruptedException, ExecutionException, TimeoutException {
        return exec.submit(() -> {
            ScrubReport report = scrubber.scrubAll(true);
            lastScrub = Instant.now();
            replication.runPass();
            return report;
        }).get(10, TimeUnit.MINUTES);
    }

    private void safeTick() {
        try {
            tick();
        } catch (Throwable t) {
            log.error("Maintenance tick failed", t);
        }
    }

    private void tick() {
        scrubber.reconcileRecoveringWorkers();
        for (RepairQueue.RepairRequest r : repairQueue.drain()) {
            replication.repairReplica(r.chunkId(), r.workerId(), r.reason());
        }
        replication.runPass();

        Instant now = Instant.now();
        if (Duration.between(lastScrub, now).toMillis() >= props.scrubIntervalMs()) {
            lastScrub = now;
            scrubber.scrubAll(false);
            replication.runPass();
        }
        if (Duration.between(lastHousekeeping, now).toSeconds() >= 60) {
            lastHousekeeping = now;
            objectService.cleanupStaleOperations(Duration.ofMillis(props.staleOperationTimeoutMs()));
            events.prune(props.eventRetention());
        }
    }
}

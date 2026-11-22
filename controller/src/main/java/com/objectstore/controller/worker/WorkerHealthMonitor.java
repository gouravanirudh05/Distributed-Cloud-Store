package com.objectstore.controller.worker;

import com.objectstore.controller.cluster.LeaderElection;
import com.objectstore.controller.config.StoreProperties;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.object.ChunkDao;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Marks a worker DOWN when it has not sent a heartbeat for store.heartbeat-timeout-ms.
 * Only the active controller does this, and only once it has been active for a full timeout: right after a
 * controller failover, heartbeats sent while no controller was active are missing, which is not the workers' fault.
 */
@Component
public class WorkerHealthMonitor {

    private final WorkerRepository workers;
    private final ChunkDao chunks;
    private final EventLog events;
    private final StoreProperties props;
    private final LeaderElection election;

    public WorkerHealthMonitor(WorkerRepository workers, ChunkDao chunks, EventLog events, StoreProperties props,
                               LeaderElection election) {
        this.workers = workers;
        this.chunks = chunks;
        this.events = events;
        this.props = props;
        this.election = election;
    }

    @Scheduled(fixedDelayString = "${store.heartbeat-check-interval-ms:2000}", initialDelay = 5000)
    public void checkWorkers() {
        Instant now = Instant.now();
        Instant cutoff = now.minus(props.heartbeatTimeout());
        Instant activeSince = election.leaderSince();
        if (!election.isLeader() || activeSince == null || activeSince.isAfter(cutoff)) {
            return;
        }
        for (WorkerNode node : workers.findAll()) {
            if (node.getStatus() == WorkerStatus.DOWN || node.getLastHeartbeat() == null
                    || !node.getLastHeartbeat().isBefore(cutoff)) {
                continue;
            }
            if (workers.markDownIfStale(node.getId(), cutoff, now) == 1) {
                long replicas = chunks.replicaCountOnWorker(node.getId());
                events.worker(EventType.WORKER_DOWN, node.getId(), String.format(
                        "Worker %s marked DOWN: no heartbeat for %ds. %d chunk copies on it are unavailable; reads use "
                                + "the other copies. Its chunks will be re-replicated at %s if it has not returned.",
                        node.getId(), Duration.between(node.getLastHeartbeat(), now).toSeconds(), replicas,
                        now.plus(props.reReplicationGrace()).truncatedTo(java.time.temporal.ChronoUnit.SECONDS)));
            }
        }
    }
}

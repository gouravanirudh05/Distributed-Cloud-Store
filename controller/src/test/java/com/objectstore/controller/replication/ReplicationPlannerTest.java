package com.objectstore.controller.replication;

import com.objectstore.controller.object.ChunkDao.ReplicaRow;
import com.objectstore.controller.replication.ReplicationPlanner.*;
import com.objectstore.controller.worker.WorkerStatus;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationPlannerTest {

    private static final Instant NOW = Instant.parse("2026-11-10T12:00:00Z");
    private static final Duration GRACE = Duration.ofSeconds(30);

    private static ReplicaRow replica(long id, String worker, WorkerStatus status, Instant created, Instant downSince) {
        return new ReplicaRow(id, "chunk", worker, created, status, "http://" + worker, downSince);
    }

    private static ReplicaRow up(long id, String worker) {
        return replica(id, worker, WorkerStatus.UP, NOW.minusSeconds(1000 - id), null);
    }

    @Test
    void exactlyRfLiveCopiesIsHealthy() {
        assertInstanceOf(Healthy.class, ReplicationPlanner.decide(List.of(up(1, "w1"), up(2, "w2")), 2, GRACE, NOW));
    }

    @Test
    void recentlyDownWorkerMeansWaitForGracePeriod() {
        Instant down = NOW.minusSeconds(10);
        Decision d = ReplicationPlanner.decide(List.of(up(1, "w1"),
                replica(2, "w2", WorkerStatus.DOWN, NOW.minusSeconds(999), down)), 2, GRACE, NOW);
        Wait w = assertInstanceOf(Wait.class, d);
        assertEquals(down.plus(GRACE), w.until());
        assertEquals(List.of("w2"), w.waitingFor());
    }

    @Test
    void workerDownLongerThanGraceTriggersReReplicationFromLiveCopy() {
        Decision d = ReplicationPlanner.decide(List.of(up(1, "w1"),
                replica(2, "w2", WorkerStatus.DOWN, NOW.minusSeconds(999), NOW.minusSeconds(31))), 2, GRACE, NOW);
        Replicate r = assertInstanceOf(Replicate.class, d);
        assertEquals(1, r.copiesNeeded());
        assertEquals("w1", r.sources().get(0).workerId());
    }

    @Test
    void recoveringWorkerMeansWait() {
        Decision d = ReplicationPlanner.decide(List.of(up(1, "w1"),
                replica(2, "w2", WorkerStatus.RECOVERING, NOW.minusSeconds(999), NOW.minusSeconds(300))), 2, GRACE, NOW);
        assertInstanceOf(Wait.class, d);
    }

    @Test
    void degradedUploadWithNoDownWorkersIsReplicatedImmediately() {
        Replicate r = assertInstanceOf(Replicate.class, ReplicationPlanner.decide(List.of(up(1, "w1")), 2, GRACE, NOW));
        assertEquals(1, r.copiesNeeded());
    }

    @Test
    void noLiveCopyIsUnavailable() {
        Decision d = ReplicationPlanner.decide(List.of(
                replica(1, "w1", WorkerStatus.DOWN, NOW, NOW.minusSeconds(100)),
                replica(2, "w2", WorkerStatus.DOWN, NOW, NOW.minusSeconds(100))), 2, GRACE, NOW);
        assertInstanceOf(Unavailable.class, d);
    }

    @Test
    void surplusCopiesAreTrimmedNewestFirst() {
        // w1, w2 are the original copies; w3 was added while w2 was down. w2 is back -> remove w3's temporary copy.
        ReplicaRow w1 = replica(1, "w1", WorkerStatus.UP, NOW.minusSeconds(600), null);
        ReplicaRow w2 = replica(2, "w2", WorkerStatus.UP, NOW.minusSeconds(600), null);
        ReplicaRow w3 = replica(9, "w3", WorkerStatus.UP, NOW.minusSeconds(60), null);
        Trim t = assertInstanceOf(Trim.class, ReplicationPlanner.decide(List.of(w1, w2, w3), 2, GRACE, NOW));
        assertEquals(List.of(w3), t.remove());
    }
}

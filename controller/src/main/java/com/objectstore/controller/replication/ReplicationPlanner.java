package com.objectstore.controller.replication;

import com.objectstore.controller.object.ChunkDao.ReplicaRow;
import com.objectstore.controller.worker.WorkerStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure decision logic: given a chunk's replicas and the state of the workers holding them, what should
 * the controller do? "Healthy" copies are copies on UP workers.
 *
 *  healthy == RF                      -> nothing
 *  healthy  > RF                      -> trim the newest surplus copies (e.g. temporary copies made while a
 *                                        worker was down, once that worker is back and verified)
 *  healthy == 0                       -> unavailable; nothing to copy from
 *  a holder is RECOVERING, or DOWN for less than the grace period
 *                                     -> wait (a short outage should not trigger a full copy)
 *  otherwise                          -> copy the chunk to (RF - healthy) more workers
 */
public final class ReplicationPlanner {

    public sealed interface Decision permits Healthy, Trim, Replicate, Wait, Unavailable {}
    public record Healthy() implements Decision {}
    public record Trim(List<ReplicaRow> remove) implements Decision {}
    public record Replicate(int copiesNeeded, List<ReplicaRow> sources) implements Decision {}
    public record Wait(Instant until, List<String> waitingFor) implements Decision {}
    public record Unavailable() implements Decision {}

    private ReplicationPlanner() {}

    public static Decision decide(List<ReplicaRow> replicas, int rf, Duration grace, Instant now) {
        List<ReplicaRow> up = replicas.stream().filter(ReplicaRow::isUp).toList();
        if (up.size() == rf) return new Healthy();
        if (up.size() > rf) {
            List<ReplicaRow> newestFirst = up.stream()
                    .sorted(Comparator.comparing(ReplicaRow::createdAt).thenComparing(ReplicaRow::id).reversed())
                    .toList();
            return new Trim(newestFirst.subList(0, up.size() - rf));
        }
        if (up.isEmpty()) return new Unavailable();

        Instant until = null;
        List<String> waitingFor = new ArrayList<>();
        for (ReplicaRow r : replicas) {
            if (r.isUp()) continue;
            Instant candidate = null;
            if (r.workerStatus() == WorkerStatus.RECOVERING) {
                candidate = now;
            } else if (r.workerStatus() == WorkerStatus.DOWN && r.workerDownSince() != null
                    && r.workerDownSince().plus(grace).isAfter(now)) {
                candidate = r.workerDownSince().plus(grace);
            }
            if (candidate != null) {
                waitingFor.add(r.workerId());
                if (until == null || candidate.isAfter(until)) until = candidate;
            }
        }
        if (!waitingFor.isEmpty()) return new Wait(until, waitingFor);
        return new Replicate(rf - up.size(), up);
    }
}

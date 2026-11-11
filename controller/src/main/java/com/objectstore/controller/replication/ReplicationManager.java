package com.objectstore.controller.replication;

import com.objectstore.controller.config.StoreProperties;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.object.ChunkDao;
import com.objectstore.controller.object.ChunkDao.ChunkRow;
import com.objectstore.controller.object.ChunkDao.ReplicaRow;
import com.objectstore.controller.object.ChunkPlacementService;
import com.objectstore.controller.object.ChunkReader;
import com.objectstore.controller.replication.ReplicationPlanner.*;
import com.objectstore.controller.worker.WorkerClientService;
import com.objectstore.controller.worker.WorkerNode;
import com.objectstore.controller.worker.WorkerRepository;
import com.objectstore.controller.worker.WorkerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Keeps every chunk at exactly replication-factor live copies, and repairs individual bad replicas.
 * Only ever called from the single maintenance thread, so it needs no locking of its own.
 */
@Service
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    public enum RepairOutcome { NOT_NEEDED, REPAIRED, DROPPED, FAILED }

    private record PassContext(Map<String, WorkerNode> workers, List<String> upIds, Map<String, Long> load,
                               Instant now) {}

    private final ChunkDao chunks;
    private final WorkerRepository workers;
    private final WorkerClientService workerClient;
    private final ChunkReader reader;
    private final ChunkPlacementService placement;
    private final EventLog events;
    private final StoreProperties props;
    private final RepairQueue repairQueue;

    private final Set<String> reportedWaiting = new HashSet<>();
    private final Set<String> reportedUnavailable = new HashSet<>();
    private final Set<String> reportedNoTarget = new HashSet<>();
    private boolean imbalanceSeen = false;
    private int rotation = 0;

    public ReplicationManager(ChunkDao chunks, WorkerRepository workers, WorkerClientService workerClient,
                              ChunkReader reader, ChunkPlacementService placement, EventLog events,
                              StoreProperties props, RepairQueue repairQueue) {
        this.chunks = chunks;
        this.workers = workers;
        this.workerClient = workerClient;
        this.reader = reader;
        this.placement = placement;
        this.events = events;
        this.props = props;
        this.repairQueue = repairQueue;
    }

    /** One pass over all chunks whose live-copy count differs from RF. */
    public void runPass() {
        int rf = props.replicationFactor();
        List<String> ids = chunks.chunkIdsNeedingAttention(rf);
        if (ids.isEmpty()) {
            if (imbalanceSeen) {
                events.record(EventType.REPLICATION_RESTORED, null, null, null,
                        "Replication factor restored: every chunk has exactly " + rf + " live copies");
                imbalanceSeen = false;
            }
            reportedWaiting.clear();
            reportedUnavailable.clear();
            reportedNoTarget.clear();
            return;
        }
        imbalanceSeen = true;

        Map<String, WorkerNode> all = workers.findAll().stream()
                .collect(Collectors.toMap(WorkerNode::getId, Function.identity()));
        List<String> upIds = all.values().stream().filter(WorkerNode::isUp).map(WorkerNode::getId).sorted().toList();
        PassContext ctx = new PassContext(all, upIds, new HashMap<>(chunks.replicaCountsByWorker()), Instant.now());

        int replicated = 0, trimmed = 0;
        int newlyWaiting = 0;
        Instant waitUntil = null;
        Set<String> waitingFor = new TreeSet<>();

        for (String chunkId : ids) {
            Optional<ChunkRow> chunk = chunks.findChunk(chunkId);
            if (chunk.isEmpty()) continue;
            List<ReplicaRow> replicas = chunks.replicasOfChunk(chunkId);
            Decision d = ReplicationPlanner.decide(replicas, rf, props.reReplicationGrace(), ctx.now());
            switch (d) {
                case Healthy h -> { }
                case Trim t -> trimmed += trim(chunk.get(), replicas, t.remove(), ctx);
                case Wait w -> {
                    if (reportedWaiting.add(chunkId)) {
                        newlyWaiting++;
                        waitingFor.addAll(w.waitingFor());
                        if (waitUntil == null || w.until().isAfter(waitUntil)) waitUntil = w.until();
                    }
                }
                case Unavailable u -> {
                    if (reportedUnavailable.add(chunkId)) {
                        events.chunk(EventType.CHUNK_UNAVAILABLE, null, chunkId, String.format(
                                "%s has no copy on any live worker (copies on: %s); it is unreadable until one returns",
                                capital(chunk.get().label()), describe(replicas)));
                    }
                }
                case Replicate r -> replicated += reReplicate(chunk.get(), replicas, r, ctx);
            }
        }
        if (newlyWaiting > 0) {
            events.record(EventType.CHUNK_UNDER_REPLICATED, null, null, null, String.format(
                    "%d chunk(s) are under-replicated (fewer than %d live copies) because %s %s unavailable; "
                            + "they are still readable from the remaining copy. Re-replication starts at %s unless %s back.",
                    newlyWaiting, rf, String.join(", ", waitingFor), waitingFor.size() == 1 ? "is" : "are",
                    seconds(waitUntil), waitingFor.size() == 1 ? "it comes" : "they come"));
        }
        if (replicated + trimmed > 0) {
            log.info("Replication pass: {} copies created, {} surplus copies removed", replicated, trimmed);
        }
    }

    private int reReplicate(ChunkRow chunk, List<ReplicaRow> replicas, Replicate r, PassContext ctx) {
        Set<String> holders = replicas.stream().map(ReplicaRow::workerId).collect(Collectors.toSet());
        List<String> candidates = ctx.upIds().stream().filter(id -> !holders.contains(id)).toList();
        List<String> targets = placement.choose(candidates, ctx.load(), r.copiesNeeded(), rotation++);
        if (targets.isEmpty()) {
            if (reportedNoTarget.add(chunk.id())) {
                events.chunk(EventType.CHUNK_UNDER_REPLICATED, null, chunk.id(), String.format(
                        "%s has %d live cop%s but no other live worker can take another copy; will retry",
                        capital(chunk.label()), r.sources().size(), r.sources().size() == 1 ? "y" : "ies"));
            }
            return 0;
        }
        Optional<ChunkReader.ReadResult> source = reader.readVerified(chunk, r.sources(), "re-replication");
        if (source.isEmpty()) return 0;

        int done = 0;
        for (String target : targets) {
            WorkerNode w = ctx.workers().get(target);
            try {
                workerClient.putChunk(w.getUrl(), chunk.id(), source.get().data(), chunk.checksum());
                chunks.insertReplica(chunk.id(), target, Instant.now());
                ctx.load().merge(target, 1L, Long::sum);
                done++;
                events.chunk(EventType.CHUNK_RE_REPLICATED, target, chunk.id(), String.format(
                        "Re-replicated %s: copied from %s to %s (live copies %d -> %d; lost copy was on %s)",
                        chunk.label(), source.get().servedBy(), target, r.sources().size(),
                        r.sources().size() + done, describeNotUp(replicas)));
            } catch (Exception e) {
                log.warn("Re-replication of {} to {} failed: {}", chunk.id(), target, e.getMessage());
            }
        }
        return done;
    }

    private int trim(ChunkRow chunk, List<ReplicaRow> replicas, List<ReplicaRow> remove, PassContext ctx) {
        int liveBefore = (int) replicas.stream().filter(ReplicaRow::isUp).count();
        int done = 0;
        for (ReplicaRow r : remove) {
            workerClient.deleteChunk(r.workerUrl(), chunk.id()); // best effort; a leftover file is cleaned by the scrubber
            chunks.deleteReplica(chunk.id(), r.workerId());
            ctx.load().merge(r.workerId(), -1L, Long::sum);
            done++;
            events.chunk(EventType.CHUNK_SURPLUS_REMOVED, r.workerId(), chunk.id(), String.format(
                    "%s had %d live copies (> RF %d); removed the newest (temporary) copy on %s",
                    capital(chunk.label()), liveBefore, props.replicationFactor(), r.workerId()));
        }
        return done;
    }

    /**
     * Fixes one bad (corrupted or missing) replica. If enough healthy copies exist elsewhere the bad replica
     * is simply dropped; otherwise it is rewritten in place from a verified copy.
     */
    public RepairOutcome repairReplica(String chunkId, String workerId, String reason) {
        if (repairQueue.isSuppressed(chunkId, workerId)) return RepairOutcome.FAILED; // already known: no valid source
        Optional<ChunkRow> chunkOpt = chunks.findChunk(chunkId);
        if (chunkOpt.isEmpty()) return RepairOutcome.NOT_NEEDED;   // object deleted meanwhile
        ChunkRow chunk = chunkOpt.get();
        List<ReplicaRow> replicas = chunks.replicasOfChunk(chunkId);
        ReplicaRow bad = replicas.stream().filter(r -> r.workerId().equals(workerId)).findFirst().orElse(null);
        if (bad == null || bad.workerStatus() == WorkerStatus.DOWN) return RepairOutcome.NOT_NEEDED;

        List<ReplicaRow> others = replicas.stream().filter(r -> !r.workerId().equals(workerId) && r.isUp()).toList();
        if (others.size() >= props.replicationFactor()) {
            workerClient.deleteChunk(bad.workerUrl(), chunkId);
            chunks.deleteReplica(chunkId, workerId);
            events.chunk(EventType.CHUNK_REPLICA_DROPPED, workerId, chunkId, String.format(
                    "Dropped %s copy of %s on %s: %d healthy copies already exist elsewhere",
                    reason, chunk.label(), workerId, others.size()));
            repairQueue.clearUnrepairable(chunkId, workerId);
            return RepairOutcome.DROPPED;
        }

        Optional<ChunkReader.ReadResult> good = reader.readVerified(chunk, others, "repair");
        if (good.isEmpty()) {
            repairQueue.markUnrepairable(chunkId, workerId);
            events.chunk(EventType.CHUNK_LOST, workerId, chunkId, String.format(
                    "Cannot repair %s copy of %s on %s: no valid copy is available on any live worker "
                            + "(reads of this chunk fail with 503 instead of returning bad data)",
                    reason, chunk.label(), workerId));
            return RepairOutcome.FAILED;
        }
        try {
            workerClient.putChunk(bad.workerUrl(), chunkId, good.get().data(), chunk.checksum());
            events.chunk(EventType.CHUNK_REPAIRED, workerId, chunkId, String.format(
                    "Repaired %s copy of %s on %s by rewriting it from the verified copy on %s",
                    reason, chunk.label(), workerId, good.get().servedBy()));
            repairQueue.clearUnrepairable(chunkId, workerId);
            return RepairOutcome.REPAIRED;
        } catch (Exception e) {
            chunks.deleteReplica(chunkId, workerId);
            events.chunk(EventType.CHUNK_REPLICA_DROPPED, workerId, chunkId, String.format(
                    "Could not rewrite %s copy of %s on %s (%s); removed it so a new copy is placed elsewhere",
                    reason, chunk.label(), workerId, e.getMessage()));
            return RepairOutcome.DROPPED;
        }
    }

    private static String capital(String s) {
        return s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    private static Instant seconds(Instant t) {
        return t == null ? null : t.truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    }

    private static String describe(List<ReplicaRow> replicas) {
        if (replicas.isEmpty()) return "none";
        return replicas.stream().map(r -> r.workerId() + " (" + r.workerStatus() + ")").collect(Collectors.joining(", "));
    }

    private static String describeNotUp(List<ReplicaRow> replicas) {
        String s = replicas.stream().filter(r -> !r.isUp()).map(ReplicaRow::workerId).collect(Collectors.joining(", "));
        return s.isEmpty() ? "a removed replica" : s;
    }
}

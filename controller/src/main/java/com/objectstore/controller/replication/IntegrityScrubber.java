package com.objectstore.controller.replication;

import com.objectstore.controller.config.StoreProperties;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.object.*;
import com.objectstore.controller.object.ChunkDao.ExpectedReplica;
import com.objectstore.controller.object.ChunkDao.ReplicaRow;
import com.objectstore.controller.worker.*;
import com.objectstore.controller.worker.WorkerClientService.Inventory;
import com.objectstore.controller.worker.WorkerClientService.InventoryItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Compares what a worker physically stores (its inventory, with every chunk re-hashed on the worker) with
 * what the metadata says it should store, and fixes the differences:
 *   corrupted / missing replica -> repaired from a valid copy (or dropped if enough copies exist elsewhere)
 *   file whose object is gone   -> orphan, deleted
 *   unaccounted copy of a live chunk -> adopted if the chunk is short of copies, otherwise deleted
 * Runs for a worker before it is promoted from RECOVERING to UP, and periodically for all UP workers.
 */
@Service
public class IntegrityScrubber {

    private static final Logger log = LoggerFactory.getLogger(IntegrityScrubber.class);

    private final ChunkDao chunks;
    private final ObjectRepository objects;
    private final WorkerRepository workers;
    private final WorkerClientService workerClient;
    private final ReplicationManager replication;
    private final EventLog events;
    private final StoreProperties props;

    public IntegrityScrubber(ChunkDao chunks, ObjectRepository objects, WorkerRepository workers,
                             WorkerClientService workerClient, ReplicationManager replication, EventLog events,
                             StoreProperties props) {
        this.chunks = chunks;
        this.objects = objects;
        this.workers = workers;
        this.workerClient = workerClient;
        this.replication = replication;
        this.events = events;
        this.props = props;
    }

    /** Verifies RECOVERING workers and promotes them to UP. */
    public void reconcileRecoveringWorkers() {
        for (WorkerNode w : workers.findByStatus(WorkerStatus.RECOVERING)) {
            ScrubReport.WorkerResult r = scrubWorker(w, "recovery check");
            if (!r.reachable()) continue; // retry next tick; if heartbeats stop it becomes DOWN
            if (workers.markUpIfRecovering(w.getId()) == 1) {
                events.worker(EventType.WORKER_UP, w.getId(), String.format(
                        "Worker %s is UP: verified its stored chunks before serving traffic "
                                + "(%d valid, %d corrupted, %d missing, %d repaired, %d dropped, %d orphan/stale removed)",
                        w.getId(), r.verified(), r.corrupted(), r.missing(), r.repaired(), r.dropped(),
                        r.orphansRemoved() + r.staleCopiesRemoved()));
            }
        }
    }

    public ScrubReport scrubAll(boolean manual) {
        Instant start = Instant.now();
        List<ScrubReport.WorkerResult> results = new ArrayList<>();
        for (WorkerNode w : workers.findByStatus(WorkerStatus.UP)) {
            results.add(scrubWorker(w, manual ? "manual integrity scan" : "scheduled integrity scan"));
        }
        ScrubReport report = new ScrubReport(start, java.time.Duration.between(start, Instant.now()).toMillis(),
                results.size(),
                sum(results, ScrubReport.WorkerResult::verified),
                sum(results, ScrubReport.WorkerResult::corrupted),
                sum(results, ScrubReport.WorkerResult::missing),
                sum(results, ScrubReport.WorkerResult::repaired),
                sum(results, ScrubReport.WorkerResult::dropped),
                sum(results, ScrubReport.WorkerResult::unrepairable),
                sum(results, ScrubReport.WorkerResult::orphansRemoved),
                sum(results, ScrubReport.WorkerResult::staleCopiesRemoved),
                sum(results, ScrubReport.WorkerResult::adopted),
                results);
        boolean problems = results.stream().anyMatch(ScrubReport.WorkerResult::foundProblems);
        if (manual || problems) {
            events.record(EventType.SCRUB_COMPLETED, null, null, null, String.format(
                    "%s integrity scan of %d worker(s): %d chunk copies verified, %d corrupted, %d missing, "
                            + "%d repaired, %d dropped, %d unrepairable, %d orphans removed",
                    manual ? "Manual" : "Scheduled", report.workersScanned(), report.chunksVerified(),
                    report.corruptedFound(), report.missingFound(), report.repaired(), report.dropped(),
                    report.unrepairable(), report.orphansRemoved()));
        }
        return report;
    }

    public ScrubReport.WorkerResult scrubWorker(WorkerNode w, String trigger) {
        // expected first, then inventory: anything listed as expected was written before its row existed
        List<ExpectedReplica> expectedList = chunks.expectedOnWorker(w.getId());
        Inventory inv;
        try {
            inv = workerClient.inventory(w.getUrl(), w.getId());
        } catch (Exception e) {
            log.warn("{} of {} skipped: {}", trigger, w.getId(), e.getMessage());
            return new ScrubReport.WorkerResult(w.getId(), false, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }
        Map<String, InventoryItem> actual = inv.chunks().stream()
                .collect(Collectors.toMap(InventoryItem::chunkId, Function.identity(), (a, b) -> a));
        Set<String> expectedIds = expectedList.stream().map(ExpectedReplica::chunkId).collect(Collectors.toSet());

        int verified = 0, corrupted = 0, missing = 0, repaired = 0, dropped = 0, failed = 0;
        int orphans = 0, stale = 0, adopted = 0;

        for (ExpectedReplica e : expectedList) {
            if (e.objectStatus() != ObjectStatus.AVAILABLE) continue;
            InventoryItem item = actual.get(e.chunkId());
            String reason;
            if (item == null) {
                missing++;
                reason = "missing";
                events.chunk(EventType.CHUNK_MISSING, w.getId(), e.chunkId(), String.format(
                        "Copy of chunk %d of '%s' is missing on %s (found by %s)",
                        e.index(), e.objectName(), w.getId(), trigger));
            } else if (!e.checksum().equalsIgnoreCase(item.checksum())) {
                corrupted++;
                reason = "corrupted";
                events.chunk(EventType.CHUNK_CORRUPTED, w.getId(), e.chunkId(), String.format(
                        "Corrupted copy of chunk %d of '%s' on %s found by %s: expected sha256 %s, stored data hashes to %s",
                        e.index(), e.objectName(), w.getId(), trigger, Checksums.shortHash(e.checksum()),
                        Checksums.shortHash(item.checksum())));
            } else {
                verified++;
                continue;
            }
            switch (replication.repairReplica(e.chunkId(), w.getId(), reason)) {
                case REPAIRED -> repaired++;
                case DROPPED -> dropped++;
                case FAILED -> failed++;
                case NOT_NEEDED -> { }
            }
        }

        for (InventoryItem item : inv.chunks()) {
            if (expectedIds.contains(item.chunkId())) continue;
            switch (handleUnexpected(w, item)) {
                case ORPHAN -> orphans++;
                case STALE -> stale++;
                case ADOPTED -> adopted++;
                case SKIPPED -> { }
            }
        }
        return new ScrubReport.WorkerResult(w.getId(), true, verified, corrupted, missing, repaired, dropped, failed,
                orphans, stale, adopted);
    }

    private enum Unexpected { ORPHAN, STALE, ADOPTED, SKIPPED }

    private Unexpected handleUnexpected(WorkerNode w, InventoryItem item) {
        String objectId = ChunkIds.objectIdOf(item.chunkId());
        Optional<ObjectStatus> status = objectId == null ? Optional.empty() : objects.findStatusById(objectId);
        if (status.isPresent() && status.get() != ObjectStatus.AVAILABLE) {
            return Unexpected.SKIPPED; // upload or delete in flight
        }
        Optional<ChunkDao.ChunkRow> chunk = status.isPresent() ? chunks.findChunk(item.chunkId()) : Optional.empty();
        if (chunk.isEmpty()) {
            workerClient.deleteChunk(w.getUrl(), item.chunkId());
            events.chunk(EventType.ORPHAN_REMOVED, w.getId(), item.chunkId(), String.format(
                    "Removed orphan chunk %s from %s (its object no longer exists)", item.chunkId(), w.getId()));
            return Unexpected.ORPHAN;
        }
        List<ReplicaRow> replicas = chunks.replicasOfChunk(item.chunkId());
        if (replicas.stream().anyMatch(r -> r.workerId().equals(w.getId()))) {
            return Unexpected.SKIPPED; // became expected while we were scanning
        }
        long healthy = replicas.stream().filter(ReplicaRow::isUp).count();
        if (healthy < props.replicationFactor() && chunk.get().checksum().equalsIgnoreCase(item.checksum())) {
            chunks.insertReplica(item.chunkId(), w.getId(), Instant.now());
            events.chunk(EventType.REPLICA_ADOPTED, w.getId(), item.chunkId(), String.format(
                    "Adopted valid untracked copy of chunk %d on %s (chunk had only %d live copies)",
                    chunk.get().index(), w.getId(), healthy));
            return Unexpected.ADOPTED;
        }
        workerClient.deleteChunk(w.getUrl(), item.chunkId());
        events.chunk(EventType.STALE_COPY_REMOVED, w.getId(), item.chunkId(), String.format(
                "Removed unneeded copy of chunk %d on %s (chunk already has %d live copies)",
                chunk.get().index(), w.getId(), healthy));
        return Unexpected.STALE;
    }

    private static int sum(List<ScrubReport.WorkerResult> rs, java.util.function.ToIntFunction<ScrubReport.WorkerResult> f) {
        return rs.stream().mapToInt(f).sum();
    }
}

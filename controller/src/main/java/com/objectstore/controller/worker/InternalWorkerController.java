package com.objectstore.controller.worker;

import com.objectstore.controller.api.ApiException;
import com.objectstore.controller.api.ApiModels.ExpectedChunk;
import com.objectstore.controller.api.ApiModels.WorkerChunksView;
import com.objectstore.controller.cluster.ClusterIdentity;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.object.ChunkDao;
import com.objectstore.controller.object.ObjectStatus;
import com.objectstore.controller.replication.MaintenanceScheduler;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Internal API hosted by the controller (called by workers):
 *   POST /internal/workers/register             a worker announces itself (on every start)
 *   POST /internal/workers/heartbeat            periodic liveness signal + storage stats
 *   GET  /internal/workers                      registry
 *   GET  /internal/workers/{workerId}/chunks    chunks the metadata says this worker stores
 *        (the worker hosts the same path, answering from its disk instead)
 */
@RestController
@RequestMapping("/internal/workers")
public class InternalWorkerController {

    private static final Pattern WORKER_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    public record RegisterRequest(String workerId, String url, Long chunkCount, Long usedBytes, Long freeBytes,
                                  String clusterId) {}
    public record HeartbeatRequest(String workerId, String url, Long chunkCount, Long usedBytes, Long freeBytes,
                                   String clusterId) {}

    private final WorkerRepository workers;
    private final ChunkDao chunks;
    private final EventLog events;
    private final MaintenanceScheduler maintenance;
    private final ClusterIdentity cluster;
    /** worker|clusterId pairs already reported as rejected (the worker retries every few seconds). */
    private final Set<String> rejectionsReported = ConcurrentHashMap.newKeySet();

    public InternalWorkerController(WorkerRepository workers, ChunkDao chunks, EventLog events,
                                    MaintenanceScheduler maintenance, ClusterIdentity cluster) {
        this.workers = workers;
        this.chunks = chunks;
        this.events = events;
        this.maintenance = maintenance;
        this.cluster = cluster;
    }

    @PostMapping("/register")
    public ResponseEntity<Map<String, Object>> register(@RequestBody RegisterRequest req) {
        validate(req.workerId(), req.url());
        ResponseEntity<Map<String, Object>> foreign = rejectForeign(req.workerId(), req.clusterId(), orZero(req.chunkCount()));
        if (foreign != null) return foreign;
        Optional<WorkerNode> before = workers.findById(req.workerId());
        workers.upsertRegistration(req.workerId(), req.url(), Instant.now(),
                orZero(req.chunkCount()), orZero(req.usedBytes()), orMinusOne(req.freeBytes()));
        events.worker(EventType.WORKER_REGISTERED, req.workerId(), before.isEmpty()
                ? String.format("New worker %s registered at %s (%d chunks on disk); verifying before marking UP",
                req.workerId(), req.url(), orZero(req.chunkCount()))
                : String.format("Worker %s (re)started and registered (was %s, %d chunks on disk); verifying its chunks before marking UP",
                req.workerId(), before.get().getStatus(), orZero(req.chunkCount())));
        maintenance.triggerSoon();
        return ResponseEntity.ok(Map.of("registered", true, "workerId", req.workerId(),
                "status", WorkerStatus.RECOVERING.name(), "clusterId", cluster.clusterId()));
    }

    /**
     * A worker that already belongs to a different cluster (its stored cluster id differs from this metadata's)
     * is refused: this metadata does not describe its chunks, so treating them as orphans would delete data.
     * A worker without a cluster id (new, or from before cluster ids existed) is accepted and adopts ours.
     */
    private ResponseEntity<Map<String, Object>> rejectForeign(String workerId, String workerCluster, long chunkCount) {
        String ours = cluster.clusterId();
        if (workerCluster == null || workerCluster.isBlank() || workerCluster.equals(ours)) return null;
        if (rejectionsReported.add(workerId + "|" + workerCluster)) {
            events.worker(EventType.WORKER_REJECTED, workerId, String.format(
                    "Worker %s refused: its %d stored chunks belong to cluster %s, but this metadata is cluster %s "
                            + "(empty, re-created or wrong database?). Its data is left untouched.",
                    workerId, chunkCount, workerCluster, ours));
        }
        return ResponseEntity.status(409).body(Map.of("error", "CLUSTER_ID_MISMATCH",
                "workerClusterId", workerCluster, "controllerClusterId", ours));
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Map<String, Object>> heartbeat(@RequestBody HeartbeatRequest req) {
        if (req.workerId() == null || req.workerId().isBlank()) {
            throw ApiException.badRequest("'workerId' is required");
        }
        ResponseEntity<Map<String, Object>> foreign = rejectForeign(req.workerId(), req.clusterId(), orZero(req.chunkCount()));
        if (foreign != null) return foreign;
        Optional<WorkerNode> before = workers.findById(req.workerId());
        if (before.isEmpty()) {
            // unknown worker -> it must register again
            return ResponseEntity.status(404).body(Map.of("error", "unknown worker, please register"));
        }
        workers.recordHeartbeat(req.workerId(), Instant.now(), orZero(req.chunkCount()), orZero(req.usedBytes()),
                orMinusOne(req.freeBytes()));
        if (before.get().getStatus() == WorkerStatus.DOWN) {
            events.worker(EventType.WORKER_RECOVERING, req.workerId(), String.format(
                    "Heartbeat received from %s again after being DOWN since %s; verifying its chunks before marking UP",
                    req.workerId(), before.get().getDownSince() == null ? "?"
                            : before.get().getDownSince().truncatedTo(java.time.temporal.ChronoUnit.SECONDS)));
            maintenance.triggerSoon();
        }
        return ResponseEntity.ok(Map.of("ok", true, "clusterId", cluster.clusterId()));
    }

    @GetMapping
    public List<WorkerNode> list() {
        return workers.findAllByOrderByIdAsc();
    }

    @GetMapping("/{workerId}/chunks")
    public WorkerChunksView chunksOf(@PathVariable String workerId) {
        WorkerNode w = workers.findById(workerId)
                .orElseThrow(() -> ApiException.notFound("WORKER_NOT_FOUND", "Worker '" + workerId + "' is not registered"));
        List<ExpectedChunk> list = chunks.expectedOnWorker(workerId).stream()
                .filter(e -> e.objectStatus() == ObjectStatus.AVAILABLE)
                .map(e -> new ExpectedChunk(e.chunkId(), e.objectId(), e.objectName(), e.index(), e.size(), e.checksum()))
                .toList();
        return new WorkerChunksView(workerId, w.getStatus().name(), "controller-metadata", list.size(), list);
    }

    private static void validate(String workerId, String url) {
        if (workerId == null || !WORKER_ID.matcher(workerId).matches()) {
            throw ApiException.badRequest("'workerId' must be 1-64 characters of [A-Za-z0-9._-]");
        }
        if (url == null || !(url.startsWith("http://") || url.startsWith("https://"))) {
            throw ApiException.badRequest("'url' must be an http(s) URL where the controller can reach the worker");
        }
    }

    private static long orZero(Long v) { return v == null ? 0 : v; }
    private static long orMinusOne(Long v) { return v == null ? -1 : v; }
}

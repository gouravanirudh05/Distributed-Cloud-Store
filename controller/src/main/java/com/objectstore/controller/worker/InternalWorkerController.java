package com.objectstore.controller.worker;

import com.objectstore.controller.api.ApiException;
import com.objectstore.controller.api.ApiModels.ExpectedChunk;
import com.objectstore.controller.api.ApiModels.WorkerChunksView;
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

    public record RegisterRequest(String workerId, String url, Long chunkCount, Long usedBytes, Long freeBytes) {}
    public record HeartbeatRequest(String workerId, String url, Long chunkCount, Long usedBytes, Long freeBytes) {}

    private final WorkerRepository workers;
    private final ChunkDao chunks;
    private final EventLog events;
    private final MaintenanceScheduler maintenance;

    public InternalWorkerController(WorkerRepository workers, ChunkDao chunks, EventLog events,
                                    MaintenanceScheduler maintenance) {
        this.workers = workers;
        this.chunks = chunks;
        this.events = events;
        this.maintenance = maintenance;
    }

    @PostMapping("/register")
    public Map<String, Object> register(@RequestBody RegisterRequest req) {
        validate(req.workerId(), req.url());
        Optional<WorkerNode> before = workers.findById(req.workerId());
        workers.upsertRegistration(req.workerId(), req.url(), Instant.now(),
                orZero(req.chunkCount()), orZero(req.usedBytes()), orMinusOne(req.freeBytes()));
        events.worker(EventType.WORKER_REGISTERED, req.workerId(), before.isEmpty()
                ? String.format("New worker %s registered at %s (%d chunks on disk); verifying before marking UP",
                req.workerId(), req.url(), orZero(req.chunkCount()))
                : String.format("Worker %s (re)started and registered (was %s, %d chunks on disk); verifying its chunks before marking UP",
                req.workerId(), before.get().getStatus(), orZero(req.chunkCount())));
        maintenance.triggerSoon();
        return Map.of("registered", true, "workerId", req.workerId(), "status", WorkerStatus.RECOVERING.name());
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Map<String, Object>> heartbeat(@RequestBody HeartbeatRequest req) {
        if (req.workerId() == null || req.workerId().isBlank()) {
            throw ApiException.badRequest("'workerId' is required");
        }
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
        return ResponseEntity.ok(Map.of("ok", true));
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

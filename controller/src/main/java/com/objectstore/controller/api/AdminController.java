package com.objectstore.controller.api;

import com.objectstore.controller.api.ApiModels.*;
import com.objectstore.controller.config.StoreProperties;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.object.ChunkDao;
import com.objectstore.controller.object.ObjectStatus;
import com.objectstore.controller.replication.MaintenanceScheduler;
import com.objectstore.controller.replication.ScrubReport;
import com.objectstore.controller.worker.WorkerClientService;
import com.objectstore.controller.worker.WorkerNode;
import com.objectstore.controller.worker.WorkerRepository;
import com.objectstore.controller.worker.WorkerStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Observability/operations API used by the CLI, the web UI and the demo (not required by the spec, but
 * needed to *show* replication, failures and integrity handling).
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final WorkerRepository workers;
    private final ChunkDao chunks;
    private final WorkerClientService workerClient;
    private final MaintenanceScheduler maintenance;
    private final EventLog events;
    private final StoreProperties props;

    public AdminController(WorkerRepository workers, ChunkDao chunks, WorkerClientService workerClient,
                           MaintenanceScheduler maintenance, EventLog events, StoreProperties props) {
        this.workers = workers;
        this.chunks = chunks;
        this.workerClient = workerClient;
        this.maintenance = maintenance;
        this.events = events;
        this.props = props;
    }

    @GetMapping("/workers")
    public List<WorkerView> workers() {
        Instant now = Instant.now();
        Map<String, Long> counts = chunks.replicaCountsByWorker();
        return workers.findAllByOrderByIdAsc().stream().map(w -> new WorkerView(
                w.getId(), w.getUrl(), w.getStatus().name(), w.getLastHeartbeat(),
                w.getLastHeartbeat() == null ? null : Duration.between(w.getLastHeartbeat(), now).toSeconds(),
                w.getDownSince(),
                w.getStatus() == WorkerStatus.DOWN && w.getDownSince() != null
                        ? w.getDownSince().plus(props.reReplicationGrace()) : null,
                counts.getOrDefault(w.getId(), 0L), w.getReportedChunkCount(), w.getReportedUsedBytes(),
                w.getReportedFreeBytes())).toList();
    }

    /** Compares the chunks the metadata assigns to a worker with what the worker actually stores (re-hashed). */
    @GetMapping("/workers/{workerId}/chunks")
    public WorkerChunkComparison workerChunks(@PathVariable String workerId) {
        WorkerNode w = workers.findById(workerId)
                .orElseThrow(() -> ApiException.notFound("WORKER_NOT_FOUND", "Worker '" + workerId + "' is not registered"));
        List<ExpectedChunk> expected = chunks.expectedOnWorker(workerId).stream()
                .filter(e -> e.objectStatus() == ObjectStatus.AVAILABLE)
                .map(e -> new ExpectedChunk(e.chunkId(), e.objectId(), e.objectName(), e.index(), e.size(), e.checksum()))
                .toList();
        WorkerClientService.Inventory inv = null;
        String error = null;
        if (w.getStatus() == WorkerStatus.DOWN) {
            error = "worker is DOWN";
        } else {
            try {
                inv = workerClient.inventory(w.getUrl(), workerId);
            } catch (Exception e) {
                error = e.getMessage();
            }
        }
        List<String> missing = new ArrayList<>(), corrupted = new ArrayList<>(), unexpected = new ArrayList<>();
        if (inv != null) {
            Map<String, String> actual = new HashMap<>();
            inv.chunks().forEach(c -> actual.put(c.chunkId(), c.checksum()));
            Set<String> expectedIds = new HashSet<>();
            for (ExpectedChunk e : expected) {
                expectedIds.add(e.chunkId());
                String sum = actual.get(e.chunkId());
                if (sum == null) missing.add(e.chunkId());
                else if (!sum.equalsIgnoreCase(e.checksum())) corrupted.add(e.chunkId());
            }
            actual.keySet().stream().filter(id -> !expectedIds.contains(id)).sorted().forEach(unexpected::add);
        }
        return new WorkerChunkComparison(workerId, w.getStatus().name(), expected.size(),
                inv == null ? null : inv.chunks().size(), error, missing, corrupted, unexpected, expected, inv);
    }

    @GetMapping("/replication")
    public ReplicationReport replication(@RequestParam(defaultValue = "200") int limit) {
        int rf = props.replicationFactor();
        Map<Integer, Long> hist = chunks.healthHistogram();
        long total = 0, healthy = 0, under = 0, over = 0, unavailable = 0;
        for (Map.Entry<Integer, Long> e : hist.entrySet()) {
            total += e.getValue();
            if (e.getKey() == 0) unavailable += e.getValue();
            else if (e.getKey() < rf) under += e.getValue();
            else if (e.getKey() > rf) over += e.getValue();
            else healthy += e.getValue();
        }
        String status = unavailable > 0 ? "UNAVAILABLE" : under > 0 ? "DEGRADED" : over > 0 ? "OVER_REPLICATED" : "HEALTHY";
        Map<String, Long> counts = chunks.replicaCountsByWorker();
        List<WorkerReplicaCount> perWorker = workers.findAllByOrderByIdAsc().stream()
                .map(w -> new WorkerReplicaCount(w.getId(), w.getStatus().name(), counts.getOrDefault(w.getId(), 0L)))
                .toList();
        List<ProblemChunkView> problems = chunks.problemChunks(rf, Math.max(0, Math.min(limit, 1000))).stream()
                .map(p -> new ProblemChunkView(p.chunkId(), p.objectId(), p.objectName(), p.index(), p.healthyReplicas(),
                        p.replicas().stream().map(r -> Map.of("workerId", r.workerId(), "workerStatus", r.workerStatus().name())).toList()))
                .toList();
        return new ReplicationReport(rf, status, total, healthy, under, over, unavailable, perWorker, problems);
    }

    @PostMapping("/scrub")
    public ScrubReport scrub() throws Exception {
        return maintenance.scrubNow();
    }

    @GetMapping("/events")
    public List<EventLog.Event> events(@RequestParam(defaultValue = "50") int limit,
                                       @RequestParam(required = false) String type) {
        if (type != null) {
            try {
                EventType.valueOf(type);
            } catch (IllegalArgumentException e) {
                throw ApiException.badRequest("Unknown event type '" + type + "'; valid: " + Arrays.toString(EventType.values()));
            }
        }
        return events.recent(Math.max(1, Math.min(limit, 1000)), type);
    }
}

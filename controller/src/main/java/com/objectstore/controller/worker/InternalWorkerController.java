package com.objectstore.controller.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/internal/workers")
public class InternalWorkerController {

    private static final Logger log = LoggerFactory.getLogger(InternalWorkerController.class);

    public record RegisterRequest(String workerId, String url) {}
    public record HeartbeatRequest(String workerId) {}

    private final WorkerRepository workers;

    public InternalWorkerController(WorkerRepository workers) {
        this.workers = workers;
    }

    @PostMapping("/register")
    public Map<String, Object> register(@RequestBody RegisterRequest req) {
        WorkerNode node = workers.findById(req.workerId())
                .orElseGet(() -> new WorkerNode(req.workerId(), req.url()));
        node.setUrl(req.url());
        node.setStatus(WorkerStatus.UP);
        node.setLastHeartbeat(Instant.now());
        workers.save(node);
        log.info("Worker registered: {} at {}", req.workerId(), req.url());
        return Map.of("registered", true, "workerId", req.workerId());
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Map<String, Object>> heartbeat(@RequestBody HeartbeatRequest req) {
        var found = workers.findById(req.workerId());
        if (found.isEmpty()) {
            // unknown worker -> tell it to register again
            return ResponseEntity.status(404).body(Map.of("error", "unknown worker"));
        }
        WorkerNode node = found.get();
        if (node.getStatus() == WorkerStatus.DOWN) {
            log.info("Worker {} is back UP", node.getId());
        }
        node.setStatus(WorkerStatus.UP);
        node.setLastHeartbeat(Instant.now());
        workers.save(node);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    @GetMapping
    public List<WorkerNode> list() {
        return workers.findAll();
    }
}

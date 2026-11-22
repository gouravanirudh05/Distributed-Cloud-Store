package com.objectstore.controller.cluster;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

/**
 * Role of this controller instance.
 *   GET /internal/controller/leader   200 if this instance is the active controller, 503 if standby
 *                                     (the gateway's health check, so it routes only to the leader)
 *   GET /api/v1/admin/controller      the active controller's view (through the gateway)
 */
@RestController
public class ControllerStatusController {

    public record ControllerStatus(String instanceId, String role, String activeController, long term,
                                   Instant leaderSince, String election, int leaseTtlSeconds) {}

    private final LeaderElection election;

    public ControllerStatusController(LeaderElection election) {
        this.election = election;
    }

    @GetMapping(LeaderGuardFilter.STATUS_PATH)
    public ResponseEntity<ControllerStatus> leader() {
        return ResponseEntity.status(election.isLeader() ? 200 : 503).body(status());
    }

    @GetMapping("/api/v1/admin/controller")
    public ControllerStatus status() {
        boolean leader = election.isLeader();
        return new ControllerStatus(election.instanceId(), leader ? "ACTIVE" : "STANDBY",
                election.knownLeader().orElse(null), election.term(), election.leaderSince(), election.backend(),
                election.leaseTtlSeconds());
    }
}

package com.objectstore.controller.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A standby controller must not serve requests or accept worker heartbeats: only the active controller does.
 * Standbys answer every /api and /internal request with 503 (the gateway normally never routes to them).
 * {@code /internal/controller/leader} and the actuator stay reachable so health checks can see the role.
 */
@Component
public class LeaderGuardFilter extends OncePerRequestFilter {

    static final String STATUS_PATH = "/internal/controller/leader";

    private final LeaderElection election;
    private final ObjectMapper json = new ObjectMapper();

    public LeaderGuardFilter(LeaderElection election) {
        this.election = election;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return !(path.startsWith("/api/") || path.startsWith("/internal/")) || path.equals(STATUS_PATH);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (election.isLeader()) {
            chain.doFilter(request, response);
            return;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", 503);
        body.put("error", "Service Unavailable");
        body.put("code", "NOT_ACTIVE_CONTROLLER");
        body.put("message", "Controller " + election.instanceId() + " is a standby"
                + election.knownLeader().map(l -> "; the active controller is " + l).orElse(" and no active controller is elected yet")
                + ". Send requests through the gateway.");
        body.put("path", request.getRequestURI());
        response.setStatus(503);
        response.setHeader("Retry-After", "2");
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        json.writeValue(response.getOutputStream(), body);
    }
}

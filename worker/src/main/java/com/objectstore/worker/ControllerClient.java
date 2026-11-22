package com.objectstore.worker;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/**
 * Registers this worker with the controller on startup and then sends a heartbeat (with storage stats)
 * every worker.heartbeat-interval-ms. If the controller answers 404 to a heartbeat it has forgotten us,
 * so we register again.
 *
 * <p>Cluster identity: the first controller this worker joins tells it the cluster id, which is stored next to
 * the chunks ({@code .cluster-id}). It is sent with every register/heartbeat; a controller whose metadata has a
 * different id (an empty or wrong database) refuses the worker (409), and the worker stays out of that cluster,
 * so its chunks are never mistaken for orphans and deleted.
 */
@Component
public class ControllerClient {

    private static final Logger log = LoggerFactory.getLogger(ControllerClient.class);

    private final RestClient http;
    private final String workerId;
    private final String advertisedUrl;
    private final ChunkStore store;
    private volatile boolean registered = false;
    private volatile String clusterId;
    private volatile Instant lastRejectionLog = Instant.EPOCH;

    public ControllerClient(@Value("${worker.controller-url}") String controllerUrl,
                            @Value("${worker.id}") String workerId,
                            @Value("${worker.advertised-url}") String advertisedUrl,
                            ChunkStore store) {
        HttpClient jdk = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdk);
        factory.setReadTimeout(Duration.ofSeconds(5));
        this.http = RestClient.builder().baseUrl(controllerUrl).requestFactory(factory).build();
        this.workerId = workerId;
        this.advertisedUrl = advertisedUrl;
        this.store = store;
        this.clusterId = readClusterId();
    }

    private Path clusterIdFile() {
        return store.getDataDir().resolve(".cluster-id");
    }

    private String readClusterId() {
        try {
            Path f = clusterIdFile();
            return Files.exists(f) ? Files.readString(f, StandardCharsets.UTF_8).trim() : null;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + clusterIdFile(), e);
        }
    }

    private void adoptClusterId(String id) throws IOException {
        if (id == null || id.isBlank() || id.equals(clusterId)) return;
        Path tmp = store.getDataDir().resolve(".cluster-id.part");
        Files.writeString(tmp, id + "\n", StandardCharsets.UTF_8);
        Files.move(tmp, clusterIdFile(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        clusterId = id;
        log.info("Joined storage cluster {}", id);
    }

    private void logRejected(HttpClientErrorException e) {
        if (Instant.now().isAfter(lastRejectionLog.plusSeconds(60))) {
            lastRejectionLog = Instant.now();
            log.error("Controller REFUSED this worker: its chunks belong to cluster {}, but the controller's metadata is a "
                    + "different cluster ({}). Not joining, chunks left untouched. Fix the controller's database, "
                    + "or wipe this worker's volume to join the new cluster.", clusterId, e.getResponseBodyAsString());
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        log.info("Worker {} storing chunks in {}", workerId, store.getDataDir());
        if (!registered) register();
    }

    @Scheduled(fixedDelayString = "${worker.heartbeat-interval-ms}")
    public void heartbeat() {
        if (!registered) {
            register();
            return;
        }
        try {
            Map<?, ?> resp = http.post().uri("/internal/workers/heartbeat")
                    .body(payload())
                    .retrieve().body(Map.class);
            if (clusterId == null && resp != null && resp.get("clusterId") instanceof String id) adoptClusterId(id);
        } catch (HttpClientErrorException.NotFound e) {
            log.warn("Controller doesn't know us, re-registering");
            registered = false;
        } catch (HttpClientErrorException.Conflict e) {
            registered = false;
            logRejected(e);
        } catch (Exception e) {
            log.warn("Heartbeat failed: {}", e.getMessage());
        }
    }

    private synchronized void register() {
        if (registered) return;
        try {
            Map<?, ?> resp = http.post().uri("/internal/workers/register")
                    .body(payload())
                    .retrieve().body(Map.class);
            if (resp != null && resp.get("clusterId") instanceof String id) adoptClusterId(id);
            registered = true;
            log.info("Registered with controller as {} ({})", workerId, advertisedUrl);
        } catch (HttpClientErrorException.Conflict e) {
            logRejected(e);
        } catch (Exception e) {
            log.warn("Register failed (controller not ready?): {}", e.getMessage());
        }
    }

    private Map<String, Object> payload() {
        ChunkStore.Stats s = store.stats();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("workerId", workerId);
        body.put("url", advertisedUrl);
        body.put("chunkCount", s.chunkCount());
        body.put("usedBytes", s.usedBytes());
        body.put("freeBytes", s.freeBytes());
        body.put("clusterId", clusterId);
        return body;
    }
}

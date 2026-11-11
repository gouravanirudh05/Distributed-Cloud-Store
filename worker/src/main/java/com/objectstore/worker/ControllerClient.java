package com.objectstore.worker;

import java.net.http.HttpClient;
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
 */
@Component
public class ControllerClient {

    private static final Logger log = LoggerFactory.getLogger(ControllerClient.class);

    private final RestClient http;
    private final String workerId;
    private final String advertisedUrl;
    private final ChunkStore store;
    private volatile boolean registered = false;

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
            http.post().uri("/internal/workers/heartbeat")
                    .body(payload())
                    .retrieve().toBodilessEntity();
        } catch (HttpClientErrorException.NotFound e) {
            log.warn("Controller doesn't know us, re-registering");
            registered = false;
        } catch (Exception e) {
            log.warn("Heartbeat failed: {}", e.getMessage());
        }
    }

    private synchronized void register() {
        if (registered) return;
        try {
            http.post().uri("/internal/workers/register")
                    .body(payload())
                    .retrieve().toBodilessEntity();
            registered = true;
            log.info("Registered with controller as {} ({})", workerId, advertisedUrl);
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
        return body;
    }
}

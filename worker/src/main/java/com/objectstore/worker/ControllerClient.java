package com.objectstore.worker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class ControllerClient {

    private static final Logger log = LoggerFactory.getLogger(ControllerClient.class);

    private final RestClient http;
    private final String workerId;
    private final String advertisedUrl;
    private final Path dataDir;
    private volatile boolean registered = false;

    public ControllerClient(@Value("${worker.controller-url}") String controllerUrl,
                            @Value("${worker.id}") String workerId,
                            @Value("${worker.advertised-url}") String advertisedUrl,
                            @Value("${worker.data-dir}") String dataDir) {
        this.http = RestClient.create(controllerUrl);
        this.workerId = workerId;
        this.advertisedUrl = advertisedUrl;
        this.dataDir = Path.of(dataDir);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() throws IOException {
        Files.createDirectories(dataDir);
        register();
    }

    @Scheduled(fixedDelayString = "${worker.heartbeat-interval-ms}")
    public void heartbeat() {
        if (!registered) {
            register();
            return;
        }
        try {
            http.post().uri("/internal/workers/heartbeat")
                    .body(Map.of("workerId", workerId))
                    .retrieve().toBodilessEntity();
        } catch (HttpClientErrorException.NotFound e) {
            log.warn("Controller doesn't know us, re-registering");
            registered = false;
        } catch (Exception e) {
            log.warn("Heartbeat failed: {}", e.getMessage());
        }
    }

    private void register() {
        try {
            http.post().uri("/internal/workers/register")
                    .body(Map.of("workerId", workerId, "url", advertisedUrl))
                    .retrieve().toBodilessEntity();
            registered = true;
            log.info("Registered with controller as {}", workerId);
        } catch (Exception e) {
            log.warn("Register failed (controller not ready?): {}", e.getMessage());
        }
    }
}

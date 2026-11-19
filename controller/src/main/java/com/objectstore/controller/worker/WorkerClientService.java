package com.objectstore.controller.worker;

import com.objectstore.controller.config.StoreProperties;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** HTTP client for the internal worker API (/internal/chunks/..., /internal/workers/{id}/chunks). */
@Component
public class WorkerClientService {

    /** Outcome of reading one replica. */
    public sealed interface FetchResult permits Ok, Missing, Unreachable, Failed {}
    public record Ok(byte[] data) implements FetchResult {}
    public record Missing() implements FetchResult {}
    public record Unreachable(String reason) implements FetchResult {}
    public record Failed(String reason) implements FetchResult {}

    public record InventoryItem(String chunkId, long size, String checksum, Instant storedAt) {}
    public record Inventory(String workerId, int chunkCount, long totalBytes, List<InventoryItem> chunks) {}

    private final RestClient http;
    private final RestClient slowHttp;

    public WorkerClientService(RestClient.Builder builder, StoreProperties props) {
        this.http = builder.clone().requestFactory(factory(props.workerConnectTimeoutMs(), props.workerReadTimeoutMs())).build();
        this.slowHttp = builder.clone().requestFactory(factory(props.workerConnectTimeoutMs(), props.workerInventoryTimeoutMs())).build();
    }

    private static JdkClientHttpRequestFactory factory(long connectMs, long readMs) {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(connectMs))
                .build();
        JdkClientHttpRequestFactory f = new JdkClientHttpRequestFactory(client);
        f.setReadTimeout(Duration.ofMillis(readMs));
        return f;
    }

    /** A downstream worker in a write pipeline. */
    public record PipelineTarget(String workerId, String url) {}

    /** Outcome of a replicate command: OK, MISSING (source lacks the chunk), CORRUPT (source copy bad) or FAILED. */
    public record ReplicateResult(String outcome, List<String> storedOn) {}

    private static String pipelineHeader(List<PipelineTarget> targets) {
        return String.join(",", targets.stream().map(t -> t.workerId() + "=" + t.url()).toList());
    }

    /**
     * HDFS-style pipeline write: the chunk is sent only to the first worker (workerUrl), which stores it and
     * forwards it to the downstream targets. Returns ids of all workers that acknowledged storing it.
     */
    @SuppressWarnings("unchecked")
    public List<String> putChunkPipeline(String workerUrl, String chunkId, byte[] data, String sha256,
                                         List<PipelineTarget> downstream) {
        try {
            var req = http.put().uri(workerUrl + "/internal/chunks/{id}", chunkId)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header("X-Expected-Checksum", sha256);
            if (!downstream.isEmpty()) req = req.header("X-Pipeline", pipelineHeader(downstream));
            java.util.Map<String, Object> body = req.body(data).retrieve().body(java.util.Map.class);
            return body == null ? List.of() : (List<String>) body.getOrDefault("storedOn", List.of());
        } catch (RestClientException e) {
            throw new WorkerException("PUT chunk " + chunkId + " to " + workerUrl + " failed: " + e.getMessage(), e);
        }
    }

    /** Tells the worker holding a chunk to verify it and push it down a pipeline to the targets. */
    @SuppressWarnings("unchecked")
    public ReplicateResult replicate(String sourceUrl, String chunkId, String sha256, List<PipelineTarget> targets) {
        try {
            java.util.Map<String, Object> body = http.post().uri(sourceUrl + "/internal/chunks/{id}/replicate", chunkId)
                    .header("X-Expected-Checksum", sha256)
                    .header("X-Pipeline", pipelineHeader(targets))
                    .retrieve().body(java.util.Map.class);
            return new ReplicateResult("OK", body == null ? List.of() : (List<String>) body.getOrDefault("storedOn", List.of()));
        } catch (RestClientResponseException e) {
            int c = e.getStatusCode().value();
            return new ReplicateResult(c == 404 ? "MISSING" : c == 409 ? "CORRUPT" : "FAILED", List.of());
        } catch (RestClientException e) {
            return new ReplicateResult("FAILED", List.of());
        }
    }

    /** Stores a chunk; the worker re-hashes the body and rejects it (400) if it does not match sha256. */
    public void putChunk(String workerUrl, String chunkId, byte[] data, String sha256) {
        try {
            http.put().uri(workerUrl + "/internal/chunks/{id}", chunkId)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header("X-Expected-Checksum", sha256)
                    .body(data)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException e) {
            throw new WorkerException("PUT chunk " + chunkId + " to " + workerUrl + " failed: " + e.getMessage(), e);
        }
    }

    public FetchResult getChunk(String workerUrl, String chunkId) {
        try {
            return http.get().uri(workerUrl + "/internal/chunks/{id}", chunkId)
                    .accept(MediaType.APPLICATION_OCTET_STREAM)
                    .exchange((req, res) -> {
                        int code = res.getStatusCode().value();
                        if (code == 200) return new Ok(res.getBody().readAllBytes());
                        if (code == 404) return new Missing();
                        return new Failed("HTTP " + code);
                    });
        } catch (ResourceAccessException e) {
            return new Unreachable(e.getMessage());
        } catch (RestClientException e) {
            return new Failed(e.getMessage());
        }
    }

    /** Best-effort delete. Returns true if the chunk is gone afterwards. */
    public boolean deleteChunk(String workerUrl, String chunkId) {
        try {
            http.delete().uri(workerUrl + "/internal/chunks/{id}", chunkId).retrieve().toBodilessEntity();
            return true;
        } catch (RestClientResponseException e) {
            return e.getStatusCode().value() == 404;
        } catch (RestClientException e) {
            return false;
        }
    }

    /** Physical inventory of a worker, with SHA-256 recomputed over every stored chunk. */
    public Inventory inventory(String workerUrl, String workerId) {
        try {
            Inventory inv = slowHttp.get()
                    .uri(workerUrl + "/internal/workers/{id}/chunks?verify=true", workerId)
                    .retrieve()
                    .body(Inventory.class);
            if (inv == null || inv.chunks() == null) {
                throw new WorkerException("Empty inventory response from " + workerUrl, null);
            }
            return inv;
        } catch (RestClientException e) {
            throw new WorkerException("Inventory of " + workerId + " failed: " + e.getMessage(), e);
        }
    }
}

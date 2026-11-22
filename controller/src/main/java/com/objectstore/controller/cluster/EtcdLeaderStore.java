package com.objectstore.controller.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * {@link LeaderStore} backed by an etcd v3 cluster, using etcd's JSON/HTTP gateway (/v3/...), so no gRPC client
 * library is needed. Requests go to the last endpoint that answered and fail over to the others, so the
 * election keeps working while a minority of the etcd nodes is down.
 */
public class EtcdLeaderStore implements LeaderStore {

    private static final Duration TIMEOUT = Duration.ofMillis(1000);

    private final List<String> endpoints;
    private final HttpClient http;
    private final ObjectMapper json = new ObjectMapper();
    private volatile int preferred = 0;

    public EtcdLeaderStore(List<String> endpoints) {
        if (endpoints.isEmpty()) throw new IllegalArgumentException("no etcd endpoints");
        this.endpoints = endpoints.stream().map(e -> e.endsWith("/") ? e.substring(0, e.length() - 1) : e).toList();
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(TIMEOUT).build();
    }

    @Override
    public long grantLease(int ttlSeconds) throws IOException {
        JsonNode r = call("/v3/lease/grant", Map.of("TTL", ttlSeconds));
        long id = r.path("ID").asLong(0);
        if (id == 0) throw new IOException("etcd did not grant a lease: " + r);
        return id;
    }

    @Override
    public long keepAlive(long leaseId) throws IOException {
        JsonNode r = call("/v3/lease/keepalive", Map.of("ID", Long.toString(leaseId)));
        // an expired/unknown lease is answered without a TTL (i.e. 0)
        return r.path("result").path("TTL").asLong(0);
    }

    @Override
    public long createIfAbsent(String key, String value, long leaseId) throws IOException {
        String k = b64(key);
        Map<String, Object> txn = Map.of(
                "compare", List.of(Map.of("target", "CREATE", "key", k, "createRevision", "0")),
                "success", List.of(Map.of("requestPut", Map.of("key", k, "value", b64(value), "lease", Long.toString(leaseId)))));
        JsonNode r = call("/v3/kv/txn", txn);
        if (!r.path("succeeded").asBoolean(false)) return -1;
        return r.path("header").path("revision").asLong();
    }

    @Override
    public Optional<String> get(String key) throws IOException {
        JsonNode kvs = call("/v3/kv/range", Map.of("key", b64(key))).path("kvs");
        if (!kvs.isArray() || kvs.isEmpty()) return Optional.empty();
        return Optional.of(new String(Base64.getDecoder().decode(kvs.get(0).path("value").asText("")), StandardCharsets.UTF_8));
    }

    @Override
    public void revoke(long leaseId) throws IOException {
        call("/v3/lease/revoke", Map.of("ID", Long.toString(leaseId)));
    }

    @Override
    public String describe() {
        return "etcd " + String.join(",", endpoints);
    }

    private JsonNode call(String path, Object body) throws IOException {
        byte[] payload = json.writeValueAsBytes(body);
        IOException last = null;
        int start = preferred;
        for (int i = 0; i < endpoints.size(); i++) {
            int idx = (start + i) % endpoints.size();
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create(endpoints.get(idx) + path))
                        .timeout(TIMEOUT)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                        .build();
                HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
                if (resp.statusCode() != 200) {
                    // e.g. a member that lost quorum answers with an error: try the next one
                    last = new IOException(endpoints.get(idx) + path + " -> HTTP " + resp.statusCode() + " "
                            + new String(resp.body(), StandardCharsets.UTF_8));
                    continue;
                }
                preferred = idx;
                return json.readTree(resp.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            } catch (IOException e) {
                last = new IOException(endpoints.get(idx) + ": " + e.getClass().getSimpleName()
                        + (e.getMessage() == null ? "" : " " + e.getMessage()), e);
            }
        }
        throw last;
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }
}

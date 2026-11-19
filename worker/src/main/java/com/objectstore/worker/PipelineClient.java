package com.objectstore.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HDFS-style write pipeline: a worker that stored a chunk forwards it to the next worker in the pipeline,
 * which stores it and forwards it on, and so on. Acknowledgements travel back up the chain, so the reply to
 * the first worker lists every worker that now holds the chunk. If the next worker fails it is skipped
 * and the following one is tried, as HDFS drops a bad datanode from the pipeline.
 */
@Component
public class PipelineClient {

    public static final String HEADER = "X-Pipeline";

    private static final Logger log = LoggerFactory.getLogger(PipelineClient.class);

    public record Target(String workerId, String url) {}

    private final RestClient http;

    public PipelineClient() {
        HttpClient jdk = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2)).build();
        JdkClientHttpRequestFactory f = new JdkClientHttpRequestFactory(jdk);
        f.setReadTimeout(Duration.ofSeconds(30));
        this.http = RestClient.builder().requestFactory(f).build();
    }

    /** Header format: "worker2=http://worker2:8081,worker3=http://worker3:8081". */
    public static List<Target> parse(String header) {
        List<Target> out = new ArrayList<>();
        if (header == null || header.isBlank()) return out;
        for (String part : header.split(",")) {
            int i = part.indexOf('=');
            if (i > 0) out.add(new Target(part.substring(0, i).trim(), part.substring(i + 1).trim()));
        }
        return out;
    }

    static String format(List<Target> targets) {
        return String.join(",", targets.stream().map(t -> t.workerId() + "=" + t.url()).toList());
    }

    /** Sends the chunk down the pipeline; returns ids of the downstream workers that stored it. */
    @SuppressWarnings("unchecked")
    public List<String> forward(String chunkId, byte[] data, String checksum, List<Target> downstream) {
        for (int i = 0; i < downstream.size(); i++) {
            Target next = downstream.get(i);
            List<Target> rest = downstream.subList(i + 1, downstream.size());
            try {
                var req = http.put().uri(next.url() + "/internal/chunks/{id}", chunkId)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .header("X-Expected-Checksum", checksum);
                if (!rest.isEmpty()) req = req.header(HEADER, format(rest));
                Map<String, Object> body = req.body(data).retrieve().body(Map.class);
                List<String> stored = body == null ? List.of() : (List<String>) body.getOrDefault("storedOn", List.of());
                log.info("Pipeline: forwarded chunk {} to {} (acked by {})", chunkId, next.workerId(), stored);
                return stored;
            } catch (Exception e) {
                log.warn("Pipeline: {} failed for chunk {} ({}); skipping it", next.workerId(), chunkId, e.getMessage());
            }
        }
        return List.of();
    }
}

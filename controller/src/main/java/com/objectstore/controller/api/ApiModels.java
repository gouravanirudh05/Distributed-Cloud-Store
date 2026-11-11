package com.objectstore.controller.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** JSON response shapes of the public and admin REST APIs. */
public final class ApiModels {

    private ApiModels() {}

    /** Full metadata of an object: system metadata, custom metadata, and chunk locations. */
    public record ObjectMetadata(
            String objectId,
            String name,
            long size,
            String contentType,
            String checksumAlgorithm,
            String checksum,
            Instant createdAt,
            Instant lastModified,
            String status,
            int chunkSize,
            int chunkCount,
            int replicationFactor,
            Map<String, String> customMetadata,
            ReplicationSummary replication,
            List<ChunkView> chunks) {}

    /** HEALTHY: every chunk has exactly RF live copies. DEGRADED: some have fewer. UNAVAILABLE: some have none. */
    public record ReplicationSummary(String status, int healthyChunks, int underReplicatedChunks,
                                     int overReplicatedChunks, int unavailableChunks) {}

    public record ChunkView(String chunkId, int index, int size, String checksum, int healthyReplicas,
                            List<ReplicaView> replicas) {}

    public record ReplicaView(String workerId, String workerStatus, Instant storedAt) {}

    public record ObjectSummary(String objectId, String name, long size, String contentType, int chunkCount,
                                Instant createdAt, Instant lastModified) {}

    public record PageResponse<T>(List<T> items, int page, int size, long totalItems, int totalPages) {}

    public record ErrorResponse(Instant timestamp, int status, String error, String code, String message, String path) {}

    // ---- admin ----

    public record WorkerView(String workerId, String url, String status, Instant lastHeartbeat,
                             Long secondsSinceHeartbeat, Instant downSince, Instant reReplicationAt,
                             long replicaCount, Long reportedChunkCount, Long reportedUsedBytes,
                             Long reportedFreeBytes) {}

    public record WorkerReplicaCount(String workerId, String status, long replicaCount) {}

    public record ProblemChunkView(String chunkId, String objectId, String objectName, int index,
                                   int healthyReplicas, List<Map<String, String>> replicas) {}

    public record ReplicationReport(int replicationFactor, String status, long totalChunks, long healthyChunks,
                                    long underReplicatedChunks, long overReplicatedChunks, long unavailableChunks,
                                    List<WorkerReplicaCount> workers, List<ProblemChunkView> problemChunks) {}

    public record ExpectedChunk(String chunkId, String objectId, String objectName, int index, int size,
                                String checksum) {}

    public record WorkerChunksView(String workerId, String status, String source, int chunkCount,
                                   List<ExpectedChunk> chunks) {}

    public record WorkerChunkComparison(String workerId, String status, int expectedCount, Integer actualCount,
                                        String actualError, List<String> missing, List<String> corrupted,
                                        List<String> unexpected, List<ExpectedChunk> expected,
                                        Object actual) {}
}

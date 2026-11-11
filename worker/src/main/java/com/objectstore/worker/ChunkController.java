package com.objectstore.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal worker API (called by the controller only):
 *   PUT    /internal/chunks/{chunkId}               store a chunk (raw bytes, optional X-Expected-Checksum)
 *   GET    /internal/chunks/{chunkId}               retrieve a chunk (X-Chunk-Checksum = SHA-256 of stored bytes)
 *   DELETE /internal/chunks/{chunkId}               delete a chunk (idempotent, 204)
 *   GET    /internal/workers/{workerId}/chunks      inventory of chunks physically stored on this worker
 */
@RestController
public class ChunkController {

    private static final Logger log = LoggerFactory.getLogger(ChunkController.class);

    private final ChunkStore store;
    private final String workerId;

    public ChunkController(ChunkStore store, @Value("${worker.id}") String workerId) {
        this.store = store;
        this.workerId = workerId;
    }

    @PutMapping(value = "/internal/chunks/{chunkId}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<Map<String, Object>> putChunk(@PathVariable String chunkId,
                                                        @RequestHeader(value = "X-Expected-Checksum", required = false) String expectedChecksum,
                                                        InputStream body) throws IOException {
        ChunkStore.StoredChunk stored = store.put(chunkId, body, expectedChecksum);
        log.info("{} chunk {} ({} bytes, sha256 {})", stored.replaced() ? "Replaced" : "Stored",
                chunkId, stored.size(), stored.checksum());
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("workerId", workerId);
        resp.put("chunkId", chunkId);
        resp.put("size", stored.size());
        resp.put("checksum", stored.checksum());
        return ResponseEntity.status(stored.replaced() ? HttpStatus.OK : HttpStatus.CREATED).body(resp);
    }

    @GetMapping(value = "/internal/chunks/{chunkId}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> getChunk(@PathVariable String chunkId) throws IOException {
        return store.get(chunkId)
                .map(c -> ResponseEntity.ok()
                        .header("X-Chunk-Checksum", c.checksum())
                        .header("X-Worker-Id", workerId)
                        .contentLength(c.data().length)
                        .body(c.data()))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @DeleteMapping("/internal/chunks/{chunkId}")
    public ResponseEntity<Void> deleteChunk(@PathVariable String chunkId) throws IOException {
        if (store.delete(chunkId)) {
            log.info("Deleted chunk {}", chunkId);
        }
        return ResponseEntity.noContent().build();
    }

    /**
     * Ground-truth inventory read from disk. checksum = SHA-256 of the bytes currently on disk
     * (pass verify=false to skip hashing and only list ids/sizes).
     */
    @GetMapping(value = "/internal/workers/{requestedWorkerId}/chunks", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> listChunks(@PathVariable String requestedWorkerId,
                                                          @RequestParam(defaultValue = "true") boolean verify) throws IOException {
        if (!workerId.equals(requestedWorkerId)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "This is worker '" + workerId + "', not '" + requestedWorkerId + "'"));
        }
        List<ChunkStore.ChunkInfo> chunks = store.list(verify);
        long totalBytes = chunks.stream().mapToLong(ChunkStore.ChunkInfo::size).sum();
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("workerId", workerId);
        resp.put("chunkCount", chunks.size());
        resp.put("totalBytes", totalBytes);
        resp.put("checksumsComputed", verify);
        resp.put("chunks", chunks);
        return ResponseEntity.ok(resp);
    }

    @ExceptionHandler(ChunkStore.ChecksumMismatchException.class)
    public ResponseEntity<Map<String, Object>> onChecksumMismatch(ChunkStore.ChecksumMismatchException e) {
        log.warn("Rejected chunk write: {}", e.getMessage());
        return ResponseEntity.badRequest().body(Map.of(
                "error", "checksum mismatch", "expected", e.getExpected(), "actual", e.getActual()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> onBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IOException.class)
    public ResponseEntity<Map<String, Object>> onIoError(IOException e) {
        log.error("Storage I/O error", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "storage I/O error: " + e.getMessage()));
    }
}

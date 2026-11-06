package com.objectstore.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

@RestController
public class ChunkController {

    private static final Logger log = LoggerFactory.getLogger(ChunkController.class);

    private final Path dataDir;
    private final String workerId;

    public ChunkController(@Value("${worker.data-dir}") String dataDir,
                           @Value("${worker.id}") String workerId) {
        this.dataDir = Path.of(dataDir);
        this.workerId = workerId;
    }

    public record ChunkInfo(String chunkId, long size, String checksum) {}

    /**
     * Store a chunk on the worker.
     */
    @PutMapping(value = "/internal/chunks/{chunkId}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<?> putChunk(@PathVariable String chunkId,
                                      @RequestHeader(value = "X-Expected-Checksum", required = false) String expectedChecksum,
                                      InputStream inputStream) {
        try {
            Files.createDirectories(dataDir);
            Path tempFile = Files.createTempFile(dataDir, "upload-", ".tmp");
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            long totalBytes = 0;
            try (OutputStream os = Files.newOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = inputStream.read(buffer)) != -1) {
                    os.write(buffer, 0, read);
                    digest.update(buffer, 0, read);
                    totalBytes += read;
                }
            }

            String actualChecksum = HexFormat.of().formatHex(digest.digest());

            if (expectedChecksum != null && !expectedChecksum.isBlank() && !actualChecksum.equalsIgnoreCase(expectedChecksum)) {
                Files.deleteIfExists(tempFile);
                log.warn("Checksum mismatch for chunk {}: expected {} but got {}", chunkId, expectedChecksum, actualChecksum);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "checksum mismatch", "expected", expectedChecksum, "actual", actualChecksum));
            }

            Path targetFile = dataDir.resolve(chunkId);
            Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            log.info("Stored chunk {} ({} bytes, sha256: {})", chunkId, totalBytes, actualChecksum);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(new ChunkInfo(chunkId, totalBytes, actualChecksum));

        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("Failed to store chunk {}", chunkId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Failed to store chunk: " + e.getMessage()));
        }
    }

    /**
     * Retrieve a chunk from the worker.
     */
    @GetMapping(value = "/internal/chunks/{chunkId}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<byte[]> getChunk(@PathVariable String chunkId) {
        Path chunkFile = dataDir.resolve(chunkId);
        if (!Files.exists(chunkFile) || !Files.isRegularFile(chunkFile)) {
            return ResponseEntity.notFound().build();
        }

        try {
            byte[] data = Files.readAllBytes(chunkFile);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            String checksum = HexFormat.of().formatHex(hash);

            HttpHeaders headers = new HttpHeaders();
            headers.add("X-Chunk-Checksum", checksum);
            headers.setContentLength(data.length);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(data);

        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("Error reading chunk {}", chunkId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Delete a chunk from the worker.
     */
    @DeleteMapping("/internal/chunks/{chunkId}")
    public ResponseEntity<Void> deleteChunk(@PathVariable String chunkId) {
        Path chunkFile = dataDir.resolve(chunkId);
        try {
            boolean deleted = Files.deleteIfExists(chunkFile);
            if (deleted) {
                log.info("Deleted chunk {}", chunkId);
            }
            return ResponseEntity.noContent().build();
        } catch (IOException e) {
            log.error("Failed to delete chunk {}", chunkId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Retrieve information about all chunks stored on this worker.
     */
    @GetMapping("/internal/workers/{workerId}/chunks")
    public ResponseEntity<List<ChunkInfo>> listChunks(@PathVariable String workerId) {
        if (!Files.exists(dataDir)) {
            return ResponseEntity.ok(Collections.emptyList());
        }

        List<ChunkInfo> chunks = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dataDir)) {
            stream.filter(Files::isRegularFile)
                  .filter(p -> !p.getFileName().toString().endsWith(".tmp"))
                  .forEach(file -> {
                      try {
                          String chunkId = file.getFileName().toString();
                          long size = Files.size(file);
                          byte[] data = Files.readAllBytes(file);
                          MessageDigest digest = MessageDigest.getInstance("SHA-256");
                          String checksum = HexFormat.of().formatHex(digest.digest(data));
                          chunks.add(new ChunkInfo(chunkId, size, checksum));
                      } catch (Exception e) {
                          log.warn("Error inspecting chunk file {}", file, e);
                      }
                  });
            return ResponseEntity.ok(chunks);
        } catch (IOException e) {
            log.error("Failed to list chunks in {}", dataDir, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}

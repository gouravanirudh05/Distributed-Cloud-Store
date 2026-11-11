package com.objectstore.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Stores chunks as plain files in the worker's data directory (a Docker volume, so it survives restarts).
 * File name = chunk id. Integrity is checked by recomputing SHA-256 over the stored bytes; the controller
 * compares that with the checksum it recorded at upload time.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);
    private static final Pattern VALID_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final String TMP_PREFIX = "upload-";
    private static final String TMP_SUFFIX = ".tmp";

    public record StoredChunk(String chunkId, long size, String checksum, boolean replaced) {}
    public record ChunkData(byte[] data, String checksum) {}
    public record ChunkInfo(String chunkId, long size, String checksum, Instant storedAt) {}
    public record Stats(long chunkCount, long usedBytes, long freeBytes) {}

    public static class ChecksumMismatchException extends RuntimeException {
        private final String expected;
        private final String actual;

        public ChecksumMismatchException(String expected, String actual) {
            super("checksum mismatch: expected " + expected + " but received data hashes to " + actual);
            this.expected = expected;
            this.actual = actual;
        }

        public String getExpected() { return expected; }
        public String getActual() { return actual; }
    }

    private final Path dataDir;

    public ChunkStore(@Value("${worker.data-dir}") String dataDir) throws IOException {
        this.dataDir = Path.of(dataDir).toAbsolutePath().normalize();
        Files.createDirectories(this.dataDir);
        removeLeftoverTempFiles();
    }

    public Path getDataDir() { return dataDir; }

    public static boolean isValidId(String chunkId) {
        return chunkId != null && VALID_ID.matcher(chunkId).matches() && !chunkId.endsWith(TMP_SUFFIX);
    }

    /** Writes the chunk atomically (temp file + rename). Rejects the write if expectedChecksum is given and differs. */
    public StoredChunk put(String chunkId, InputStream in, String expectedChecksum) throws IOException {
        Path target = resolve(chunkId);
        Path tmp = Files.createTempFile(dataDir, TMP_PREFIX, TMP_SUFFIX);
        try {
            MessageDigest digest = sha256();
            long size;
            try (OutputStream os = new DigestOutputStream(Files.newOutputStream(tmp), digest)) {
                size = in.transferTo(os);
            }
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true); // durable before we acknowledge the write
            }
            String actual = HexFormat.of().formatHex(digest.digest());
            if (expectedChecksum != null && !expectedChecksum.isBlank()
                    && !actual.equalsIgnoreCase(expectedChecksum.trim())) {
                throw new ChecksumMismatchException(expectedChecksum.trim(), actual);
            }
            boolean existed = Files.exists(target);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return new StoredChunk(chunkId, size, actual, existed);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /** Returns the stored bytes plus the SHA-256 of what is actually on disk right now. */
    public Optional<ChunkData> get(String chunkId) throws IOException {
        Path file = resolve(chunkId);
        try {
            byte[] data = Files.readAllBytes(file);
            return Optional.of(new ChunkData(data, sha256Hex(data)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    public boolean delete(String chunkId) throws IOException {
        return Files.deleteIfExists(resolve(chunkId));
    }

    /** Lists every stored chunk. With computeChecksums=true each file is re-hashed (used for integrity scrubs). */
    public List<ChunkInfo> list(boolean computeChecksums) throws IOException {
        List<ChunkInfo> result = new ArrayList<>();
        try (Stream<Path> files = Files.list(dataDir)) {
            for (Path file : (Iterable<Path>) files::iterator) {
                String name = file.getFileName().toString();
                if (!isValidId(name)) continue;
                try {
                    BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
                    if (!attrs.isRegularFile()) continue;
                    String checksum = computeChecksums ? sha256Hex(Files.readAllBytes(file)) : null;
                    result.add(new ChunkInfo(name, attrs.size(), checksum, attrs.lastModifiedTime().toInstant()));
                } catch (NoSuchFileException e) {
                    // deleted while listing
                }
            }
        }
        result.sort(Comparator.comparing(ChunkInfo::chunkId));
        return result;
    }

    public Stats stats() {
        long count = 0, used = 0, free = -1;
        try {
            for (ChunkInfo c : list(false)) {
                count++;
                used += c.size();
            }
            free = Files.getFileStore(dataDir).getUsableSpace();
        } catch (IOException e) {
            log.warn("Could not compute storage stats: {}", e.getMessage());
        }
        return new Stats(count, used, free);
    }

    private Path resolve(String chunkId) {
        if (!isValidId(chunkId)) {
            throw new IllegalArgumentException("Invalid chunk id: " + chunkId);
        }
        Path p = dataDir.resolve(chunkId).normalize();
        if (!dataDir.equals(p.getParent())) {
            throw new IllegalArgumentException("Invalid chunk id: " + chunkId);
        }
        return p;
    }

    private void removeLeftoverTempFiles() throws IOException {
        try (Stream<Path> files = Files.list(dataDir)) {
            files.filter(p -> {
                String n = p.getFileName().toString();
                return n.startsWith(TMP_PREFIX) && n.endsWith(TMP_SUFFIX);
            }).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                    log.info("Removed leftover partial upload {}", p.getFileName());
                } catch (IOException e) {
                    log.warn("Could not remove {}: {}", p, e.getMessage());
                }
            });
        }
    }

    static String sha256Hex(byte[] data) {
        return HexFormat.of().formatHex(sha256().digest(data));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

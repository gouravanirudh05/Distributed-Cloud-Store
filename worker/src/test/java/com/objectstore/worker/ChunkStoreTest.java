package com.objectstore.worker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChunkStoreTest {

    @TempDir
    Path dir;
    ChunkStore store;

    @BeforeEach
    void setUp() throws Exception {
        store = new ChunkStore(dir.toString());
    }

    private static ByteArrayInputStream bytes(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void putThenGetRoundTripsAndReportsChecksum() throws Exception {
        ChunkStore.StoredChunk stored = store.put("obj_000000", bytes("hello"), null);
        assertFalse(stored.replaced());
        assertEquals(5, stored.size());
        assertEquals(ChunkStore.sha256Hex("hello".getBytes()), stored.checksum());

        ChunkStore.ChunkData data = store.get("obj_000000").orElseThrow();
        assertArrayEquals("hello".getBytes(), data.data());
        assertEquals(stored.checksum(), data.checksum());
    }

    @Test
    void putWithWrongExpectedChecksumIsRejectedAndLeavesNothingBehind() throws Exception {
        assertThrows(ChunkStore.ChecksumMismatchException.class,
                () -> store.put("obj_000001", bytes("hello"), "deadbeef"));
        assertTrue(store.get("obj_000001").isEmpty());
        try (var files = Files.list(dir)) {
            assertEquals(0, files.count(), "temp file must be cleaned up");
        }
    }

    @Test
    void putWithCorrectExpectedChecksumSucceedsAndOverwriteIsReported() throws Exception {
        String sum = ChunkStore.sha256Hex("v1".getBytes());
        store.put("c", bytes("v1"), sum);
        ChunkStore.StoredChunk again = store.put("c", bytes("v2"), null);
        assertTrue(again.replaced());
        assertArrayEquals("v2".getBytes(), store.get("c").orElseThrow().data());
    }

    @Test
    void corruptionOnDiskIsVisibleThroughChecksum() throws Exception {
        String original = store.put("c", bytes("important data"), null).checksum();
        Files.writeString(dir.resolve("c"), "CORRUPTED data");
        assertNotEquals(original, store.get("c").orElseThrow().checksum());
        List<ChunkStore.ChunkInfo> inv = store.list(true);
        assertEquals(1, inv.size());
        assertNotEquals(original, inv.get(0).checksum());
    }

    @Test
    void invalidIdsAreRejected() {
        for (String bad : new String[]{"..", "../x", "a/b", "", ".hidden", "x.tmp"}) {
            assertThrows(IllegalArgumentException.class, () -> store.get(bad), bad);
        }
    }

    @Test
    void deleteAndListAndStats() throws Exception {
        store.put("a", bytes("1234"), null);
        store.put("b", bytes("56"), null);
        assertEquals(List.of("a", "b"), store.list(false).stream().map(ChunkStore.ChunkInfo::chunkId).toList());
        ChunkStore.Stats stats = store.stats();
        assertEquals(2, stats.chunkCount());
        assertEquals(6, stats.usedBytes());
        assertTrue(store.delete("a"));
        assertFalse(store.delete("a"));
        assertEquals(1, store.list(false).size());
    }

    @Test
    void leftoverTempFilesAreRemovedOnStartup() throws Exception {
        Files.writeString(dir.resolve("upload-123.tmp"), "partial");
        new ChunkStore(dir.toString());
        assertFalse(Files.exists(dir.resolve("upload-123.tmp")));
    }
}

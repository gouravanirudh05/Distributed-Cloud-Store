package com.objectstore.controller.object;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ObjectHelpersTest {

    @Test
    void chunkIdsEncodeObjectAndOrder() {
        String obj = "3f1c2b4a-0000-4000-8000-123456789abc";
        assertEquals(obj + "_000000", ChunkIds.of(obj, 0));
        assertEquals(obj + "_000012", ChunkIds.of(obj, 12));
        assertTrue(ChunkIds.of(obj, 2).compareTo(ChunkIds.of(obj, 10)) < 0, "lexicographic order = chunk order");
        assertEquals(obj, ChunkIds.objectIdOf(ChunkIds.of(obj, 7)));
        assertNull(ChunkIds.objectIdOf("random-file"));
        assertNull(ChunkIds.objectIdOf("notauuid_000001"));
    }

    @Test
    void contentTypeUsesDeclaredTypeOrFallsBackToExtension() {
        assertEquals("image/png", ObjectService.resolveContentType("image/png", "x.bin"));
        assertEquals("application/pdf", ObjectService.resolveContentType("application/octet-stream", "report.pdf"));
        assertEquals("video/mp4", ObjectService.resolveContentType(null, "clip.mp4"));
        assertEquals("application/octet-stream", ObjectService.resolveContentType(null, "data.unknownext"));
        assertEquals("text/plain", ObjectService.resolveContentType("garbage///", "notes.txt"));
    }

    @Test
    void namesAreStrippedOfPaths() {
        assertEquals("photo.jpg", ObjectService.resolveName("C:\\Users\\me\\photo.jpg", "id"));
        assertEquals("photo.jpg", ObjectService.resolveName("/tmp/photo.jpg", "id"));
        assertEquals("id", ObjectService.resolveName("   ", "id"));
        assertEquals("id", ObjectService.resolveName(null, "id"));
    }

    @Test
    void sha256OfKnownInput() {
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                Checksums.sha256Hex("hello".getBytes()));
    }
}

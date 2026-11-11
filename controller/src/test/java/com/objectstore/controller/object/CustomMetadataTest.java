package com.objectstore.controller.object;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.controller.api.ApiException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomMetadataTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void uploadFieldParsesStringPairs() {
        assertEquals(Map.of("author", "alice", "project", "demo"),
                CustomMetadata.parseUploadField("{\"author\":\"alice\",\"project\":\"demo\"}", mapper));
        assertEquals(Map.of(), CustomMetadata.parseUploadField(null, mapper));
        assertEquals(Map.of(), CustomMetadata.parseUploadField("  ", mapper));
    }

    @Test
    void uploadFieldRejectsInvalidInput() {
        for (String bad : new String[]{"not json", "[1,2]", "\"str\"", "{\"n\":1}", "{\"n\":null}", "{\"\":\"x\"}"}) {
            ApiException e = assertThrows(ApiException.class, () -> CustomMetadata.parseUploadField(bad, mapper), bad);
            assertEquals(400, e.getStatus().value());
        }
    }

    @Test
    void mergePatchAddsUpdatesAndRemovesKeys() throws Exception {
        Map<String, String> current = new HashMap<>(Map.of("author", "alice", "draft", "yes", "keep", "1"));
        Map<String, String> patch = CustomMetadata.parsePatch(
                mapper.readTree("{\"author\":\"bob\",\"draft\":null,\"reviewed\":\"true\"}"));
        assertTrue(CustomMetadata.applyPatch(current, patch));
        assertEquals(Map.of("author", "bob", "keep", "1", "reviewed", "true"), current);
    }

    @Test
    void patchThatChangesNothingReportsNoChange() throws Exception {
        Map<String, String> current = new HashMap<>(Map.of("a", "1"));
        assertFalse(CustomMetadata.applyPatch(current, CustomMetadata.parsePatch(mapper.readTree("{\"a\":\"1\",\"zzz\":null}"))));
        assertEquals(Map.of("a", "1"), current);
    }

    @Test
    void patchRejectsNonObjectsAndNonStringValues() throws Exception {
        assertThrows(ApiException.class, () -> CustomMetadata.parsePatch(mapper.readTree("[]")));
        assertThrows(ApiException.class, () -> CustomMetadata.parsePatch(mapper.readTree("{\"a\":5}")));
        assertThrows(ApiException.class, () -> CustomMetadata.parsePatch(mapper.readTree("{\"a\":{\"b\":\"c\"}}")));
        assertThrows(ApiException.class, () -> CustomMetadata.parsePatch(null));
    }

    @Test
    void entryLimitIsEnforced() {
        Map<String, String> current = new HashMap<>();
        Map<String, String> patch = new HashMap<>();
        for (int i = 0; i <= CustomMetadata.MAX_ENTRIES; i++) patch.put("k" + i, "v");
        assertThrows(ApiException.class, () -> CustomMetadata.applyPatch(current, patch));
        assertTrue(current.isEmpty(), "a rejected patch must not partially apply");
    }
}

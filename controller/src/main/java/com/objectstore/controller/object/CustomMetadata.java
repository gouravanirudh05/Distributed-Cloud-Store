package com.objectstore.controller.object;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.controller.api.ApiException;

import java.util.*;

/**
 * Rules for custom (user-defined) metadata:
 *  - a flat JSON object of string keys to string values;
 *  - at most 64 entries, keys 1-128 chars, values up to 4096 chars, no control characters in keys;
 *  - PATCH is a merge patch (RFC 7396 style): listed keys are added/updated, a null value removes the key,
 *    keys not mentioned are left untouched.
 */
public final class CustomMetadata {

    public static final int MAX_ENTRIES = 64;
    public static final int MAX_KEY_LENGTH = 128;
    public static final int MAX_VALUE_LENGTH = 4096;

    private CustomMetadata() {}

    /** Parses the optional "metadata" multipart field of an upload. Null/blank means no custom metadata. */
    public static Map<String, String> parseUploadField(String json, ObjectMapper mapper) {
        if (json == null || json.isBlank()) return Map.of();
        JsonNode node;
        try {
            node = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw ApiException.badRequest("Field 'metadata' is not valid JSON: " + e.getOriginalMessage());
        }
        if (node == null || !node.isObject()) {
            throw ApiException.badRequest("Field 'metadata' must be a JSON object of string key/value pairs, e.g. {\"author\":\"alice\"}");
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> e : node.properties()) {
            validateKey(e.getKey());
            JsonNode v = e.getValue();
            if (!v.isTextual()) {
                throw ApiException.badRequest("Metadata value for key '" + e.getKey() + "' must be a string");
            }
            validateValue(e.getKey(), v.textValue());
            out.put(e.getKey(), v.textValue());
        }
        if (out.size() > MAX_ENTRIES) {
            throw ApiException.badRequest("At most " + MAX_ENTRIES + " custom metadata entries are allowed");
        }
        return out;
    }

    /** Parses a PATCH body. The result maps key -> new value, or key -> null for "remove this key". */
    public static Map<String, String> parsePatch(JsonNode body) {
        if (body == null || !body.isObject()) {
            throw ApiException.badRequest("Request body must be a JSON object of string values (null removes a key), e.g. {\"author\":\"bob\",\"draft\":null}");
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> e : body.properties()) {
            validateKey(e.getKey());
            JsonNode v = e.getValue();
            if (v.isNull()) {
                out.put(e.getKey(), null);
            } else if (v.isTextual()) {
                validateValue(e.getKey(), v.textValue());
                out.put(e.getKey(), v.textValue());
            } else {
                throw ApiException.badRequest("Metadata value for key '" + e.getKey() + "' must be a string or null");
            }
        }
        return out;
    }

    /** Applies a parsed patch in place. Returns true if anything changed. */
    public static boolean applyPatch(Map<String, String> target, Map<String, String> patch) {
        Map<String, String> result = new HashMap<>(target);
        for (Map.Entry<String, String> e : patch.entrySet()) {
            if (e.getValue() == null) result.remove(e.getKey());
            else result.put(e.getKey(), e.getValue());
        }
        if (result.size() > MAX_ENTRIES) {
            throw ApiException.badRequest("At most " + MAX_ENTRIES + " custom metadata entries are allowed");
        }
        if (result.equals(target)) return false;
        target.clear();
        target.putAll(result);
        return true;
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw ApiException.badRequest("Metadata keys must be non-empty");
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw ApiException.badRequest("Metadata key '" + key.substring(0, 20) + "...' is longer than " + MAX_KEY_LENGTH + " characters");
        }
        if (key.chars().anyMatch(Character::isISOControl)) {
            throw ApiException.badRequest("Metadata key '" + key + "' contains control characters");
        }
    }

    private static void validateValue(String key, String value) {
        if (value.length() > MAX_VALUE_LENGTH) {
            throw ApiException.badRequest("Metadata value for key '" + key + "' is longer than " + MAX_VALUE_LENGTH + " characters");
        }
    }
}

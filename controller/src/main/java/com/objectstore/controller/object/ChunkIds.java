package com.objectstore.controller.object;

import java.util.regex.Pattern;

/** Chunk id = "<objectId>_<6-digit index>", e.g. 3f1c...-9a2b_000004. Unique, ordered, and traceable to its object. */
public final class ChunkIds {

    private static final Pattern UUID = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    private ChunkIds() {}

    public static String of(String objectId, int index) {
        return objectId + "_" + String.format("%06d", index);
    }

    /** Returns the object id encoded in a chunk id, or null if the id does not follow the scheme. */
    public static String objectIdOf(String chunkId) {
        if (chunkId == null) return null;
        int i = chunkId.lastIndexOf('_');
        if (i <= 0) return null;
        String objectId = chunkId.substring(0, i);
        return UUID.matcher(objectId).matches() ? objectId : null;
    }
}

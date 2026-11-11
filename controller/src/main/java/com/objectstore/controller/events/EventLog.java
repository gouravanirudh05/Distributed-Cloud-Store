package com.objectstore.controller.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/** Append-only log of cluster events, persisted in the "events" table and mirrored to the application log. */
@Component
public class EventLog {

    private static final Logger log = LoggerFactory.getLogger(EventLog.class);

    public record Event(long id, Instant timestamp, EventType type, String workerId, String objectId,
                        String chunkId, String message) {}

    private final JdbcTemplate jdbc;

    public EventLog(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void record(EventType type, String workerId, String objectId, String chunkId, String message) {
        String msg = message.length() > 2000 ? message.substring(0, 2000) + "..." : message;
        switch (type) {
            case WORKER_DOWN, CHUNK_CORRUPTED, CHUNK_MISSING, CHUNK_LOST, CHUNK_UNAVAILABLE, UPLOAD_FAILED,
                 UPLOAD_DEGRADED, CHUNK_UNDER_REPLICATED -> log.warn("[{}] {}", type, msg);
            default -> log.info("[{}] {}", type, msg);
        }
        try {
            jdbc.update("INSERT INTO events (ts, type, worker_id, object_id, chunk_id, message) VALUES (?,?,?,?,?,?)",
                    Timestamp.from(Instant.now()), type.name(), workerId, objectId, chunkId, msg);
        } catch (Exception e) {
            log.warn("Could not persist event {}: {}", type, e.getMessage());
        }
    }

    public void worker(EventType type, String workerId, String message) {
        record(type, workerId, null, null, message);
    }

    public void chunk(EventType type, String workerId, String chunkId, String message) {
        record(type, workerId, objectIdOf(chunkId), chunkId, message);
    }

    public List<Event> recent(int limit, String type) {
        String sql = "SELECT id, ts, type, worker_id, object_id, chunk_id, message FROM events "
                + (type == null ? "" : "WHERE type = ? ") + "ORDER BY id DESC LIMIT ?";
        Object[] args = type == null ? new Object[]{limit} : new Object[]{type, limit};
        return jdbc.query(sql, (rs, i) -> new Event(
                rs.getLong("id"),
                rs.getTimestamp("ts").toInstant(),
                EventType.valueOf(rs.getString("type")),
                rs.getString("worker_id"),
                rs.getString("object_id"),
                rs.getString("chunk_id"),
                rs.getString("message")), args);
    }

    public void prune(int retain) {
        jdbc.update("DELETE FROM events WHERE id <= (SELECT COALESCE(MAX(id), 0) - ? FROM events)", retain);
    }

    private static String objectIdOf(String chunkId) {
        if (chunkId == null) return null;
        int i = chunkId.lastIndexOf('_');
        return i == 36 ? chunkId.substring(0, i) : null;
    }
}

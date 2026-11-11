package com.objectstore.controller.object;

import com.objectstore.controller.worker.WorkerStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;

/** Chunk and replica bookkeeping (plain JDBC: bulk inserts and aggregate replication queries). */
@Repository
public class ChunkDao {

    public record ChunkRow(String id, String objectId, int index, int size, String checksum) {
        /** e.g. "chunk 3 of object 3f1c2b4a" (for log/event messages). */
        public String label() {
            return "chunk " + index + " of object " + (objectId.length() > 8 ? objectId.substring(0, 8) : objectId);
        }
    }

    public record ReplicaRow(long id, String chunkId, String workerId, Instant createdAt,
                             WorkerStatus workerStatus, String workerUrl, Instant workerDownSince) {
        public boolean isUp() { return workerStatus == WorkerStatus.UP; }
    }

    public record NewReplica(String chunkId, String workerId) {}

    public record ExpectedReplica(String chunkId, String objectId, String objectName, ObjectStatus objectStatus,
                                  int index, int size, String checksum) {}

    public record ReplicaState(String workerId, WorkerStatus workerStatus) {}

    public record ProblemChunk(String chunkId, String objectId, String objectName, int index, int healthyReplicas,
                               List<ReplicaState> replicas) {}

    private static final String REPLICA_COLUMNS =
            "r.id, r.chunk_id, r.worker_id, r.created_at, w.status, w.url, w.down_since";

    private static final RowMapper<ChunkRow> CHUNK_MAPPER = (rs, i) -> new ChunkRow(
            rs.getString("id"), rs.getString("object_id"), rs.getInt("chunk_index"), rs.getInt("size"),
            rs.getString("checksum"));

    private static final RowMapper<ReplicaRow> REPLICA_MAPPER = (rs, i) -> new ReplicaRow(
            rs.getLong(1), rs.getString(2), rs.getString(3), instant(rs, 4),
            WorkerStatus.valueOf(rs.getString(5)), rs.getString(6), instant(rs, 7));

    private final JdbcTemplate jdbc;

    public ChunkDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertChunks(List<ChunkRow> chunks) {
        jdbc.batchUpdate("INSERT INTO chunks (id, object_id, chunk_index, size, checksum) VALUES (?,?,?,?,?)",
                chunks, 500, (ps, c) -> {
                    ps.setString(1, c.id());
                    ps.setString(2, c.objectId());
                    ps.setInt(3, c.index());
                    ps.setInt(4, c.size());
                    ps.setString(5, c.checksum());
                });
    }

    public void insertReplicas(List<NewReplica> replicas, Instant now) {
        Timestamp ts = Timestamp.from(now);
        jdbc.batchUpdate("INSERT INTO chunk_replicas (chunk_id, worker_id, created_at) VALUES (?,?,?) "
                        + "ON CONFLICT (chunk_id, worker_id) DO NOTHING",
                replicas, 500, (ps, r) -> {
                    ps.setString(1, r.chunkId());
                    ps.setString(2, r.workerId());
                    ps.setTimestamp(3, ts);
                });
    }

    public void insertReplica(String chunkId, String workerId, Instant now) {
        insertReplicas(List.of(new NewReplica(chunkId, workerId)), now);
    }

    public int deleteReplica(String chunkId, String workerId) {
        return jdbc.update("DELETE FROM chunk_replicas WHERE chunk_id = ? AND worker_id = ?", chunkId, workerId);
    }

    public Optional<ChunkRow> findChunk(String chunkId) {
        return jdbc.query("SELECT * FROM chunks WHERE id = ?", CHUNK_MAPPER, chunkId).stream().findFirst();
    }

    public List<ChunkRow> chunksOfObject(String objectId) {
        return jdbc.query("SELECT * FROM chunks WHERE object_id = ? ORDER BY chunk_index", CHUNK_MAPPER, objectId);
    }

    /** Replicas in creation order (the first one is the "primary" copy, read first). */
    public List<ReplicaRow> replicasOfChunk(String chunkId) {
        return jdbc.query("SELECT " + REPLICA_COLUMNS + " FROM chunk_replicas r JOIN workers w ON w.id = r.worker_id "
                + "WHERE r.chunk_id = ? ORDER BY r.id", REPLICA_MAPPER, chunkId);
    }

    /** chunkId -> replicas (creation order) for every chunk of the object. */
    public Map<String, List<ReplicaRow>> replicasOfObject(String objectId) {
        List<ReplicaRow> rows = jdbc.query("SELECT " + REPLICA_COLUMNS + " FROM chunk_replicas r "
                + "JOIN workers w ON w.id = r.worker_id JOIN chunks c ON c.id = r.chunk_id "
                + "WHERE c.object_id = ? ORDER BY c.chunk_index, r.id", REPLICA_MAPPER, objectId);
        Map<String, List<ReplicaRow>> out = new LinkedHashMap<>();
        for (ReplicaRow r : rows) {
            out.computeIfAbsent(r.chunkId(), k -> new ArrayList<>()).add(r);
        }
        return out;
    }

    public Map<String, Long> replicaCountsByWorker() {
        Map<String, Long> out = new HashMap<>();
        jdbc.query("SELECT worker_id, COUNT(*) FROM chunk_replicas GROUP BY worker_id",
                rs -> { out.put(rs.getString(1), rs.getLong(2)); });
        return out;
    }

    public long replicaCountOnWorker(String workerId) {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM chunk_replicas WHERE worker_id = ?", Long.class, workerId);
        return n == null ? 0 : n;
    }

    /** Chunks the metadata says this worker should hold (any object status; callers filter). */
    public List<ExpectedReplica> expectedOnWorker(String workerId) {
        return jdbc.query("""
                SELECT c.id, c.object_id, o.name, o.status, c.chunk_index, c.size, c.checksum
                FROM chunk_replicas r
                JOIN chunks c ON c.id = r.chunk_id
                JOIN objects o ON o.id = c.object_id
                WHERE r.worker_id = ?
                ORDER BY o.created_at, c.chunk_index
                """, (rs, i) -> new ExpectedReplica(rs.getString(1), rs.getString(2), rs.getString(3),
                ObjectStatus.valueOf(rs.getString(4)), rs.getInt(5), rs.getInt(6), rs.getString(7)), workerId);
    }

    private static final String HEALTH_JOIN = """
            FROM chunks c
            JOIN objects o ON o.id = c.object_id AND o.status = 'AVAILABLE'
            LEFT JOIN chunk_replicas r ON r.chunk_id = c.id
            LEFT JOIN workers w ON w.id = r.worker_id AND w.status = 'UP'
            """;

    /** Ids of chunks (of AVAILABLE objects) whose number of replicas on UP workers differs from rf. */
    public List<String> chunkIdsNeedingAttention(int rf) {
        return jdbc.queryForList("SELECT c.id " + HEALTH_JOIN
                + "GROUP BY c.id, c.object_id, c.chunk_index HAVING COUNT(w.id) <> ? "
                + "ORDER BY c.object_id, c.chunk_index", String.class, rf);
    }

    /** healthy-replica-count -> number of chunks with that count (AVAILABLE objects only). */
    public Map<Integer, Long> healthHistogram() {
        Map<Integer, Long> out = new TreeMap<>();
        jdbc.query("SELECT healthy, COUNT(*) FROM (SELECT c.id, COUNT(w.id) AS healthy " + HEALTH_JOIN
                + "GROUP BY c.id) t GROUP BY healthy", rs -> { out.put(rs.getInt(1), rs.getLong(2)); });
        return out;
    }

    public List<ProblemChunk> problemChunks(int rf, int limit) {
        return jdbc.query("""
                SELECT c.id, c.object_id, o.name, c.chunk_index,
                       COUNT(w.id) AS healthy,
                       STRING_AGG(r.worker_id || ':' || w2.status, ',' ORDER BY r.id) AS replicas
                FROM chunks c
                JOIN objects o ON o.id = c.object_id AND o.status = 'AVAILABLE'
                LEFT JOIN chunk_replicas r ON r.chunk_id = c.id
                LEFT JOIN workers w ON w.id = r.worker_id AND w.status = 'UP'
                LEFT JOIN workers w2 ON w2.id = r.worker_id
                GROUP BY c.id, c.object_id, o.name, c.chunk_index
                HAVING COUNT(w.id) <> ?
                ORDER BY c.object_id, c.chunk_index
                LIMIT ?
                """, (rs, i) -> new ProblemChunk(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4),
                rs.getInt(5), parseReplicaStates(rs.getString(6))), rf, limit);
    }

    private static List<ReplicaState> parseReplicaStates(String agg) {
        if (agg == null || agg.isBlank()) return List.of();
        List<ReplicaState> out = new ArrayList<>();
        for (String part : agg.split(",")) {
            int i = part.lastIndexOf(':');
            out.add(new ReplicaState(part.substring(0, i), WorkerStatus.valueOf(part.substring(i + 1))));
        }
        return out;
    }

    private static Instant instant(ResultSet rs, int col) throws SQLException {
        Timestamp ts = rs.getTimestamp(col);
        return ts == null ? null : ts.toInstant();
    }
}

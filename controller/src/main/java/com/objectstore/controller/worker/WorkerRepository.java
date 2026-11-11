package com.objectstore.controller.worker;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Status transitions are single conditional UPDATE statements so that concurrent heartbeats, the
 * health monitor and the maintenance thread can never overwrite each other's changes.
 */
public interface WorkerRepository extends JpaRepository<WorkerNode, String> {

    List<WorkerNode> findByStatus(WorkerStatus status);

    List<WorkerNode> findAllByOrderByIdAsc();

    /** Insert or refresh a worker. A (re)registered worker always starts as RECOVERING until its chunks are verified. */
    @Modifying
    @Transactional
    @Query(value = """
            INSERT INTO workers (id, url, status, last_heartbeat, registered_at,
                                 reported_chunk_count, reported_used_bytes, reported_free_bytes)
            VALUES (:id, :url, 'RECOVERING', :now, :now, :chunkCount, :usedBytes, :freeBytes)
            ON CONFLICT (id) DO UPDATE SET
                url = EXCLUDED.url,
                status = 'RECOVERING',
                last_heartbeat = EXCLUDED.last_heartbeat,
                registered_at = EXCLUDED.registered_at,
                reported_chunk_count = EXCLUDED.reported_chunk_count,
                reported_used_bytes = EXCLUDED.reported_used_bytes,
                reported_free_bytes = EXCLUDED.reported_free_bytes
            """, nativeQuery = true)
    int upsertRegistration(@Param("id") String id, @Param("url") String url, @Param("now") Instant now,
                           @Param("chunkCount") long chunkCount, @Param("usedBytes") long usedBytes,
                           @Param("freeBytes") long freeBytes);

    /** Records a heartbeat. A DOWN worker that heartbeats again moves to RECOVERING (not straight to UP). */
    @Modifying
    @Transactional
    @Query(value = """
            UPDATE workers SET
                last_heartbeat = :now,
                status = CASE WHEN status = 'DOWN' THEN 'RECOVERING' ELSE status END,
                reported_chunk_count = :chunkCount,
                reported_used_bytes = :usedBytes,
                reported_free_bytes = :freeBytes
            WHERE id = :id
            """, nativeQuery = true)
    int recordHeartbeat(@Param("id") String id, @Param("now") Instant now, @Param("chunkCount") long chunkCount,
                        @Param("usedBytes") long usedBytes, @Param("freeBytes") long freeBytes);

    @Modifying
    @Transactional
    @Query(value = """
            UPDATE workers SET status = 'DOWN', down_since = :now
            WHERE id = :id AND status <> 'DOWN' AND last_heartbeat < :cutoff
            """, nativeQuery = true)
    int markDownIfStale(@Param("id") String id, @Param("cutoff") Instant cutoff, @Param("now") Instant now);

    @Modifying
    @Transactional
    @Query(value = "UPDATE workers SET status = 'UP', down_since = NULL WHERE id = :id AND status = 'RECOVERING'",
            nativeQuery = true)
    int markUpIfRecovering(@Param("id") String id);
}

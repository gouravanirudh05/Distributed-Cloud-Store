package com.objectstore.controller.object;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ObjectRepository extends JpaRepository<ObjectRecord, String> {

    Page<ObjectRecord> findByStatus(ObjectStatus status, Pageable pageable);

    List<ObjectRecord> findByStatusIn(Collection<ObjectStatus> statuses);

    List<ObjectRecord> findByStatusInAndLastModifiedBefore(Collection<ObjectStatus> statuses, Instant cutoff);

    @Query("select o.status from ObjectRecord o where o.id = :id")
    Optional<ObjectStatus> findStatusById(@Param("id") String id);

    /** Atomically claims an AVAILABLE object for deletion; returns 0 if it is missing or busy. */
    @Modifying
    @Transactional
    @Query(value = """
            UPDATE objects SET status = 'DELETING', version = version + 1, last_modified = :now
            WHERE id = :id AND status = 'AVAILABLE'
            """, nativeQuery = true)
    int markDeleting(@Param("id") String id, @Param("now") Instant now);

    /** Removes the object row; chunks, replicas and custom metadata cascade in the database. */
    @Modifying
    @Transactional
    @Query(value = "DELETE FROM objects WHERE id = :id", nativeQuery = true)
    int hardDelete(@Param("id") String id);
}

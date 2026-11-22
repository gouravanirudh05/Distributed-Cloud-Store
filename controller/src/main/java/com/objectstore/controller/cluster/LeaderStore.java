package com.objectstore.controller.cluster;

import java.io.IOException;
import java.util.Optional;

/** The coordination-service operations leader election needs (implemented with etcd; faked in tests). */
public interface LeaderStore {

    /** Creates a lease that expires after {@code ttlSeconds} unless kept alive. Returns the lease id. */
    long grantLease(int ttlSeconds) throws IOException;

    /** Renews a lease. Returns the remaining TTL in seconds, or 0 if the lease no longer exists. */
    long keepAlive(long leaseId) throws IOException;

    /**
     * Atomically creates {@code key = value} bound to the lease, only if the key does not exist.
     * Returns the store revision of the write (used as the leadership term), or -1 if the key already exists.
     */
    long createIfAbsent(String key, String value, long leaseId) throws IOException;

    /** Current value of the key, if any. */
    Optional<String> get(String key) throws IOException;

    /** Deletes the lease (and every key bound to it). */
    void revoke(long leaseId) throws IOException;

    /** Human-readable description, e.g. the endpoints. */
    String describe();
}

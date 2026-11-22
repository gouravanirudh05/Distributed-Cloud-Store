package com.objectstore.controller.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;

import java.net.ConnectException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.time.Instant;

/**
 * Tracks whether the metadata store (PostgreSQL) is reachable.
 *
 * <p>While it is down nothing can be served, so requests fail fast with 503. When it comes back, the time it
 * returned is remembered: worker heartbeats could not be recorded during the outage, so failure detection waits
 * a full heartbeat timeout after that moment before it marks any worker DOWN.
 */
@Component
public class MetadataStoreStatus {

    /** An outage that finished within this time is not interesting (e.g. one failed connection attempt). */
    private static final Duration REPORT_THRESHOLD = Duration.ofSeconds(2);

    private static final Logger log = LoggerFactory.getLogger(MetadataStoreStatus.class);

    private volatile Instant unavailableSince;
    private volatile Instant availableSince = Instant.EPOCH;

    /** A finished outage, reported once (to put it in the event log once the store is writable again). */
    public record Outage(Instant from, Instant to) {
        public Duration duration() { return Duration.between(from, to); }
    }
    private volatile Outage unreported;

    public void failure(Instant now) {
        if (unavailableSince == null) {
            unavailableSince = now;
            log.warn("Metadata store (PostgreSQL) is unreachable; API requests fail with 503 until it is back");
        }
    }

    public void success(Instant now) {
        Instant since = unavailableSince;
        if (since != null) {
            unavailableSince = null;
            availableSince = now;
            Outage o = new Outage(since, now);
            log.info("Metadata store reachable again after {} s", o.duration().toSeconds());
            if (o.duration().compareTo(REPORT_THRESHOLD) >= 0) unreported = o;
        }
    }

    public boolean isAvailable() { return unavailableSince == null; }

    /** When the store last came back after an outage (EPOCH if there was none). */
    public Instant availableSince() { return availableSince; }

    /** Returns a finished outage not yet reported, at most once. */
    public synchronized Outage takeUnreportedOutage() {
        Outage o = unreported;
        unreported = null;
        return o;
    }

    /** True if the exception (or a cause) means "cannot reach the database", as opposed to a query error. */
    public static boolean isConnectionFailure(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (c instanceof CannotCreateTransactionException || c instanceof DataAccessResourceFailureException
                    || c instanceof TransientDataAccessResourceException
                    || c instanceof SQLTransientConnectionException || c instanceof ConnectException) {
                return true;
            }
            // SQLSTATE class 08 = connection exception; 57P01-03 = server shutting down / crash / cannot connect now
            if (c instanceof SQLException s && s.getSQLState() != null
                    && (s.getSQLState().startsWith("08") || s.getSQLState().startsWith("57P"))) {
                return true;
            }
            if (c.getClass().getName().equals("org.hibernate.exception.JDBCConnectionException")) {
                return true;
            }
        }
        return false;
    }
}

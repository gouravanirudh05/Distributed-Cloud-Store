package com.objectstore.controller.cluster;

import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class MetadataStoreStatusTest {

    @Test
    void recognisesConnectionFailuresButNotQueryErrors() {
        assertTrue(MetadataStoreStatus.isConnectionFailure(
                new CannotCreateTransactionException("x", new SQLTransientConnectionException("pool timeout"))));
        assertTrue(MetadataStoreStatus.isConnectionFailure(
                new RuntimeException(new SQLException("refused", "08001"))));
        assertTrue(MetadataStoreStatus.isConnectionFailure(
                new RuntimeException(new SQLException("terminating connection", "57P01"))));
        assertFalse(MetadataStoreStatus.isConnectionFailure(
                new DataIntegrityViolationException("dup", new SQLException("duplicate key", "23505"))));
        assertFalse(MetadataStoreStatus.isConnectionFailure(new IllegalStateException("bug")));
    }

    @Test
    void remembersWhenTheStoreCameBackAndReportsTheOutageOnce() {
        MetadataStoreStatus s = new MetadataStoreStatus();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        assertTrue(s.isAvailable());
        assertEquals(Instant.EPOCH, s.availableSince());

        s.failure(t0);
        s.failure(t0.plusSeconds(5));                 // still the same outage
        assertFalse(s.isAvailable());

        s.success(t0.plusSeconds(20));
        assertTrue(s.isAvailable());
        assertEquals(t0.plusSeconds(20), s.availableSince());
        MetadataStoreStatus.Outage o = s.takeUnreportedOutage();
        assertNotNull(o);
        assertEquals(t0, o.from());
        assertEquals(20, o.duration().toSeconds());
        assertNull(s.takeUnreportedOutage(), "reported only once");

        s.success(t0.plusSeconds(30));                // no outage in between: nothing changes
        assertEquals(t0.plusSeconds(20), s.availableSince());
    }
}

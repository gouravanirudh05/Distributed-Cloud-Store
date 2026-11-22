package com.objectstore.controller.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class LeaderElectionTest {

    /** Clock the test moves by hand. */
    static class TestClock extends Clock {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        void advance(Duration d) { now = now.plus(d); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    /** In-memory etcd: keys bound to leases that expire on the shared clock. */
    static class FakeEtcd {
        final TestClock clock;
        final Map<Long, Instant> leases = new HashMap<>();
        final Map<Long, Integer> ttls = new HashMap<>();
        final Map<String, String> values = new HashMap<>();
        final Map<String, Long> keyLease = new HashMap<>();
        long nextId = 100, revision = 1;
        FakeEtcd(TestClock clock) { this.clock = clock; }

        void expire() {
            leases.entrySet().removeIf(e -> !e.getValue().isAfter(clock.instant()));
            keyLease.entrySet().removeIf(e -> {
                boolean gone = !leases.containsKey(e.getValue());
                if (gone) values.remove(e.getKey());
                return gone;
            });
        }

        /** A client view of the store; {@code down} simulates this client losing contact with etcd. */
        class Client implements LeaderStore {
            boolean down;
            void check() throws IOException { if (down) throw new IOException("unreachable"); expire(); }
            public long grantLease(int ttl) throws IOException {
                check();
                long id = nextId++;
                leases.put(id, clock.instant().plusSeconds(ttl));
                ttls.put(id, ttl);
                return id;
            }
            public long keepAlive(long id) throws IOException {
                check();
                if (!leases.containsKey(id)) return 0;
                leases.put(id, clock.instant().plusSeconds(ttls.get(id)));
                return ttls.get(id);
            }
            public long createIfAbsent(String key, String value, long lease) throws IOException {
                check();
                if (values.containsKey(key)) return -1;
                values.put(key, value);
                keyLease.put(key, lease);
                return ++revision;
            }
            public Optional<String> get(String key) throws IOException { check(); return Optional.ofNullable(values.get(key)); }
            public void revoke(long id) throws IOException { check(); leases.remove(id); expire(); }
            public String describe() { return "fake"; }
        }
    }

    TestClock clock;
    FakeEtcd etcd;
    FakeEtcd.Client c1, c2;
    LeaderElection e1, e2;
    List<LeaderElection.LeadershipChanged> changes;

    @BeforeEach
    void setUp() {
        clock = new TestClock();
        etcd = new FakeEtcd(clock);
        c1 = etcd.new Client();
        c2 = etcd.new Client();
        changes = new ArrayList<>();
        e1 = new LeaderElection(c1, "controller1", 5, ev -> changes.add((LeaderElection.LeadershipChanged) ev), clock);
        e2 = new LeaderElection(c2, "controller2", 5, ev -> changes.add((LeaderElection.LeadershipChanged) ev), clock);
    }

    /** Advance time second by second, ticking both instances like the real 1 s loop. */
    void run(int seconds) {
        for (int i = 0; i < seconds; i++) {
            clock.advance(Duration.ofSeconds(1));
            e1.tick();
            e2.tick();
        }
    }

    @Test
    void exactlyOneLeaderIsElected() {
        e1.tick();
        e2.tick();
        assertTrue(e1.isLeader());
        assertFalse(e2.isLeader());
        assertEquals(Optional.of("controller1"), e2.knownLeader());
        run(30);   // the leader keeps renewing; nothing changes
        assertTrue(e1.isLeader());
        assertFalse(e2.isLeader());
        assertEquals(1, changes.size());
    }

    @Test
    void standbyTakesOverWhenLeaderStopsRenewing() {
        e1.tick();
        e2.tick();
        long firstTerm = e1.term();
        c1.down = true;            // leader loses contact with etcd (or crashes)
        boolean overlap = false;
        Duration takeover = null;
        for (int i = 1; i <= 10 && takeover == null; i++) {
            run(1);
            overlap |= e1.isLeader() && e2.isLeader();
            if (e2.isLeader()) takeover = Duration.ofSeconds(i);
        }
        assertFalse(overlap, "the two instances must never be leader at the same time");
        assertNotNull(takeover, "standby should take over");
        assertTrue(takeover.toSeconds() <= 6, "takeover within the lease TTL plus one tick, was " + takeover);
        assertFalse(e1.isLeader());
        assertTrue(e2.term() > firstTerm, "a new leadership term");
        assertEquals("controller1", changes.get(changes.size() - 1).previousLeader());
    }

    @Test
    void leaderStopsActingBeforeItsLeaseCanExpire() {
        e1.tick();
        c1.down = true;
        clock.advance(Duration.ofMillis(2900));
        assertTrue(e1.isLeader());
        clock.advance(Duration.ofMillis(200));   // 3.1 s without renewal > 0.6 x 5 s
        assertFalse(e1.isLeader(), "must stop acting without waiting for its next tick");
    }

    @Test
    void gracefulStopReleasesLeadershipImmediately() {
        e1.tick();
        e2.tick();
        e1.stop();
        clock.advance(Duration.ofMillis(100));
        e2.tick();
        assertTrue(e2.isLeader());
    }

    @Test
    void formerLeaderBecomesStandbyWhenItReturns() {
        e1.tick();
        e2.tick();
        c1.down = true;
        run(8);
        assertTrue(e2.isLeader());
        c1.down = false;
        run(5);
        assertTrue(e2.isLeader());
        assertFalse(e1.isLeader());
        assertEquals(Optional.of("controller2"), e1.knownLeader());
    }

    @Test
    void standaloneModeIsAlwaysLeader() {
        LeaderElection solo = new LeaderElection(null, "controller", 5, ev -> {}, clock);
        solo.start();
        clock.advance(Duration.ofHours(1));
        assertTrue(solo.isLeader());
    }
}

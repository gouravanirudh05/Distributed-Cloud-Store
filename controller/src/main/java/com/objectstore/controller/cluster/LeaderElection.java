package com.objectstore.controller.cluster;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Active/standby controller election through etcd.
 *
 * <p>Several controller instances may run; exactly one is the <b>leader</b> (the active controller) and the others
 * are standbys. The leader holds the key {@value #KEY} in etcd, bound to a lease with a short TTL. It renews the
 * lease every second. If the leader dies or loses contact with etcd, it stops renewing, etcd deletes the key when
 * the lease expires, and a standby atomically creates the key and takes over.
 *
 * <p>Safety: the leader only considers itself leader while its last successful renewal is younger than
 * {@code 0.6 x TTL}. A standby can only win after the lease has fully expired ({@code TTL} after that renewal),
 * so the old leader has always stopped acting before a new one starts.
 *
 * <p>Without {@code election.etcd-endpoints} the controller runs standalone and is always the leader.
 */
@Component
public class LeaderElection {

    public static final String KEY = "objectstore/controller/leader";

    /** Published (synchronously, on the election thread) whenever this instance gains or loses leadership. */
    public record LeadershipChanged(boolean leader, String instanceId, long term, String previousLeader, String reason) {}

    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

    private final LeaderStore store;           // null = standalone
    private final String instanceId;
    private final int ttlSeconds;
    private final Duration safeWindow;
    private final ApplicationEventPublisher publisher;
    private final Clock clock;
    private final ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "leader-election");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean leader;
    private volatile Instant lastRenewal = Instant.EPOCH;
    private volatile Instant leaderSince;
    private volatile long term;
    private volatile String knownLeader;
    private long leaseId;
    private boolean storeReachable = true;

    @Autowired
    public LeaderElection(@Value("${election.etcd-endpoints:}") String endpoints,
                          @Value("${election.instance-id:controller}") String instanceId,
                          @Value("${election.lease-ttl-seconds:5}") int ttlSeconds,
                          ApplicationEventPublisher publisher) {
        this(endpoints == null || endpoints.isBlank() ? null
                        : new EtcdLeaderStore(Arrays.stream(endpoints.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList()),
                instanceId, ttlSeconds, publisher, Clock.systemUTC());
    }

    LeaderElection(LeaderStore store, String instanceId, int ttlSeconds, ApplicationEventPublisher publisher, Clock clock) {
        if (ttlSeconds < 2) throw new IllegalArgumentException("lease TTL must be at least 2 s");
        this.store = store;
        this.instanceId = instanceId;
        this.ttlSeconds = ttlSeconds;
        this.safeWindow = Duration.ofMillis(ttlSeconds * 600L);
        this.publisher = publisher;
        this.clock = clock;
    }

    /** Starts campaigning once the application (schema, web server) is fully up. */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (store == null) {
            log.info("No etcd endpoints configured: controller {} runs standalone and is always the active controller", instanceId);
            becomeLeader(0, null);
            return;
        }
        log.info("Controller {} joining leader election via {} (lease TTL {} s)", instanceId, store.describe(), ttlSeconds);
        exec.scheduleWithFixedDelay(this::safeTick, 0, 1, TimeUnit.SECONDS);
    }

    /** On a graceful stop, give up leadership at once so a standby takes over without waiting for the TTL. */
    @PreDestroy
    public void stop() {
        exec.shutdownNow();
        if (store != null && leader) {
            leader = false;
            try {
                store.revoke(leaseId);
                log.info("Controller {} shutting down: released leadership", instanceId);
            } catch (Exception e) {
                log.warn("Could not release leadership on shutdown ({}); a standby takes over when the lease expires", e.getMessage());
            }
        }
    }

    /** True only while this instance is the leader and its lease is known to be valid. */
    public boolean isLeader() {
        if (!leader) return false;
        return store == null || Duration.between(lastRenewal, clock.instant()).compareTo(safeWindow) < 0;
    }

    public String instanceId() { return instanceId; }
    public long term() { return term; }
    public Instant leaderSince() { return leader ? leaderSince : null; }
    /** The leader as last seen in etcd (may be this instance). */
    public Optional<String> knownLeader() { return Optional.ofNullable(knownLeader); }
    public String backend() { return store == null ? "standalone" : store.describe(); }
    public int leaseTtlSeconds() { return ttlSeconds; }

    private void safeTick() {
        try {
            tick();
        } catch (Throwable t) {
            log.error("Leader election tick failed", t);
        }
    }

    synchronized void tick() {
        try {
            if (leader) {
                renew();
            } else {
                campaign();
            }
            if (!storeReachable) {
                storeReachable = true;
                log.info("etcd reachable again");
            }
        } catch (Exception e) {
            if (storeReachable) {
                storeReachable = false;
                log.warn("Cannot reach etcd: {}", e.getMessage());
            }
        }
        // whether or not etcd answered: a leader that could not renew in time must stop acting
        if (leader && !isLeader()) {
            stepDown("lease not renewed for " + Duration.between(lastRenewal, clock.instant()).toMillis() + " ms");
        }
    }

    private void renew() throws Exception {
        Instant sent = clock.instant();
        long ttl = store.keepAlive(leaseId);
        if (ttl <= 0) {
            stepDown("leadership lease expired");
            return;
        }
        lastRenewal = sent;
    }

    private void campaign() throws Exception {
        Optional<String> holder = store.get(KEY);
        if (holder.isPresent()) {
            if (!holder.get().equals(knownLeader)) {
                log.info("Standby: the active controller is {}", holder.get());
            }
            knownLeader = holder.get();
            return;
        }
        String previous = knownLeader;
        Instant sent = clock.instant();
        long lease = store.grantLease(ttlSeconds);
        long revision = store.createIfAbsent(KEY, instanceId, lease);
        if (revision < 0) {
            try { store.revoke(lease); } catch (Exception ignored) { /* expires on its own */ }
            return;
        }
        leaseId = lease;
        lastRenewal = sent;
        becomeLeader(revision, previous);
    }

    private void becomeLeader(long revision, String previous) {
        term = revision;
        leaderSince = clock.instant();
        knownLeader = instanceId;
        leader = true;
        log.info("Controller {} is now the ACTIVE controller (term {}{})", instanceId, revision,
                previous == null ? "" : ", previous leader " + previous);
        publisher.publishEvent(new LeadershipChanged(true, instanceId, revision, previous,
                previous == null ? "elected" : "took over from " + previous));
    }

    private void stepDown(String reason) {
        leader = false;
        leaseId = 0;
        knownLeader = null;
        log.warn("Controller {} is no longer the active controller: {}", instanceId, reason);
        publisher.publishEvent(new LeadershipChanged(false, instanceId, term, instanceId, reason));
    }
}

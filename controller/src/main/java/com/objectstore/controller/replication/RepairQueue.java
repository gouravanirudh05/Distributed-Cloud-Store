package com.objectstore.controller.replication;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Bad replicas found while serving reads (corrupted or missing) are queued here and repaired by the
 * maintenance thread, so a download never blocks on repair work.
 */
@Component
public class RepairQueue {

    public record RepairRequest(String chunkId, String workerId, String reason) {}

    private final Queue<RepairRequest> queue = new ConcurrentLinkedQueue<>();
    private final Set<String> pending = ConcurrentHashMap.newKeySet();
    private volatile Runnable listener = () -> {};
    /** Replicas whose repair failed because no valid copy exists anywhere; re-reporting them is suppressed for a while. */
    private final ConcurrentHashMap<String, Instant> unrepairable = new ConcurrentHashMap<>();
    private static final Duration SUPPRESS_FOR = Duration.ofMinutes(5);

    public void markUnrepairable(String chunkId, String workerId) {
        unrepairable.put(chunkId + "@" + workerId, Instant.now());
    }

    public void clearUnrepairable(String chunkId, String workerId) {
        unrepairable.remove(chunkId + "@" + workerId);
    }

    /** True if this replica was found unrepairable recently (avoids an endless detect/repair/fail loop). */
    public boolean isSuppressed(String chunkId, String workerId) {
        Instant since = unrepairable.get(chunkId + "@" + workerId);
        if (since == null) return false;
        if (since.plus(SUPPRESS_FOR).isBefore(Instant.now())) {
            unrepairable.remove(chunkId + "@" + workerId);
            return false;
        }
        return true;
    }

    public void submit(String chunkId, String workerId, String reason) {
        if (isSuppressed(chunkId, workerId)) return;
        if (pending.add(chunkId + "@" + workerId)) {
            queue.add(new RepairRequest(chunkId, workerId, reason));
            listener.run();
        }
    }

    public List<RepairRequest> drain() {
        List<RepairRequest> out = new ArrayList<>();
        RepairRequest r;
        while ((r = queue.poll()) != null) {
            pending.remove(r.chunkId() + "@" + r.workerId());
            out.add(r);
        }
        return out;
    }

    void setListener(Runnable listener) {
        this.listener = listener;
    }
}

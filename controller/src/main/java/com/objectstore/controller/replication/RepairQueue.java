package com.objectstore.controller.replication;

import org.springframework.stereotype.Component;

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

    public void submit(String chunkId, String workerId, String reason) {
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

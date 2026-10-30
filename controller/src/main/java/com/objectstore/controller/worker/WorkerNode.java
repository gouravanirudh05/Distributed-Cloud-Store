package com.objectstore.controller.worker;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "workers")
public class WorkerNode {
    @Id
    private String id;            // "worker1"
    private String url;           // "http://worker1:8081"
    @Enumerated(EnumType.STRING)
    private WorkerStatus status;
    private Instant lastHeartbeat;

    protected WorkerNode() {}     // JPA needs a no-arg constructor

    public WorkerNode(String id, String url) {
        this.id = id;
        this.url = url;
    }

    public String getId() { return id; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public WorkerStatus getStatus() { return status; }
    public void setStatus(WorkerStatus status) { this.status = status; }
    public Instant getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(Instant lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
}

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
    private Instant downSince;
    private Instant registeredAt;
    private Long reportedChunkCount;
    private Long reportedUsedBytes;
    private Long reportedFreeBytes;

    protected WorkerNode() {}     // JPA needs a no-arg constructor

    public String getId() { return id; }
    public String getUrl() { return url; }
    public WorkerStatus getStatus() { return status; }
    public Instant getLastHeartbeat() { return lastHeartbeat; }
    public Instant getDownSince() { return downSince; }
    public Instant getRegisteredAt() { return registeredAt; }
    public Long getReportedChunkCount() { return reportedChunkCount; }
    public Long getReportedUsedBytes() { return reportedUsedBytes; }
    public Long getReportedFreeBytes() { return reportedFreeBytes; }
    public boolean isUp() { return status == WorkerStatus.UP; }
}

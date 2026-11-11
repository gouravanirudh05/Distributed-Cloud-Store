package com.objectstore.controller.object;

import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.replication.RepairQueue;
import com.objectstore.controller.worker.WorkerClientService;
import com.objectstore.controller.worker.WorkerClientService.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Reads a chunk from its replicas in order, verifying every copy against the SHA-256 recorded at upload
 * time. A corrupted or missing copy is never returned: it is reported, queued for repair, and the next
 * replica is tried.
 */
@Component
public class ChunkReader {

    private static final Logger log = LoggerFactory.getLogger(ChunkReader.class);

    public record ReadResult(byte[] data, String servedBy) {}

    private final WorkerClientService workers;
    private final EventLog events;
    private final RepairQueue repairs;

    public ChunkReader(WorkerClientService workers, EventLog events, RepairQueue repairs) {
        this.workers = workers;
        this.events = events;
        this.repairs = repairs;
    }

    public Optional<ReadResult> readVerified(ChunkDao.ChunkRow chunk, List<ChunkDao.ReplicaRow> replicas, String context) {
        for (ChunkDao.ReplicaRow r : replicas) {
            if (!r.isUp()) continue;
            FetchResult result = workers.getChunk(r.workerUrl(), chunk.id());
            switch (result) {
                case Ok ok -> {
                    String actual = Checksums.sha256Hex(ok.data());
                    if (actual.equalsIgnoreCase(chunk.checksum()) && ok.data().length == chunk.size()) {
                        return Optional.of(new ReadResult(ok.data(), r.workerId()));
                    }
                    events.chunk(EventType.CHUNK_CORRUPTED, r.workerId(), chunk.id(), String.format(
                            "Corrupted replica of chunk %d on %s detected during %s: expected sha256 %s, got %s. "
                                    + "Copy rejected, trying next replica; repair scheduled.",
                            chunk.index(), r.workerId(), context, Checksums.shortHash(chunk.checksum()),
                            Checksums.shortHash(actual)));
                    repairs.submit(chunk.id(), r.workerId(), "corrupted");
                }
                case Missing m -> {
                    events.chunk(EventType.CHUNK_MISSING, r.workerId(), chunk.id(), String.format(
                            "Replica of chunk %d is missing on %s (found during %s); trying next replica; repair scheduled.",
                            chunk.index(), r.workerId(), context));
                    repairs.submit(chunk.id(), r.workerId(), "missing");
                }
                case Unreachable u -> log.warn("Chunk {} replica on {} unreachable during {}: {}",
                        chunk.id(), r.workerId(), context, u.reason());
                case Failed f -> log.warn("Chunk {} replica on {} failed during {}: {}",
                        chunk.id(), r.workerId(), context, f.reason());
            }
        }
        return Optional.empty();
    }
}

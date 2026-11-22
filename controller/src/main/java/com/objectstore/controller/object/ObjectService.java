package com.objectstore.controller.object;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.controller.api.ApiException;
import com.objectstore.controller.api.ApiModels.*;
import com.objectstore.controller.config.StoreProperties;
import com.objectstore.controller.events.EventLog;
import com.objectstore.controller.events.EventType;
import com.objectstore.controller.worker.WorkerClientService;
import com.objectstore.controller.worker.WorkerException;
import com.objectstore.controller.worker.WorkerNode;
import com.objectstore.controller.worker.WorkerRepository;
import com.objectstore.controller.worker.WorkerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.MediaTypeFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Object operations behind the public REST API. The controller is the only component that knows where
 * chunks live; clients only ever see object ids.
 */
@Service
public class ObjectService {

    /** Attempts for a metadata PATCH that keeps losing the optimistic-lock race. */
    private static final int PATCH_RETRIES = 12;

    private static final Logger log = LoggerFactory.getLogger(ObjectService.class);
    private static final int MAX_NAME_LENGTH = 1024;

    /** What a download needs, captured up front so streaming does not hold a DB transaction. */
    public record DownloadPlan(String objectId, String name, long size, String contentType, String checksum,
                               List<ChunkDao.ChunkRow> chunks, Map<String, List<ChunkDao.ReplicaRow>> replicas) {}

    private final ObjectRepository objects;
    private final ChunkDao chunks;
    private final WorkerRepository workers;
    private final WorkerClientService workerClient;
    private final ChunkPlacementService placement;
    private final ChunkReader reader;
    private final EventLog events;
    private final StoreProperties props;
    private final ExecutorService io;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;

    public ObjectService(ObjectRepository objects, ChunkDao chunks, WorkerRepository workers,
                         WorkerClientService workerClient, ChunkPlacementService placement, ChunkReader reader,
                         EventLog events, StoreProperties props, ExecutorService workerIoExecutor,
                         TransactionTemplate tx, ObjectMapper mapper) {
        this.objects = objects;
        this.chunks = chunks;
        this.workers = workers;
        this.workerClient = workerClient;
        this.placement = placement;
        this.reader = reader;
        this.events = events;
        this.props = props;
        this.io = workerIoExecutor;
        this.tx = tx;
        this.mapper = mapper;
    }

    // ------------------------------------------------------------------ upload

    /**
     * Splits the upload into chunk-size pieces and writes each piece to replication-factor distinct live
     * workers (in parallel). If fewer workers are available the chunk is stored with fewer copies
     * ("degraded") and the maintenance loop adds the missing copies later. Fails with 503 only if a chunk
     * cannot be stored on any worker.
     */
    public ObjectMetadata upload(MultipartFile file, String metadataJson) {
        Map<String, String> custom = CustomMetadata.parseUploadField(metadataJson, mapper);
        if (workers.findByStatus(WorkerStatus.UP).isEmpty()) {
            throw ApiException.unavailable("NO_WORKERS_AVAILABLE",
                    "No storage workers are available; the object cannot be stored right now");
        }

        String id = UUID.randomUUID().toString();
        String name = resolveName(file.getOriginalFilename(), id);
        String contentType = resolveContentType(file.getContentType(), name);
        objects.save(ObjectRecord.newUpload(id, name, contentType, props.chunkSize(), Instant.now(), custom));

        int rf = props.replicationFactor();
        List<ChunkDao.ChunkRow> chunkRows = new ArrayList<>();
        List<ChunkDao.NewReplica> replicaRows = new ArrayList<>();
        Map<String, String> urlByWorker = new HashMap<>();
        Map<String, Long> load = new HashMap<>(chunks.replicaCountsByWorker());
        int degraded = 0;
        long total = 0;

        try (InputStream in = file.getInputStream()) {
            MessageDigest objectDigest = Checksums.sha256();
            int index = 0;
            while (true) {
                byte[] data = in.readNBytes(props.chunkSize());
                if (data.length == 0) break;
                objectDigest.update(data);
                total += data.length;

                String chunkId = ChunkIds.of(id, index);
                String checksum = Checksums.sha256Hex(data);
                List<WorkerNode> live = workers.findByStatus(WorkerStatus.UP);
                live.forEach(w -> urlByWorker.put(w.getId(), w.getUrl()));

                List<String> stored = storeReplicas(chunkId, data, checksum, live, load, index);
                stored.forEach(w -> replicaRows.add(new ChunkDao.NewReplica(chunkId, w)));
                if (stored.isEmpty()) {
                    throw ApiException.unavailable("STORAGE_UNAVAILABLE",
                            "Chunk " + index + " could not be stored on any worker; upload aborted");
                }
                chunkRows.add(new ChunkDao.ChunkRow(chunkId, id, index, data.length, checksum));
                if (stored.size() < rf) degraded++;
                index++;
            }

            final long size = total;
            final int chunkCount = index;
            final String objectChecksum = Checksums.hex(objectDigest.digest());
            tx.executeWithoutResult(s -> {
                chunks.insertChunks(chunkRows);
                chunks.insertReplicas(replicaRows, Instant.now());
                ObjectRecord rec = objects.findById(id).orElseThrow(
                        () -> ApiException.internal("Upload record vanished (cleaned up as stale?)"));
                rec.completeUpload(size, objectChecksum, chunkCount, Instant.now());
            });
        } catch (IOException e) {
            abortUpload(id, name, replicaRows, urlByWorker, e);
            throw ApiException.internal("Failed to read uploaded data: " + e.getMessage());
        } catch (RuntimeException e) {
            abortUpload(id, name, replicaRows, urlByWorker, e);
            throw e;
        }

        events.record(EventType.OBJECT_UPLOADED, null, id, null, String.format(
                "Stored '%s' (%d bytes, %s) as %d chunk(s) x %d replica(s)",
                name, total, contentType, chunkRows.size(), rf));
        if (degraded > 0) {
            events.record(EventType.UPLOAD_DEGRADED, null, id, null, String.format(
                    "'%s': %d of %d chunk(s) stored with fewer than %d copies because not enough workers were up; "
                            + "missing copies will be added automatically when workers become available",
                    name, degraded, chunkRows.size(), rf));
        }
        return getMetadata(id);
    }

    /**
     * Writes one chunk through an HDFS-style pipeline: the chunk goes to the first chosen worker only, which
     * stores it and forwards it to the second, and so on; acknowledgements come back up the chain. If the head
     * of the pipeline fails, the next-best worker becomes the head; if a downstream worker fails, the
     * missing copy is written through a new pipeline built from workers not yet tried.
     */
    private List<String> storeReplicas(String chunkId, byte[] data, String checksum, List<WorkerNode> live,
                                       Map<String, Long> load, int rotation) {
        Map<String, String> urls = new HashMap<>();
        live.forEach(w -> urls.put(w.getId(), w.getUrl()));
        List<String> ranked = placement.rank(urls.keySet(), load, rotation);

        int rf = props.replicationFactor();
        List<String> stored = new ArrayList<>();
        Set<String> tried = new HashSet<>();
        while (stored.size() < rf) {
            List<String> candidates = ranked.stream().filter(w -> !tried.contains(w)).toList();
            if (candidates.isEmpty()) break;
            List<String> pipe = candidates.subList(0, Math.min(rf - stored.size(), candidates.size()));
            String head = pipe.get(0);
            List<WorkerClientService.PipelineTarget> downstream = pipe.subList(1, pipe.size()).stream()
                    .map(w -> new WorkerClientService.PipelineTarget(w, urls.get(w))).toList();
            tried.add(head);
            try {
                List<String> acks = workerClient.putChunkPipeline(urls.get(head), chunkId, data, checksum, downstream);
                tried.addAll(pipe);   // every pipeline member was attempted
                for (String w : acks) if (!stored.contains(w) && urls.containsKey(w)) stored.add(w);
            } catch (WorkerException e) {
                log.warn("Pipeline write of chunk {} via {} failed: {}", chunkId, head, e.getMessage());
            }
        }
        stored.forEach(w -> load.merge(w, 1L, Long::sum));
        return stored;
    }

    private void abortUpload(String id, String name, List<ChunkDao.NewReplica> written,
                             Map<String, String> urlByWorker, Exception cause) {
        for (ChunkDao.NewReplica r : written) {
            String url = urlByWorker.get(r.workerId());
            if (url != null) workerClient.deleteChunk(url, r.chunkId());
        }
        try {
            objects.hardDelete(id);
        } catch (Exception e) {
            log.warn("Could not remove aborted upload {}: {}", id, e.getMessage());
        }
        events.record(EventType.UPLOAD_FAILED, null, id, null,
                "Upload of '" + name + "' failed and was rolled back: " + cause.getMessage());
    }

    static String resolveName(String original, String fallback) {
        if (original == null) return fallback;
        String n = original.replace('\\', '/');
        n = n.substring(n.lastIndexOf('/') + 1).trim();
        if (n.isEmpty()) return fallback;
        return n.length() > MAX_NAME_LENGTH ? n.substring(0, MAX_NAME_LENGTH) : n;
    }

    /** Uses the part's declared Content-Type unless it is missing/generic, then guesses from the file extension. */
    static String resolveContentType(String declared, String name) {
        if (declared != null && !declared.isBlank()) {
            try {
                MediaType mt = MediaType.parseMediaType(declared);
                if (!mt.equalsTypeAndSubtype(MediaType.APPLICATION_OCTET_STREAM)) {
                    return mt.toString();
                }
            } catch (InvalidMediaTypeException ignored) {
                // fall through to extension-based detection
            }
        }
        return MediaTypeFactory.getMediaType(name).map(MediaType::toString)
                .orElse(MediaType.APPLICATION_OCTET_STREAM_VALUE);
    }

    // ------------------------------------------------------------------ download

    public DownloadPlan prepareDownload(String id) {
        ObjectRecord rec = requireAvailable(id, "downloaded");
        List<ChunkDao.ChunkRow> cs = chunks.chunksOfObject(id);
        Map<String, List<ChunkDao.ReplicaRow>> reps = chunks.replicasOfObject(id);
        List<Integer> unavailable = cs.stream()
                .filter(c -> reps.getOrDefault(c.id(), List.of()).stream().noneMatch(ChunkDao.ReplicaRow::isUp))
                .map(ChunkDao.ChunkRow::index)
                .toList();
        if (!unavailable.isEmpty()) {
            throw ApiException.unavailable("DATA_UNAVAILABLE", String.format(
                    "Object '%s' is temporarily unavailable: no live worker holds a copy of chunk(s) %s. "
                            + "Retry once the workers holding them are back.", id, abbreviate(unavailable)));
        }
        return new DownloadPlan(rec.getId(), rec.getName(), rec.getSize(), rec.getContentType(), rec.getChecksum(),
                cs, reps);
    }

    /** Returns verified bytes of the i-th chunk, failing over between replicas. */
    public byte[] readChunk(DownloadPlan plan, int i) {
        ChunkDao.ChunkRow c = plan.chunks().get(i);
        return reader.readVerified(c, plan.replicas().getOrDefault(c.id(), List.of()), "download")
                .map(ChunkReader.ReadResult::data)
                .orElseThrow(() -> ApiException.unavailable("DATA_UNAVAILABLE", String.format(
                        "No valid copy of chunk %d of object '%s' is available (copies unreachable or corrupted)",
                        c.index(), plan.objectId())));
    }

    // ------------------------------------------------------------------ metadata

    public ObjectMetadata getMetadata(String id) {
        ObjectRecord rec = objects.findById(id).orElseThrow(() -> ApiException.objectNotFound(id));
        return toView(rec);
    }

    public ObjectMetadata patchMetadata(String id, JsonNode body) {
        Map<String, String> patch = CustomMetadata.parsePatch(body);
        Boolean changed = null;
        for (int attempt = 1; changed == null; attempt++) {
            try {
                changed = tx.execute(s -> {
                    ObjectRecord rec = objects.findById(id).orElseThrow(() -> ApiException.objectNotFound(id));
                    requireState(rec, "modified");
                    boolean c = CustomMetadata.applyPatch(rec.getCustomMetadata(), patch);
                    if (c) rec.setLastModified(Instant.now());
                    return c;
                });
            } catch (OptimisticLockingFailureException e) {
                // Another request changed the object first. A merge patch can simply be re-applied to the new
                // state, so retry a few times; only a persistent conflict is reported to the client (409).
                if (attempt >= PATCH_RETRIES) throw e;
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(5, 25) * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        if (Boolean.TRUE.equals(changed)) {
            events.record(EventType.METADATA_UPDATED, null, id, null,
                    "Custom metadata updated (no object data re-uploaded): " + patch);
        }
        return getMetadata(id);
    }

    private ObjectMetadata toView(ObjectRecord rec) {
        int rf = props.replicationFactor();
        List<ChunkDao.ChunkRow> cs = chunks.chunksOfObject(rec.getId());
        Map<String, List<ChunkDao.ReplicaRow>> reps = chunks.replicasOfObject(rec.getId());
        int healthy = 0, under = 0, over = 0, unavailable = 0;
        List<ChunkView> views = new ArrayList<>(cs.size());
        for (ChunkDao.ChunkRow c : cs) {
            List<ChunkDao.ReplicaRow> rs = reps.getOrDefault(c.id(), List.of());
            int up = (int) rs.stream().filter(ChunkDao.ReplicaRow::isUp).count();
            if (up == 0) unavailable++;
            else if (up < rf) under++;
            else if (up > rf) over++;
            else healthy++;
            views.add(new ChunkView(c.id(), c.index(), c.size(), c.checksum(), up,
                    rs.stream().map(r -> new ReplicaView(r.workerId(), r.workerStatus().name(), r.createdAt())).toList()));
        }
        String status = unavailable > 0 ? "UNAVAILABLE" : under > 0 ? "DEGRADED" : over > 0 ? "OVER_REPLICATED" : "HEALTHY";
        return new ObjectMetadata(rec.getId(), rec.getName(), rec.getSize(), rec.getContentType(),
                rec.getChecksumAlgorithm(), rec.getChecksum(), rec.getCreatedAt(), rec.getLastModified(),
                rec.getStatus().name(), rec.getChunkSize(), rec.getChunkCount(), rf,
                new TreeMap<>(rec.getCustomMetadata()),
                new ReplicationSummary(status, healthy, under, over, unavailable), views);
    }

    // ------------------------------------------------------------------ delete

    public void delete(String id) {
        ObjectRecord rec = requireAvailable(id, "deleted");
        if (objects.markDeleting(id, Instant.now()) == 0) {
            // lost a race with another delete/patch
            ObjectStatus now = objects.findStatusById(id).orElseThrow(() -> ApiException.objectNotFound(id));
            throw ApiException.conflict("OBJECT_BUSY", "Object '" + id + "' is currently " + now.name().toLowerCase());
        }
        Map<String, List<ChunkDao.ReplicaRow>> reps = chunks.replicasOfObject(id);
        List<Future<Boolean>> deletes = new ArrayList<>();
        int skipped = 0;
        for (List<ChunkDao.ReplicaRow> rs : reps.values()) {
            for (ChunkDao.ReplicaRow r : rs) {
                if (r.workerStatus() == WorkerStatus.DOWN) {
                    skipped++;   // orphan; removed by the integrity scan when that worker returns
                    continue;
                }
                deletes.add(io.submit(() -> workerClient.deleteChunk(r.workerUrl(), r.chunkId())));
            }
        }
        int removed = 0, failed = 0;
        for (Future<Boolean> f : deletes) {
            try {
                if (Boolean.TRUE.equals(f.get())) removed++;
                else failed++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed++;
            } catch (ExecutionException e) {
                failed++;
            }
        }
        objects.hardDelete(id);
        events.record(EventType.OBJECT_DELETED, null, id, null, String.format(
                "Deleted '%s': %d chunk replica(s) removed from workers%s", rec.getName(), removed,
                (skipped + failed) > 0 ? String.format("; %d left on unavailable workers will be garbage-collected later", skipped + failed) : ""));
    }

    // ------------------------------------------------------------------ list

    public PageResponse<ObjectSummary> list(int page, int size) {
        if (page < 0) throw ApiException.badRequest("'page' must be >= 0");
        if (size < 1 || size > 1000) throw ApiException.badRequest("'size' must be between 1 and 1000");
        Page<ObjectRecord> p = objects.findByStatus(ObjectStatus.AVAILABLE,
                PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt").and(Sort.by("id"))));
        List<ObjectSummary> items = p.getContent().stream().map(r -> new ObjectSummary(r.getId(), r.getName(),
                r.getSize(), r.getContentType(), r.getChunkCount(), r.getCreatedAt(), r.getLastModified())).toList();
        return new PageResponse<>(items, page, size, p.getTotalElements(), p.getTotalPages());
    }

    // ------------------------------------------------------------------ housekeeping

    /**
     * Removes objects stuck in UPLOADING/DELETING (e.g. the controller crashed mid-operation). With
     * olderThan == null everything in those states is removed (used at startup: a single controller means
     * nothing can be in flight then). Chunks already written become orphans and are removed by the scrubber.
     */
    public int cleanupStaleOperations(Duration olderThan) {
        List<ObjectStatus> busy = List.of(ObjectStatus.UPLOADING, ObjectStatus.DELETING);
        List<ObjectRecord> stale = olderThan == null
                ? objects.findByStatusIn(busy)
                : objects.findByStatusInAndLastModifiedBefore(busy, Instant.now().minus(olderThan));
        for (ObjectRecord r : stale) {
            objects.hardDelete(r.getId());
            events.record(EventType.STALE_OPERATION_CLEANED, null, r.getId(), null, String.format(
                    "Removed '%s' left in state %s by an interrupted operation", r.getName(), r.getStatus()));
        }
        return stale.size();
    }

    // ------------------------------------------------------------------ helpers

    private ObjectRecord requireAvailable(String id, String action) {
        ObjectRecord rec = objects.findById(id).orElseThrow(() -> ApiException.objectNotFound(id));
        requireState(rec, action);
        return rec;
    }

    private static void requireState(ObjectRecord rec, String action) {
        switch (rec.getStatus()) {
            case AVAILABLE -> { }
            case UPLOADING -> throw ApiException.conflict("UPLOAD_IN_PROGRESS",
                    "Object '" + rec.getId() + "' is still being uploaded and cannot be " + action + " yet");
            case DELETING -> throw ApiException.conflict("DELETE_IN_PROGRESS",
                    "Object '" + rec.getId() + "' is being deleted");
        }
    }

    private static String abbreviate(List<Integer> indexes) {
        if (indexes.size() <= 10) return indexes.toString();
        return indexes.subList(0, 10) + " and " + (indexes.size() - 10) + " more";
    }
}

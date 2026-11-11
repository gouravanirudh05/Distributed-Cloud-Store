package com.objectstore.controller.object;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** System metadata of a stored object plus its custom (user-defined) key/value metadata. */
@Entity
@Table(name = "objects")
public class ObjectRecord {

    public static final String CHECKSUM_ALGORITHM = "SHA-256";

    @Id
    private String id;
    private String name;
    private long size;
    private String contentType;
    private String checksum;
    private String checksumAlgorithm;
    private int chunkSize;
    private int chunkCount;
    @Enumerated(EnumType.STRING)
    private ObjectStatus status;
    private Instant createdAt;
    private Instant lastModified;
    @Version
    private long version;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "object_custom_metadata", joinColumns = @JoinColumn(name = "object_id"))
    @MapKeyColumn(name = "meta_key")
    @Column(name = "meta_value")
    private Map<String, String> customMetadata = new HashMap<>();

    protected ObjectRecord() {}

    public static ObjectRecord newUpload(String id, String name, String contentType, int chunkSize,
                                         Instant now, Map<String, String> customMetadata) {
        ObjectRecord r = new ObjectRecord();
        r.id = id;
        r.name = name;
        r.contentType = contentType;
        r.chunkSize = chunkSize;
        r.checksumAlgorithm = CHECKSUM_ALGORITHM;
        r.status = ObjectStatus.UPLOADING;
        r.createdAt = now;
        r.lastModified = now;
        r.customMetadata = new HashMap<>(customMetadata);
        return r;
    }

    public void completeUpload(long size, String checksum, int chunkCount, Instant now) {
        this.size = size;
        this.checksum = checksum;
        this.chunkCount = chunkCount;
        this.status = ObjectStatus.AVAILABLE;
        this.createdAt = now;
        this.lastModified = now;
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public long getSize() { return size; }
    public String getContentType() { return contentType; }
    public String getChecksum() { return checksum; }
    public String getChecksumAlgorithm() { return checksumAlgorithm; }
    public int getChunkSize() { return chunkSize; }
    public int getChunkCount() { return chunkCount; }
    public ObjectStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getLastModified() { return lastModified; }
    public void setLastModified(Instant lastModified) { this.lastModified = lastModified; }
    public long getVersion() { return version; }
    public Map<String, String> getCustomMetadata() { return customMetadata; }
}

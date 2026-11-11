-- Metadata schema for the controller. Every statement is idempotent: this runs on each start.

CREATE TABLE IF NOT EXISTS workers (
    id                   VARCHAR(64)  PRIMARY KEY,
    url                  VARCHAR(255) NOT NULL,
    status               VARCHAR(16)  NOT NULL,
    last_heartbeat       TIMESTAMPTZ,
    down_since           TIMESTAMPTZ,
    registered_at        TIMESTAMPTZ,
    reported_chunk_count BIGINT,
    reported_used_bytes  BIGINT,
    reported_free_bytes  BIGINT
);
-- databases created by the early Hibernate-generated prototype: drop its enum check and add new columns
ALTER TABLE workers DROP CONSTRAINT IF EXISTS workers_status_check;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS down_since TIMESTAMPTZ;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS registered_at TIMESTAMPTZ;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS reported_chunk_count BIGINT;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS reported_used_bytes BIGINT;
ALTER TABLE workers ADD COLUMN IF NOT EXISTS reported_free_bytes BIGINT;

CREATE TABLE IF NOT EXISTS objects (
    id                 VARCHAR(36)   PRIMARY KEY,
    name               VARCHAR(1024) NOT NULL,
    size               BIGINT        NOT NULL,
    content_type       VARCHAR(255)  NOT NULL,
    checksum           VARCHAR(64),
    checksum_algorithm VARCHAR(16)   NOT NULL,
    chunk_size         INTEGER       NOT NULL,
    chunk_count        INTEGER       NOT NULL,
    status             VARCHAR(16)   NOT NULL,
    created_at         TIMESTAMPTZ   NOT NULL,
    last_modified      TIMESTAMPTZ   NOT NULL,
    version            BIGINT        NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_objects_status_created ON objects (status, created_at DESC);

CREATE TABLE IF NOT EXISTS object_custom_metadata (
    object_id  VARCHAR(36)   NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    meta_key   VARCHAR(128)  NOT NULL,
    meta_value VARCHAR(4096) NOT NULL,
    PRIMARY KEY (object_id, meta_key)
);

CREATE TABLE IF NOT EXISTS chunks (
    id          VARCHAR(64) PRIMARY KEY,               -- <objectId>_<6-digit index>
    object_id   VARCHAR(36) NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    chunk_index INTEGER     NOT NULL,
    size        INTEGER     NOT NULL,
    checksum    VARCHAR(64) NOT NULL,                  -- SHA-256 of the chunk bytes
    UNIQUE (object_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS chunk_replicas (
    id         BIGSERIAL   PRIMARY KEY,
    chunk_id   VARCHAR(64) NOT NULL REFERENCES chunks (id) ON DELETE CASCADE,
    worker_id  VARCHAR(64) NOT NULL REFERENCES workers (id),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (chunk_id, worker_id)
);
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON chunk_replicas (worker_id);

CREATE TABLE IF NOT EXISTS events (
    id        BIGSERIAL     PRIMARY KEY,
    ts        TIMESTAMPTZ   NOT NULL,
    type      VARCHAR(48)   NOT NULL,
    worker_id VARCHAR(64),
    object_id VARCHAR(36),
    chunk_id  VARCHAR(64),
    message   VARCHAR(2048) NOT NULL
);

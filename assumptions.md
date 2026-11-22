# Assumptions and design decisions

This file records how the project interprets the specification (*Mini Distributed Cloud Object Store*)
wherever the spec is silent or open to interpretation.

* Items marked **[confirmed]** were discussed and agreed with the team before implementation.
* The rest are defaults chosen by the implementation, with the reason.

## A. Decisions confirmed before implementation

| # | Topic | Decision |
|---|-------|----------|
| A1 | Clients ("1 or more") **[confirmed]** | Two clients: a **Java CLI** in its own container (`client`, command `objstore`) and a **browser UI** (`web`, nginx). Both use only the public REST API (plus the read-only admin API). |
| A2 | Restoring RF=2 after a worker failure **[confirmed]** | The controller waits a **30 s grace period** after a worker is marked DOWN, then copies each affected chunk from a verified live copy to another live worker. When the failed worker returns and its chunks are verified, the chunks have 3 live copies. The **newest (temporary) copy is removed**, so every chunk is back to exactly 2 and the original layout is restored. |
| A3 | Upload when fewer than 2 workers are UP **[confirmed]** | **Accepted, degraded.** Each chunk is stored on as many workers as are available (at least 1), an `UPLOAD_DEGRADED` event is logged, and the missing copies are added automatically as soon as another worker is UP. Only if **no** worker can store a chunk does the upload fail, with **503**. |
| A4 | Object names and 409 Conflict **[confirmed]** | **Duplicate object names are allowed.** Objects are identified only by their system-generated UUID. **409** is used for operations on an object that is still being uploaded or deleted, and for a metadata update that loses a race with a concurrent delete. Concurrent metadata PATCHes of one object are retried automatically (up to 12 times, merge patches can be re-applied), so 409 only appears if the contention persists. |
| A5 | When corruption is detected **[confirmed]** | **On every read**: each chunk copy is verified against its recorded SHA-256 before it is used. **Background scrub**: every 60 s, plus on demand via `POST /api/v1/admin/scrub`, every chunk on every UP worker is re-hashed and compared with the metadata. Also, before a returning worker is marked UP. |
| A6 | Custom metadata format **[confirmed]** | A flat JSON object of **string → string**. Non-string values are rejected with 400. PATCH is a **merge patch**: listed keys are added or updated, a key set to `null` is removed, and unlisted keys are unchanged. |
| A7 | Metadata store **[confirmed]** | **PostgreSQL** (separate container) is the controller's metadata database. It is the controller's backing store, not a second controller node. The system still has exactly 1 controller. |
| A8 | Deliverables **[confirmed]** | Code, README, this file, `demo.sh` (automated, narrated demonstrations with PASS/FAIL checks), JUnit unit tests, an end-to-end run of the full Docker stack, and git commits on a feature branch. |

## B. Interpretation of the specification

| # | Spec text | Interpretation |
|---|-----------|----------------|
| B1 | "1 MB" chunks | **1 MiB = 1,048,576 bytes.** The final chunk holds the remainder. A 0-byte object is allowed and has 0 chunks. |
| B2 | "Each chunk must have a unique identifier and its ordering … preserved" | Chunk id = `<objectId>_<6-digit index>` (e.g. `…_000004`). It is globally unique because object ids are UUIDs. The index is also stored explicitly, and the zero-padding means ids sort in chunk order. |
| B3 | "Unique Object ID" | A random **UUID v4**, assigned by the controller. Clients cannot choose it. |
| B4 | "Checksum" | **SHA-256**, for the whole object and for every chunk. The object checksum is returned in metadata and in the `X-Checksum-SHA256` / `ETag` download headers. |
| B5 | "Content type" | Taken from the `Content-Type` of the multipart `file` part. If it is missing or `application/octet-stream`, it is guessed from the file extension. If nothing matches, it is `application/octet-stream`. Downloads return it unchanged. |
| B6 | "Object name" | The original filename of the uploaded part, with any directory path removed. If no filename was sent, the object id is used as the name. |
| B7 | "Creation time and last modified time" | `createdAt` is the moment the upload completed. `lastModified` changes when custom metadata actually changes. A PATCH that changes nothing leaves it as is. Object data is immutable, so a new version is a new upload with a new id. |
| B8 | "Chunk information and storage locations" | The metadata response lists every chunk (id, index, size, SHA-256) and the worker ids holding its copies, with each worker's current status. Worker URLs are not exposed to clients ("the client should not need to know the worker locations"). |
| B9 | "Pagination may be supported" | `GET /api/v1/objects?page=<0-based>&size=<1..1000>` (default size 50), newest first. Only fully stored (`AVAILABLE`) objects are listed. |
| B10 | Error codes table | Only the listed codes are used for API errors: 200/201/204/400/404/409/500/503. An over-size upload and an unsupported `Content-Type` are reported as **400**, not 413/415. The exception is **405**, which Spring returns for a wrong HTTP method on an existing path. That is routing, not object-store semantics. Every error has a JSON body `{timestamp, status, error, code, message, path}`. |
| B11 | A malformed object id (not a UUID) | Treated as "object does not exist": **404**. |
| B12 | "Delete … and its associated stored data" | Copies on live workers are deleted immediately. Copies on a DOWN worker are removed as **orphans** when that worker returns: the recovery check deletes chunk files whose object no longer exists. The client gets 204 at once. |
| B13 | Internal API `GET /internal/workers/{worker_id}/chunks` | The spec does not say which side hosts it, so it is implemented on **both**. The **worker** returns its physical inventory (re-hashed from disk); the controller uses this for integrity scans. The **controller** returns the chunks its metadata assigns to that worker. The other internal endpoints follow the obvious direction: register and heartbeat on the controller, chunk PUT/GET/DELETE on the worker. |
| B14 | "detect when the required replication factor is no longer maintained" | A chunk is healthy when it has exactly RF copies **on UP workers**. The cluster and per-object replication status is `HEALTHY`, `DEGRADED` (some chunk has fewer than RF live copies), `UNAVAILABLE` (some chunk has none) or `OVER_REPLICATED` (transient). It is exposed in object metadata, `GET /api/v1/admin/replication`, the event log and the web UI. |
| B15 | "docker stop worker2 / docker start worker2" | Containers have fixed names (`container_name: worker1..4`), so these exact commands work. Workers have no restart policy, so a stopped worker stays stopped until it is started again. |
| B16 | "Persistent storage" for workers | Each worker has a named Docker volume (`worker1-data` … `worker4-data`) mounted at `/data/chunks`. It survives `docker stop/start`, container re-creation and `docker compose down`. Only `docker compose down -v` deletes it. |
| B17 | "Kubernetes should not be used" | Plain Docker Compose only. |

## C. Design decisions (defaults chosen by the implementation)

| # | Topic | Decision and reason |
|---|-------|---------------------|
| C1 | Data path | Client data flows **through the controller** (client ↔ controller → worker pipeline). Re-replication data flows worker → worker. This keeps clients unaware of worker locations and gives one place to verify checksums. The trade-off is that the controller is the bandwidth bottleneck, which is acceptable at this scale. |
| C2 | Upload buffering | The servlet container stores the multipart upload in a temporary file on the controller. The controller then reads it chunk by chunk, so memory use is about one chunk per upload. Maximum object size is **2 GB** (configurable). |
| C3 | Placement | For each chunk, pick the RF UP workers with the **fewest chunk copies**. Ties are broken by a rotating order, so consecutive chunks of one object land on different worker pairs. Copies of one chunk are always on distinct workers. |
| C4 | Write acknowledgement | An upload returns 201 only after every chunk copy was durably written: worker fsync plus atomic rename, and the worker verified the SHA-256 of the received bytes. The ack travels back up the HDFS-style pipeline: a chunk is acknowledged by the second worker to the first, and by the first to the controller. |
| C4a | HDFS-style replication **[confirmed]** | Writes use a **pipeline** (controller → worker A → worker B) rather than the controller writing both copies. Re-replication is a **command** to a worker holding a verified copy, which pushes it down a pipeline to the targets (as an HDFS NameNode instructs a DataNode). We deliberately did **not** make clients write to workers directly (unlike HDFS): the spec requires a plain multipart POST to the controller and clients not knowing worker locations. Also not replicated from HDFS: rack awareness, block reports, large blocks. |
| C5 | Failure detection | Workers send a heartbeat every **3 s**. A worker with no heartbeat for **10 s** is marked **DOWN** (checked every 2 s). In practice detection takes 10–12 s. |
| C6 | Returning workers | A worker that registers (every start) or heartbeats again after being DOWN enters **RECOVERING**. It is not used for reads or writes until the controller has re-hashed every chunk on it and fixed differences: missing or corrupted copies repaired, orphans removed. Only then is it **UP**. This prevents counting stale or corrupted copies as healthy. |
| C7 | Which copy a read uses | Copies are tried in creation order. The first copy written is the "primary", so reads are deterministic and the corruption demo is reproducible. If a copy is unreachable, corrupted or missing, the next one is used. |
| C8 | Repairing a bad copy | If enough healthy copies exist elsewhere, the bad copy is dropped. Otherwise it is **rewritten in place** from a verified copy. If no valid copy exists anywhere, the chunk is reported once as `CHUNK_LOST`. Reads of it fail with **503** rather than return bad data, and repeat reports are suppressed for 5 minutes to avoid a detect/fail loop. |
| C9 | Download failure after streaming started | The first chunk is fetched before the HTTP status is sent, so failures there return a JSON 503. A failure on a later chunk (all of its copies became unavailable or corrupted mid-download) can only **abort the connection**, because the 200 status line was already sent. The client sees a truncated transfer, not corrupted bytes. The CLI reports this as an error. |
| C10 | Background work | Recovery checks, repairs, re-replication, scrubs and housekeeping all run on **one maintenance thread** (tick every 3 s, woken early by events). They never race each other. Uploads and downloads run concurrently on request threads. |
| C11 | Interrupted operations | If the controller crashes mid-upload or mid-delete, the object stays `UPLOADING`/`DELETING`. At controller start, and later after 1 h, such objects are removed. Chunk files they left behind are deleted by the next integrity scan as orphans. |
| C12 | Untracked chunk copies | A valid chunk file on a worker that the metadata does not list (e.g. a write that succeeded after a timeout) is **adopted** if the chunk is short of copies, otherwise **deleted**. |
| C13 | Metadata limits | At most 64 custom keys per object; keys 1–128 characters with no control characters; values up to 4096 characters. Object names up to 1024 characters. |
| C14 | Schema management | `schema.sql` (idempotent `CREATE TABLE IF NOT EXISTS`) owns the schema. Hibernate auto-DDL is disabled. Foreign keys cascade deletes from objects to chunks to replicas. |
| C15 | Observability | All significant events (worker DOWN/UP, re-replication, trimming, corruption, repair, …) are stored in an `events` table (last 2000 kept). They are exposed via `GET /api/v1/admin/events`, the CLI and the web UI. This is what makes the before/during/after states visible in the demo. |
| C16 | Admin API | `/api/v1/admin/*` (workers, replication report, scrub, events) is an addition to the spec for demonstration and operations. It has no authentication. |
| C17 | Security | No authentication, authorization or TLS; out of scope for this project. Worker ports are not published to the host, and only the controller talks to workers. Chunk ids are validated by the worker to prevent path traversal. |
| C18 | Ports | Controller published on host port **8082** (the team's compose default; 8080 often collides with other local services), web UI on **3000**, PostgreSQL on **5433** (for inspection). |
| C19 | Controller restarts | Metadata lives in PostgreSQL, so a restarted controller keeps all objects. Workers that keep heartbeating stay UP. Workers that were restarted at the same time go through the RECOVERING check. |

## D. Out of scope / known limitations

* **Single controller**: it is a single point of failure, as the spec requires one controller. PostgreSQL is likewise a single instance.
* **No object versioning, range requests, multipart (resumable) uploads, or authentication.**
* **Chunk data is not compressed, deduplicated or encrypted.**
* **Only worker failures are handled** (as the spec asks). Controller or database failure makes the API unavailable until restarted, but no data is lost.
* If a worker is **permanently** lost, its chunks are re-replicated after the grace period, but its registry entry stays DOWN until it returns. Removing a worker from the cluster is not automated.
* Both copies of a chunk can be lost if two workers holding the same chunk fail before re-replication completes. The data is then unavailable (503) until one of them returns.

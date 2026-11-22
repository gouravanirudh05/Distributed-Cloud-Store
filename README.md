# Mini Distributed Cloud Object Store

A distributed object store that splits objects into 1 MB chunks, keeps two copies of every chunk on
different storage workers, survives worker, controller and metadata-database failures, and detects and
repairs corrupted data.

| Spec parameter      | This system                                                        |
|---------------------|--------------------------------------------------------------------|
| Controller nodes    | 1 active controller (`controller1` or `controller2`, Spring Boot) + a hot standby, elected through etcd |
| Metadata store      | PostgreSQL primary + synchronous replica (`postgres1`/`postgres2`, Patroni), reached via `pg-proxy`; periodic backups (`pg-backup`) |
| Storage workers     | 4 (`worker1` … `worker4`), each with its own persistent Docker volume |
| Clients             | 2: CLI container (`client`) and browser UI (`web`)                  |
| Chunk size          | 1 MB (1,048,576 bytes); the last chunk may be smaller               |
| Replication factor  | 2, copies always on different workers                              |
| Deployment          | Docker Compose (no Kubernetes)                                     |

Design decisions and assumptions are listed in [assumptions.md](assumptions.md).

---

## 1. Architecture

```
            ┌───────────────┐       ┌──────────────────┐     ┌──────────────────┐   ┌──────────┐   ┌───────────────────┐
  browser ─▶│ web (nginx)   │──────▶│ gateway (HAProxy)│────▶│ controller1      │──▶│ pg-proxy │──▶│ postgres1 PRIMARY │
            │ :3000         │ /api  │ host port 8082   │     │  ACTIVE          │   │ (HAProxy)│   │      │ sync       │
            └───────────────┘       │ routes to the    │     ├──────────────────┤   └──────────┘   │      ▼ replication│
            ┌───────────────┐       │ ACTIVE controller│ ┄┄▶ │ controller2      │        ▲         │ postgres2 REPLICA │
  you ─────▶│ client (CLI)  │──────▶│ only             │     │  STANDBY         │   pg-backup      └─────────┬─────────┘
            └───────────────┘       └────────▲─────────┘     └────────┬─────────┘   (pg_dump)                │ Patroni
                                             │                        │ controller election                │ primary election
                                             │                 ┌──────▼────────────────────────────────────▼──┐
                    register + heartbeat     │                 │            etcd1    etcd2    etcd3           │
                    (every 3 s, via gateway) │                 └──────────────────────────────────────────────┘
              ┌───────────────┬──────────────┴┬───────────────┐
              │ PUT/GET/DELETE chunks (from the active controller)
         ┌────▼────┐     ┌────▼────┐     ┌────▼────┐     ┌────▼────┐
         │ worker1 │     │ worker2 │     │ worker3 │     │ worker4 │
         │ volume  │     │ volume  │     │ volume  │     │ volume  │  /data/chunks/<chunkId>
         └─────────┘     └─────────┘     └─────────┘     └─────────┘
```

* **Clients only talk to the controller.** They never learn where chunks live.
* **Controller** (`controller/`) does the following:
  * splits uploads into chunks and picks the workers for each chunk;
  * writes each chunk through an HDFS-style **pipeline** (controller → first worker → second worker, acknowledged back up the chain);
  * reassembles downloads, verifying every chunk's SHA-256;
  * tracks worker health and keeps every chunk at exactly 2 live copies;
  * runs integrity scans.
* **Workers** (`worker/`) store chunks as files on a Docker volume. They register on start and send a heartbeat (with disk stats) every 3 s. They expose the internal chunk API.
* **PostgreSQL** holds all metadata, so a controller can restart, or be replaced by the standby, without losing anything.
  It runs as two nodes under **Patroni**: a primary and a synchronous streaming replica. Patroni elects the
  primary through the same etcd cluster and promotes the replica if the primary fails. The controllers connect
  through **pg-proxy** (HAProxy), which always points at the current primary. See [Metadata store](#metadata-store).
* **pg-backup** takes a `pg_dump` of the metadata every 10 minutes into its own volume; `scripts/restore-metadata.sh` restores one.
* **Controller failover**: two controller instances run. They elect one **active** controller through a
  3-node **etcd** cluster (a lease that the active controller renews every second). The other is a hot
  **standby**: it refuses requests and does no background work. If the active controller dies, its lease
  expires and the standby takes over within about 5 s. See [Controller failover](#controller-failover).
* **gateway** (`gateway/`, HAProxy) is the one stable controller address for clients, the web UI and the
  workers. It health-checks both controllers and routes only to the active one.
* **web** (`web/`) is a static dashboard served by nginx, which proxies `/api` to the gateway.
* **client** (`client/`) is a Java CLI (`objstore`) in its own container, with `./files` mounted at `/files`.

---

## 2. Build and start

Prerequisites: Docker with Compose v2. The demo script also needs `curl`, `jq` and `sha256sum` on the host.
You do **not** need Java or Maven on the host: images are built with multi-stage Dockerfiles.

```bash
docker compose up -d --build        # etcd x3, postgres1/2 + pg-proxy + pg-backup, 2 controllers, gateway, 4 workers, client, web
docker compose ps                   # controllers, gateway and workers "healthy"
```

| What               | URL / command                                  |
|--------------------|------------------------------------------------|
| Web UI             | http://localhost:3000                          |
| REST API           | http://localhost:8082/api/v1/objects (gateway → active controller) |
| CLI                | `docker exec -it client objstore help`         |
| Logs               | `docker compose logs -f controller1 controller2 worker2` |
| Active controller  | `curl localhost:8082/api/v1/admin/controller`  |
| Stop (keep data)   | `docker compose down`                          |
| Reset all data     | `docker compose down -v`                       |

Ports can be changed with `CONTROLLER_PORT=… WEB_PORT=… docker compose up -d`.

---

## 3. Running the required demonstrations

```bash
./demo.sh            # runs all demonstrations with narration and PASS/FAIL checks (~3.5 minutes)
./demo.sh --fresh    # wipe data, rebuild, then run everything
./demo.sh --pause    # stop between sections (live presentation)
./demo.sh --only 5   # a single section, e.g. 5 = worker failure
```

| # | Demonstration (spec §10)  | What the script shows |
|---|---------------------------|-----------------------|
| 1 | Object storage            | Upload a text file with metadata, download it, compare SHA-256 / `cmp`. |
| 2 | Chunking & distribution   | 5.5 MB upload → 6 chunks (5 × 1 MB + 512 KB), ordered chunk ids, copies on all 4 workers, physical files per worker. |
| 3 | Replication               | Every chunk has 2 copies on different workers; both on-disk copies of chunk 0 hash to the recorded checksum; worker logs show the pipeline (first worker forwarded to the second). |
| 4 | Metadata                  | Get metadata; PATCH (update, add, remove a key); checksum and chunk copies unchanged (nothing re-uploaded). |
| 5 | Worker failure            | `docker stop worker2`: DOWN detected, status DEGRADED, object still downloads correctly. After 30 s, chunks are re-replicated (HEALTHY while worker2 is down). `docker start worker2`: chunks verified, worker UP, temporary copies trimmed, exactly 2 copies per chunk. |
| 6 | Data integrity            | Corrupt a chunk file with `dd`: the download still returns the original bytes and the bad copy is rewritten. A corrupted secondary copy is found by `POST /admin/scrub`. With both copies corrupted the API answers 503 instead of returning bad data. |
| 7 | Other API operations      | List, delete (204 then 404), 400 errors. |
| 8 | File formats              | Images, video, audio, ZIP/7z/tar.gz/xz, PDF, text, ELF binary, empty file, Unicode name: each uploaded, downloaded, compared byte for byte (samples are generated if missing; needs ffmpeg/zip for some). |
| 9 | Controller failover       | `docker kill` the active controller while a client polls the API: the standby takes over (~5 s gap), stored objects still download byte-identical, uploads work, no worker is marked DOWN. The killed controller restarts as the standby. |
| 10 | Metadata store failover  | `docker kill` the PostgreSQL primary while a client polls the API: the replica is promoted (~17 s gap, requests get a fast 503 meanwhile), stored objects still download byte-identical, uploads work, no worker is marked DOWN, the killed node rejoins as a replica; a metadata backup is taken. |

### Doing it by hand

```bash
# upload (multipart: file + optional metadata JSON)
curl -F file=@files/report.pdf -F 'metadata={"author":"alice"}' http://localhost:8082/api/v1/objects
ID=<objectId from the response>

curl http://localhost:8082/api/v1/objects/$ID/metadata | jq '.chunks[] | {index, size, replicas}'

# worker failure
docker stop worker2
curl http://localhost:8082/api/v1/admin/replication | jq '{status, underReplicatedChunks}'   # DEGRADED
curl -o out.pdf http://localhost:8082/api/v1/objects/$ID && cmp out.pdf files/report.pdf     # still works
sleep 35; curl http://localhost:8082/api/v1/admin/replication | jq .status                    # HEALTHY again
docker start worker2
curl http://localhost:8082/api/v1/admin/events?limit=10 | jq -r '.[].message'                 # what happened

# corruption: overwrite bytes inside a stored chunk on worker1
CHUNK=$(curl -s http://localhost:8082/api/v1/objects/$ID/metadata | jq -r '.chunks[0].chunkId')
docker exec worker1 sh -c "printf GARBAGE | dd of=/data/chunks/$CHUNK bs=1 seek=100 conv=notrunc"
curl -X POST http://localhost:8082/api/v1/admin/scrub      # or simply download the object
```

(If `worker1` does not hold that chunk, pick a worker from `.chunks[0].replicas[].workerId`.)

```bash
# controller failover
curl -s http://localhost:8082/api/v1/admin/controller | jq '{instanceId, role, term}'   # e.g. controller1 ACTIVE
docker kill controller1                       # hard crash (docker stop = clean shutdown, takeover in ~1 s)
sleep 7; curl -s http://localhost:8082/api/v1/admin/controller | jq '{instanceId, role, term}'   # controller2 ACTIVE
curl -o out.pdf http://localhost:8082/api/v1/objects/$ID && cmp out.pdf files/report.pdf          # nothing lost
docker start controller1                      # comes back as STANDBY

# metadata store failover
docker exec postgres1 patronictl -c /etc/patroni/patroni.yml list   # who is Leader / Sync Standby
docker kill postgres1                         # if postgres1 is the Leader; ~20 s later postgres2 is
curl -s http://localhost:8082/api/v1/objects  # 503 METADATA_STORE_UNAVAILABLE during the switch, then 200
docker start postgres1                        # rejoins as a replica

# metadata backups
docker exec pg-backup pg-backup.sh once       # take a backup now (also runs every 10 min)
scripts/restore-metadata.sh                   # list backups
scripts/restore-metadata.sh latest            # restore the newest (asks for confirmation)
```

---

## 4. Interacting with the system

### CLI client (`client` container)

Put files in `./files` on the host; they appear in `/files` inside the container (the working directory).

```bash
docker exec -it client objstore upload report.pdf -m author=alice -m project=demo
docker exec -it client objstore list
docker exec -it client objstore meta <objectId>            # system + custom metadata + chunk placement table
docker exec -it client objstore download <objectId>        # saved into ./files
docker exec -it client objstore verify <objectId> report.pdf
docker exec -it client objstore patch <objectId> author=bob reviewed=yes -d project
docker exec -it client objstore delete <objectId>
docker exec -it client objstore workers | replication | scrub | events
```

Add `--json` to any command for the raw API response. The CLI can also run on the host (needs Java 21):
`mvn -q -pl client -am package && java -jar client/target/client-0.1.0.jar --url http://localhost:8082 list`.

### Web UI (http://localhost:3000)

* worker cards with live status and copy counts;
* upload with custom metadata;
* object list;
* per-object panel with system metadata, a custom-metadata editor (PATCH), and a chunk × worker placement map;
* cluster event feed;
* "Run integrity scan" button.

Deep link to an object: `http://localhost:3000/#object=<objectId>`.

---

## 5. REST API

Base URL: `http://localhost:8082/api/v1`. JSON everywhere except object data. Timestamps are ISO-8601 UTC.

### Public object API (spec §5)

| Method | Endpoint                         | Success | Errors |
|--------|----------------------------------|---------|--------|
| POST   | `/objects`                       | 201 + `Location` + metadata JSON | 400 invalid request (no `file`, bad `metadata`, not multipart, too large); 503 no worker could store the data |
| GET    | `/objects/{id}`                  | 200 binary, original `Content-Type`, `Content-Disposition`, `X-Checksum-SHA256` | 404; 409 object mid-upload/mid-delete; 503 no live/valid copy of a chunk |
| GET    | `/objects/{id}/metadata`         | 200 metadata JSON | 404 |
| PATCH  | `/objects/{id}/metadata`         | 200 metadata JSON | 400 body not a JSON object of string/null values; 404; 409 busy or concurrent modification |
| DELETE | `/objects/{id}`                  | 204 | 404; 409 busy |
| GET    | `/objects?page=0&size=50`        | 200 page JSON | 400 bad page/size |

**Upload**: `multipart/form-data` with field `file` (required) and `metadata` (optional JSON object of
string → string, e.g. `{"author":"alice"}`). The content type is taken from the file part, or guessed from the
file extension when it is missing or `application/octet-stream`.

**PATCH** body: a merge patch. Listed keys are added or updated, `null` removes a key, and other keys are
unchanged, e.g. `{"author":"bob","draft":null}`. Object data is never touched.

**Metadata response** (abridged):

```json
{
  "objectId": "f471ddb7-5929-4c26-b8a7-66607c2d30e0",
  "name": "large-5.5MB.bin",
  "size": 5767168,
  "contentType": "application/octet-stream",
  "checksumAlgorithm": "SHA-256",
  "checksum": "f1a0474…",
  "createdAt": "2026-11-10T18:28:18.1Z",
  "lastModified": "2026-11-10T18:28:18.1Z",
  "status": "AVAILABLE",
  "chunkSize": 1048576,
  "chunkCount": 6,
  "replicationFactor": 2,
  "customMetadata": { "kind": "random-binary" },
  "replication": { "status": "HEALTHY", "healthyChunks": 6, "underReplicatedChunks": 0, "overReplicatedChunks": 0, "unavailableChunks": 0 },
  "chunks": [
    { "chunkId": "f471ddb7-…_000000", "index": 0, "size": 1048576, "checksum": "…", "healthyReplicas": 2,
      "replicas": [ { "workerId": "worker3", "workerStatus": "UP", "storedAt": "…" },
                    { "workerId": "worker4", "workerStatus": "UP", "storedAt": "…" } ] }
  ]
}
```

**List response**: `{"items":[{"objectId","name","size","contentType","chunkCount","createdAt","lastModified"}], "page", "size", "totalItems", "totalPages"}`.

**Error response** (every error):

```json
{ "timestamp": "…", "status": 404, "error": "Not Found", "code": "OBJECT_NOT_FOUND",
  "message": "Object 'abc' does not exist", "path": "/api/v1/objects/abc" }
```

### Admin / observability API (extension)

| Method | Endpoint                              | Purpose |
|--------|---------------------------------------|---------|
| GET    | `/admin/workers`                      | Worker status (UP / DOWN / RECOVERING), heartbeat age, copies held, disk usage, re-replication time. |
| GET    | `/admin/workers/{id}/chunks`          | Compares the metadata's view of a worker with its actual disk (missing / corrupted / unexpected). |
| GET    | `/admin/replication`                  | Cluster replication health and every chunk not at RF, with copy locations. |
| POST   | `/admin/scrub`                        | Run an integrity scan of all UP workers now; returns a report. |
| GET    | `/admin/events?limit=50&type=…`       | Cluster event log (failures, re-replication, corruption, repairs…). |
| GET    | `/admin/controller`                   | The active controller: instance id, election term, active since, election backend. |

### Internal worker API (spec §6)

| Method | Endpoint                                  | Hosted by  | Purpose |
|--------|-------------------------------------------|------------|---------|
| POST   | `/internal/workers/register`              | controller | `{workerId, url, chunkCount, usedBytes, freeBytes, clusterId}`, sent on every worker start; replies with the cluster id; **409** if the worker belongs to a different cluster |
| POST   | `/internal/workers/heartbeat`             | controller | same body every 3 s; 404 means "register again"; 409 as above |
| PUT    | `/internal/chunks/{chunkId}`              | worker     | raw bytes plus `X-Expected-Checksum`; optional `X-Pipeline: workerId=url,...` makes the worker forward the chunk downstream; replies `{storedOn:[...]}`; 400 on checksum mismatch; 201 created / 200 replaced |
| POST   | `/internal/chunks/{chunkId}/replicate`    | worker     | command: verify the local copy (409 if corrupted, 404 if missing), then push it down the `X-Pipeline` targets |
| GET    | `/internal/chunks/{chunkId}`              | worker     | raw bytes plus `X-Chunk-Checksum` (SHA-256 of what is on disk now); 404 |
| DELETE | `/internal/chunks/{chunkId}`              | worker     | 204 (idempotent) |
| GET    | `/internal/workers/{workerId}/chunks`     | worker     | physical inventory, every chunk re-hashed (`?verify=false` to skip hashing) |
| GET    | `/internal/workers/{workerId}/chunks`     | controller | the chunks the metadata assigns to that worker |
| GET    | `/internal/workers`                       | controller | worker registry |
| GET    | `/internal/controller/leader`             | controller | 200 if this instance is the active controller, 503 if standby (the gateway's health check) |

Worker and controller ports are not published to the host; only the gateway (port 8082) and the web UI (port 3000) are.
A standby controller answers every `/api` and `/internal` request with 503 `NOT_ACTIVE_CONTROLLER`.

---

## 6. How it works

### Upload
1. The controller assigns a UUID object id and creates the object row in state `UPLOADING`.
2. It reads the upload 1 MB at a time. Each chunk gets id `<objectId>_<6-digit index>` and a SHA-256.
3. **Placement**: among UP workers it picks the two holding the fewest chunk copies. Ties rotate, so consecutive chunks spread over all workers.
4. **HDFS-style pipeline**: the chunk is sent only to the first chosen worker (`PUT /internal/chunks/{id}` with an `X-Pipeline` header naming the rest). That worker stores it, forwards it to the second, and returns the list of workers that stored it (`storedOn`) once the downstream ack arrives. Each worker re-hashes the bytes and refuses a mismatch. If the first worker fails, the next-best becomes the head; if a downstream worker fails it is skipped and the missing copy goes through a new pipeline.
5. If only one worker is available, the chunk is stored with 1 copy (a *degraded* upload) and the missing copy is added automatically later. If no worker can take a chunk, the upload is rolled back and returns 503.
6. Chunk and replica rows plus the object's size, SHA-256 and chunk count are committed in one transaction, and the state becomes `AVAILABLE`.

### Download
* Before sending anything, the controller checks that every chunk has a copy on an UP worker; otherwise it returns 503.
* It streams the chunks in order. Each chunk is read from its primary copy and verified against the recorded SHA-256.
* A corrupted or missing copy is **never returned**: it is logged, the next copy is used, and the bad copy is queued for repair.
* The first chunk is fetched before the response starts, so a failure there still produces a proper JSON error.

### Worker failure detection and replication repair
| Time (defaults)    | What happens |
|--------------------|--------------|
| every 3 s          | each worker sends a heartbeat |
| ~10–12 s after stop| no heartbeat for 10 s → worker **DOWN**; its chunks are under-replicated (status `DEGRADED`), reads use the remaining copy |
| +30 s grace        | still down → for each affected chunk the controller commands a worker holding a verified copy to push it down a pipeline to another live worker (data goes worker → worker, not through the controller) → back to 2 live copies (`HEALTHY`) |
| worker restarts    | it re-registers → **RECOVERING**: the controller re-hashes every chunk on it, then repairs, drops or removes orphans → **UP** |
| right after        | chunks now have 3 live copies → the newest (temporary) copy is removed → exactly 2 per chunk again |

The grace period stops a short restart from triggering a full copy. All repair work runs on one maintenance thread (every 3 s), so steps never race each other.

### Controller failover
* **Election.** Each controller instance campaigns in etcd for the key `objectstore/controller/leader`. The
  winner writes it with an atomic "create only if absent" transaction, bound to a **5 s lease**, and renews the
  lease every second. The etcd revision of that write is the controller's **term** (shown in the UI and events).
* **Standby.** The other instance watches the key every second. It refuses requests (503) and runs no
  background work: no failure detection, repair, re-replication or scrubs.
* **Failover.** If the active controller dies or loses contact with etcd, it stops renewing. etcd deletes the key
  when the lease expires, the standby creates it and becomes active, and the gateway (health check every
  500 ms) switches traffic to it. After a hard crash this takes about 5 s. A clean `docker stop` releases the
  lease at once, so the takeover takes about 1 s.
* **No two active controllers.** A controller treats itself as active only while its last renewal is less than
  3 s old (0.6 × TTL), while a standby can only win once the full 5 s lease has expired. So the old controller has
  always stopped acting before a new one starts, even if it is still running but cut off from etcd.
* **Nothing is lost.** All state is in PostgreSQL, which both instances share. On taking over, the new active
  controller removes objects the old one left mid-upload or mid-delete (those requests failed), and waits a full
  heartbeat timeout before it marks any worker DOWN, so heartbeats lost during the switch don't count.
* **etcd** runs as 3 nodes, so election keeps working while any one etcd node is down.

### Metadata store
* **High availability.** Two PostgreSQL nodes run under Patroni. Patroni holds the primary's leader key in etcd
  (20 s TTL); if the primary dies, the replica is promoted and pg-proxy (health check every second against
  Patroni's `/primary` endpoint) switches the controllers' connections to it. A returning node rejoins as a
  replica (using `pg_rewind` if needed). Measured: about 17-20 s of unavailability after a hard kill.
* **No committed metadata is lost.** Replication is synchronous while both nodes are up: a commit is only
  acknowledged once the replica has it. With only one node left, the primary continues alone.
* **Fail fast.** While no primary is reachable, API calls answer **503 `METADATA_STORE_UNAVAILABLE`** within
  3 s (database connection timeout) instead of hanging, and recover on their own.
* **No false worker failures.** Heartbeats cannot be recorded while the database is down, so after it returns
  the controller waits a full heartbeat timeout (10 s) before marking any worker DOWN, and logs a
  `METADATA_STORE_RECOVERED` event.
* **Backups.** `pg-backup` runs `pg_dump` (custom format) against the current primary every
  `BACKUP_INTERVAL_SECONDS` (600) and keeps the newest `BACKUP_KEEP` (24) in the `pg-backups` volume.
  `scripts/restore-metadata.sh latest` stops the controllers, restores, and starts them again. The restored
  metadata is as old as the backup: objects uploaded after it are gone (their chunks are removed as orphans),
  objects deleted after it reappear but cannot be read (503, `CHUNK_LOST`).
* **Cluster identity guard.** The metadata has a cluster id (`cluster_info`), created once. Each worker stores
  the id of the cluster it joined in `/data/chunks/.cluster-id` and sends it with every heartbeat. A controller
  whose metadata has a different id (an empty, re-created or wrong database) **refuses** the worker (409,
  `WORKER_REJECTED` event) instead of treating all its chunks as orphans and deleting them. This is the same
  idea as the cluster ID in an HDFS DataNode's `VERSION` file.

### Data integrity
* SHA-256 is recorded for every chunk and for the whole object at upload.
* Integrity is checked in three places:
  1. **on every read** (see Download);
  2. **integrity scan**: every 60 s, and on demand via `POST /admin/scrub`, each UP worker re-hashes its chunks and the controller compares them with the metadata;
  3. **recovery check**: before a returning worker is marked UP.
* **Repair**: a bad copy is rewritten in place from a verified copy, or simply dropped if enough healthy copies exist elsewhere.
  * Missing copies are restored the same way.
  * Files whose object was deleted (orphans) are removed.
  * Untracked valid copies are adopted if a chunk is short of copies, otherwise removed.
* If **no** valid copy exists, reads fail with 503 rather than return corrupted bytes, and the event is logged once (`CHUNK_LOST`).

---

## 7. Configuration

| Variable (compose / controller env)          | Default   | Meaning |
|----------------------------------------------|-----------|---------|
| `CONTROLLER_PORT`, `WEB_PORT`                | 8082, 3000| host ports |
| `STORE_CHUNK_SIZE`                           | 1048576   | chunk size in bytes |
| `STORE_REPLICATION_FACTOR`                   | 2         | copies per chunk |
| `STORE_HEARTBEAT_TIMEOUT_MS`                 | 10000     | no heartbeat for this long → DOWN |
| `RE_REPLICATION_GRACE_MS`                    | 30000     | wait after DOWN before re-replicating |
| `SCRUB_INTERVAL_MS`                          | 60000     | background integrity scan period |
| `STORE_MAX_OBJECT_SIZE`                      | 2GB       | max upload size |
| `CONTROLLER_ID`                              | controller1/2 | controller instance name used in the election |
| `ETCD_ENDPOINTS`                             | etcd1-3   | etcd cluster for leader election; empty = standalone controller (always active) |
| `ELECTION_LEASE_TTL_SECONDS`                 | 5         | a crashed active controller is replaced after about this long |
| `DB_URL`                                     | pg-proxy  | metadata database (through pg-proxy, i.e. the current primary) |
| `BACKUP_INTERVAL_SECONDS`, `BACKUP_KEEP`      | 600, 24   | metadata backup period and how many backups to keep |
| `WORKER_ID`, `WORKER_URL`, `CONTROLLER_URL`, `DATA_DIR`, `WORKER_HEARTBEAT_INTERVAL_MS` | per worker | worker identity, address, storage dir, controller address (the gateway), heartbeat period (3000) |

---

## 8. Development

```bash
mvn test                      # unit tests (placement, replication planner, metadata rules, chunk store, ...)
mvn -q package -DskipTests    # build jars locally (Java 21)
```

```
controller/   Spring Boot controller
  api/          REST controllers (objects, admin), DTOs, JSON error handling
  cluster/      leader election through etcd, standby request guard, controller status
  object/       upload/download/metadata/delete, chunk DAO, placement, verified chunk reader
  replication/  maintenance loop, replication planner/manager, integrity scrubber
  worker/       worker registry, heartbeats, health monitor, HTTP client to workers
  resources/schema.sql   metadata schema
worker/       Spring Boot worker (ChunkStore + internal chunk API + heartbeats)
client/       Java CLI (objstore)
web/          nginx + static dashboard
gateway/      HAProxy config: routes to the active controller
postgres/     PostgreSQL + Patroni image (primary election through etcd, streaming replication)
pg-proxy/     HAProxy config: routes database connections to the current primary
pg-backup/    periodic pg_dump backups
scripts/      restore-metadata.sh
demo.sh       scripted demonstrations
```

### Troubleshooting
* **Port already in use**: set `CONTROLLER_PORT` / `WEB_PORT`.
* **API answers 503 `METADATA_STORE_UNAVAILABLE`**: no PostgreSQL primary is reachable (failover in progress or both nodes down). Check `docker exec postgres1 patronictl -c /etc/patroni/patroni.yml list`.
* **A worker logs "Controller REFUSED this worker"** (`WORKER_REJECTED` event): its chunks belong to a different cluster than the controller's metadata, e.g. the database was wiped while the worker volumes were kept. Restore the right database (backup), or wipe the worker volumes to start over (`docker compose down -v`).
* **API answers 503 `NO_ACTIVE_CONTROLLER`**: no controller is active (failover in progress, or both stopped). Check `docker compose ps controller1 controller2 etcd1 etcd2 etcd3`.
* **`demo.sh` says a worker is not UP**: check `docker compose ps` and `docker compose logs worker2`.
* **Start from a clean slate**: `docker compose down -v && docker compose up -d --build`.
* **Build hangs at "load metadata for docker.io/…" with a gpg/credential error**: your Docker credential helper is locked. Unlock it (e.g. `docker login`), or build with an empty config: `DOCKER_CONFIG=$(mktemp -d) docker compose build`. With an empty config you may need to copy `~/.docker/cli-plugins` into that directory.

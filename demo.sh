#!/usr/bin/env bash
# =============================================================================================
#  Mini Distributed Cloud Object Store - required demonstrations (section 10 of the spec)
#
#    1. Object storage          upload + download, retrieved object == original
#    2. Chunking & distribution object > 1 MB split into 1 MB chunks spread over the workers
#    3. Replication             every chunk has 2 copies on different workers (and where)
#    4. Metadata                custom metadata on upload; read + modify without re-uploading
#    5. Worker failure          docker stop worker2 / docker start worker2, before/during/after
#    6. Data integrity          corrupt stored data on purpose; detection, safe reads, repair
#    7. Other API operations    list, delete, error responses
#
#  Usage:  ./demo.sh [--fresh] [--pause] [--only N]
#     --fresh   wipe all data (docker compose down -v) and rebuild/start the stack first
#     --pause   wait for <Enter> between sections (for a live presentation)
#     --only N  run only section N (1-7); objects it depends on are uploaded automatically
#  Needs: docker (compose v2), curl, jq, sha256sum.  Exit code = number of failed checks.
# =============================================================================================
set -uo pipefail
cd "$(dirname "$0")"

API="${API:-http://localhost:${CONTROLLER_PORT:-8082}/api/v1}"
FILES=demo-files
FRESH=0; PAUSE=0; ONLY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --fresh) FRESH=1 ;;
    --pause) PAUSE=1 ;;
    --only) ONLY="$2"; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown option $1"; exit 2 ;;
  esac
  shift
done

if [ -t 1 ]; then
  B=$'\e[1m'; DIM=$'\e[2m'; G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; C=$'\e[36m'; N=$'\e[0m'
else
  B=""; DIM=""; G=""; R=""; Y=""; C=""; N=""
fi
PASS=0; FAIL=0
SMALL_ID=""; LARGE_ID=""

section() { echo; echo "${B}${C}=== $* ===${N}"; }
say()     { echo "${Y}# $*${N}"; }
cmd()     { echo "${DIM}\$ $*${N}"; }
pass()    { PASS=$((PASS + 1)); echo "  ${G}PASS${N} $*"; }
fail()    { FAIL=$((FAIL + 1)); echo "  ${R}FAIL${N} $*"; }
check()   { local desc="$1"; shift; if "$@"; then pass "$desc"; else fail "$desc"; fi; }
pause()   { [ "$PAUSE" = 1 ] && read -r -p "${DIM}-- press Enter to continue --${N}" _ || true; }
want()    { [ -z "$ONLY" ] || [ "$ONLY" = "$1" ]; }

# wait_for <timeout-seconds> <description> <command...>: poll every second until the command succeeds
wait_for() {
  local t="$1" desc="$2"; shift 2
  local start=$SECONDS
  printf "  ${DIM}waiting for %s " "$desc"
  while ! "$@" >/dev/null 2>&1; do
    if [ $((SECONDS - start)) -ge "$t" ]; then printf "timed out after %ss${N}\n" "$t"; return 1; fi
    printf "."; sleep 1
  done
  printf " %ss${N}\n" $((SECONDS - start))
}

sha()          { sha256sum "$1" | cut -d' ' -f1; }
meta()         { curl -s "$API/objects/$1/metadata"; }
worker_status(){ curl -s "$API/admin/workers" | jq -r --arg w "$1" '.[] | select(.workerId==$w) | .status'; }
rep_status()   { curl -s "$API/admin/replication" | jq -r .status; }
is_worker()    { [ "$(worker_status "$1")" = "$2" ]; }
is_rep()       { [ "$(rep_status)" = "$1" ]; }
all_up()       { [ "$(curl -s "$API/admin/workers" | jq '[.[] | select(.status=="UP")] | length' 2>/dev/null)" = 4 ]; }
exactly_rf()   { meta "$1" | jq -e '.chunks | all(.replicas | length == 2 and all(.workerStatus == "UP"))' >/dev/null; }

placement() { # chunk table of an object
  meta "$1" | jq -r '.chunks[] | "    chunk \(.index)  \(.size) bytes  ->  \([.replicas[] | .workerId + (if .workerStatus != "UP" then "(" + .workerStatus + ")" else "" end)] | join(", "))"'
}
replication_line() {
  curl -s "$API/admin/replication" | jq -r '"    replication: \(.status)  (\(.totalChunks) chunks: \(.healthyChunks) at RF, \(.underReplicatedChunks) under, \(.overReplicatedChunks) over, \(.unavailableChunks) unavailable)  copies/worker: \([.workers[] | "\(.workerId)=\(.replicaCount)"] | join(" "))"'
}
workers_line() {
  curl -s "$API/admin/workers" | jq -r '"    workers: " + ([.[] | "\(.workerId)=\(.status)"] | join("  "))'
}
recent_events() { # recent_events <n> [type-regex]
  curl -s "$API/admin/events?limit=$1" | jq -r --arg re "${2:-.}" 'reverse[] | select(.type | test($re)) | "    \(.timestamp[11:19])  \(.type)  \(.message)"'
}
upload() { # upload <file> [metadata-json] -> prints response JSON
  if [ -n "${2:-}" ]; then curl -s -w '\n%{http_code}' -F "file=@$1" -F "metadata=$2" "$API/objects"
  else curl -s -w '\n%{http_code}' -F "file=@$1" "$API/objects"; fi
}
corrupt() { # corrupt <worker> <chunkId>: overwrite bytes in the middle of the stored chunk file
  docker exec "$1" sh -c "printf 'CORRUPTED-ON-PURPOSE' | dd of=/data/chunks/$2 bs=1 seek=4096 conv=notrunc 2>/dev/null"
}
disk_sha() { docker exec "$1" sha256sum "/data/chunks/$2" | cut -d' ' -f1; }

# ------------------------------------------------------------------------------------ preflight
for tool in docker curl jq sha256sum; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool"; exit 2; }
done

if [ "$FRESH" = 1 ]; then
  section "Fresh start: wiping data and rebuilding the stack"
  cmd "docker compose down -v && docker compose up -d --build"
  docker compose down -v
  docker compose up -d --build || exit 2
fi

section "Cluster check"
if ! curl -s -o /dev/null "$API/objects"; then
  echo "The controller is not reachable at $API. Start the stack with: docker compose up -d --build"
  exit 2
fi
wait_for 120 "all 4 workers UP" all_up || exit 2
workers_line
replication_line

mkdir -p "$FILES"
[ -f "$FILES/hello.txt" ] || printf 'Hello from the distributed object store demo.\nThis small text file fits in a single chunk.\n' > "$FILES/hello.txt"
[ -f "$FILES/large-5.5MB.bin" ] || head -c 5767168 /dev/urandom > "$FILES/large-5.5MB.bin"   # 5.5 MiB -> 6 chunks
[ -f "$FILES/throwaway.bin" ] || head -c 300000 /dev/urandom > "$FILES/throwaway.bin"
say "demo files in ./$FILES: hello.txt ($(stat -c%s $FILES/hello.txt) B), large-5.5MB.bin ($(stat -c%s $FILES/large-5.5MB.bin) B), throwaway.bin"

ensure_small() {
  [ -n "$SMALL_ID" ] && return
  local out; out=$(upload "$FILES/hello.txt" '{"author":"alice","project":"cloud-demo","course":"CC"}')
  SMALL_ID=$(echo "$out" | head -n -1 | jq -r .objectId)
}
ensure_large() {
  [ -n "$LARGE_ID" ] && return
  local out; out=$(upload "$FILES/large-5.5MB.bin" '{"kind":"random-binary"}')
  LARGE_ID=$(echo "$out" | head -n -1 | jq -r .objectId)
}

# ------------------------------------------------------------------------------------ 1
if want 1; then
  section "1. Object storage: upload an object, retrieve it, compare with the original"
  cmd "curl -F file=@$FILES/hello.txt -F 'metadata={\"author\":\"alice\",...}' $API/objects"
  out=$(upload "$FILES/hello.txt" '{"author":"alice","project":"cloud-demo","course":"CC"}')
  code=$(echo "$out" | tail -n1); body=$(echo "$out" | head -n -1)
  echo "$body" | jq '{objectId, name, size, contentType, checksum, chunkCount, customMetadata}'
  SMALL_ID=$(echo "$body" | jq -r .objectId)
  check "upload returns 201 Created with a new object id" [ "$code" = 201 -a -n "$SMALL_ID" ]

  cmd "curl -o downloaded.txt $API/objects/$SMALL_ID"
  curl -s -D "$FILES/.headers" -o "$FILES/hello.downloaded.txt" "$API/objects/$SMALL_ID"
  grep -iE '^(HTTP|Content-Type|Content-Length|X-Checksum)' "$FILES/.headers" | sed 's/^/    /'
  echo "    original   sha256 $(sha "$FILES/hello.txt")"
  echo "    downloaded sha256 $(sha "$FILES/hello.downloaded.txt")"
  check "download returns 200 with the object's content type" grep -qi '^content-type: text/plain' "$FILES/.headers"
  check "retrieved object is byte-for-byte identical to the original" cmp -s "$FILES/hello.txt" "$FILES/hello.downloaded.txt"
  pause
fi

# ------------------------------------------------------------------------------------ 2
if want 2; then
  section "2. Chunking and distribution: an object larger than 1 MB"
  cmd "curl -F file=@$FILES/large-5.5MB.bin $API/objects"
  out=$(upload "$FILES/large-5.5MB.bin" '{"kind":"random-binary"}')
  code=$(echo "$out" | tail -n1); body=$(echo "$out" | head -n -1)
  LARGE_ID=$(echo "$body" | jq -r .objectId)
  echo "$body" | jq '{objectId, name, size, chunkSize, chunkCount}'
  check "upload of 5.5 MB object returns 201" [ "$code" = 201 ]
  say "1 MB = 1048576 bytes, so 5767168 bytes -> 5 full chunks + a final 524288-byte chunk"
  placement "$LARGE_ID"
  m=$(meta "$LARGE_ID")
  check "object split into 6 chunks" [ "$(echo "$m" | jq .chunkCount)" = 6 ]
  check "chunks 0-4 are exactly 1 MB, final chunk is smaller (524288 B)" \
    [ "$(echo "$m" | jq -c '[.chunks[].size]')" = "[1048576,1048576,1048576,1048576,1048576,524288]" ]
  check "chunk ids are unique and ordered (<objectId>_000000 ... _000005)" \
    [ "$(echo "$m" | jq -c '[.chunks[].chunkId | split("_")[1]]')" = '["000000","000001","000002","000003","000004","000005"]' ]
  check "chunks are distributed across all 4 workers" \
    [ "$(echo "$m" | jq '[.chunks[].replicas[].workerId] | unique | length')" = 4 ]
  say "physical chunk files of this object on each worker's persistent volume:"
  for w in worker1 worker2 worker3 worker4; do
    n=$(docker exec "$w" sh -c "ls /data/chunks | grep -c '^$LARGE_ID'" 2>/dev/null)
    cmd "docker exec $w ls /data/chunks | grep $LARGE_ID   -> $n file(s)"
  done
  pause
fi

# ------------------------------------------------------------------------------------ 3
if want 3; then
  ensure_large
  section "3. Replication: every chunk has two copies on different workers"
  m=$(meta "$LARGE_ID")
  echo "$m" | jq -r '.chunks[] | "    chunk \(.index) (\(.chunkId)): copies on \([.replicas[].workerId] | join(" and "))"'
  check "every chunk has exactly 2 copies" [ "$(echo "$m" | jq '[.chunks[] | (.replicas | length) == 2] | all')" = true ]
  check "the 2 copies of every chunk are on different workers" \
    [ "$(echo "$m" | jq '[.chunks[] | ([.replicas[].workerId] | unique | length) == 2] | all')" = true ]
  c0=$(echo "$m" | jq -r '.chunks[0].chunkId'); s0=$(echo "$m" | jq -r '.chunks[0].checksum')
  say "proof on disk: both copies of chunk 0 hash to the checksum recorded at upload ($s0)"
  ok=true
  for w in $(echo "$m" | jq -r '.chunks[0].replicas[].workerId'); do
    d=$(disk_sha "$w" "$c0"); cmd "docker exec $w sha256sum /data/chunks/$c0   -> $d"
    [ "$d" = "$s0" ] || ok=false
  done
  check "both physical copies of chunk 0 are identical to the original chunk" $ok
  replication_line
  check "cluster replication status is HEALTHY" is_rep HEALTHY
  pause
fi

# ------------------------------------------------------------------------------------ 4
if want 4; then
  ensure_small
  section "4. Metadata: custom metadata, read and modify without re-uploading"
  cmd "curl $API/objects/$SMALL_ID/metadata"
  before=$(meta "$SMALL_ID")
  echo "$before" | jq '{objectId, name, size, contentType, checksumAlgorithm, checksum, createdAt, lastModified, chunkCount, customMetadata, chunks: [.chunks[] | {chunkId, size, storedOn: [.replicas[].workerId]}]}'
  check "metadata contains the custom metadata given at upload" \
    [ "$(echo "$before" | jq -r '.customMetadata.author')" = alice ]
  sleep 1
  say "PATCH: change author, add reviewed, remove course (null deletes a key); 'project' is untouched"
  cmd "curl -X PATCH -H 'Content-Type: application/json' -d '{\"author\":\"bob\",\"reviewed\":\"true\",\"course\":null}' $API/objects/$SMALL_ID/metadata"
  code=$(curl -s -o "$FILES/.patch.json" -w '%{http_code}' -X PATCH -H 'Content-Type: application/json' \
    -d '{"author":"bob","reviewed":"true","course":null}' "$API/objects/$SMALL_ID/metadata")
  after=$(cat "$FILES/.patch.json")
  echo "$after" | jq '{customMetadata, createdAt, lastModified, checksum}'
  check "PATCH returns 200" [ "$code" = 200 ]
  check "custom metadata merged: author=bob, reviewed=true, project kept, course removed" \
    [ "$(echo "$after" | jq -c .customMetadata)" = '{"author":"bob","project":"cloud-demo","reviewed":"true"}' ]
  check "lastModified advanced, createdAt unchanged" \
    [ "$(echo "$after" | jq -r .lastModified)" != "$(echo "$before" | jq -r .lastModified)" -a \
      "$(echo "$after" | jq -r .createdAt)" = "$(echo "$before" | jq -r .createdAt)" ]
  check "object data untouched: same checksum and same chunk copies (nothing re-uploaded)" \
    [ "$(echo "$after" | jq -c '[.checksum, [.chunks[].replicas[].storedAt]]')" = "$(echo "$before" | jq -c '[.checksum, [.chunks[].replicas[].storedAt]]')" ]
  curl -s -o "$FILES/hello.downloaded.txt" "$API/objects/$SMALL_ID"
  check "object still downloads identical to the original" cmp -s "$FILES/hello.txt" "$FILES/hello.downloaded.txt"
  cmd "curl -X PATCH ... $API/objects/does-not-exist/metadata"
  code=$(curl -s -o "$FILES/.err.json" -w '%{http_code}' -X PATCH -H 'Content-Type: application/json' -d '{"a":"b"}' "$API/objects/does-not-exist/metadata")
  echo "    $(cat "$FILES/.err.json")"
  check "PATCH of a missing object returns 404 with a JSON message" [ "$code" = 404 ]
  pause
fi

# ------------------------------------------------------------------------------------ 5
if want 5; then
  ensure_large
  section "5. Worker failure: docker stop worker2 ... docker start worker2"
  say "BEFORE the failure"
  workers_line; replication_line; placement "$LARGE_ID"
  held=$(curl -s "$API/admin/workers" | jq -r '.[] | select(.workerId=="worker2") | .replicaCount')
  say "worker2 holds $held chunk copies"
  check "before: all chunks at replication factor 2" is_rep HEALTHY

  cmd "docker stop worker2"
  docker stop worker2 >/dev/null
  wait_for 40 "controller to detect worker2 as DOWN (missed heartbeats)" is_worker worker2 DOWN
  check "controller marks worker2 DOWN" is_worker worker2 DOWN

  say "DURING the failure (before re-replication)"
  workers_line; replication_line; placement "$LARGE_ID"
  if [ "${held:-0}" -gt 0 ]; then
    check "controller detects that the replication factor is no longer maintained (DEGRADED)" is_rep DEGRADED
  fi
  recent_events 10 'WORKER_DOWN|UNDER_REPLICATED'
  cmd "curl -o large.downloaded $API/objects/$LARGE_ID"
  code=$(curl -s -o "$FILES/large.downloaded" -w '%{http_code}' "$API/objects/$LARGE_ID")
  check "object still retrievable while worker2 is down (HTTP $code), served from the remaining copies" [ "$code" = 200 ]
  check "retrieved object matches the original" cmp -s "$FILES/large-5.5MB.bin" "$FILES/large.downloaded"

  say "after the grace period (30 s) the controller copies the affected chunks to the other live workers"
  wait_for 90 "re-replication to restore 2 live copies per chunk" is_rep HEALTHY
  workers_line; replication_line; placement "$LARGE_ID"
  check "replication factor restored while worker2 is still down" is_rep HEALTHY
  recent_events 30 'RE_REPLICATED|REPLICATION_RESTORED' | tail -8

  cmd "docker start worker2"
  docker start worker2 >/dev/null
  wait_for 90 "worker2 to re-register, have its chunks verified, and become UP" is_worker worker2 UP
  wait_for 30 "surplus (temporary) copies to be removed" exactly_rf "$LARGE_ID"
  say "AFTER the worker is back"
  workers_line; replication_line; placement "$LARGE_ID"
  recent_events 30 'WORKER_REGISTERED|WORKER_UP|SURPLUS|REPLICATION_RESTORED' | tail -8
  check "worker2 is UP again (its stored chunks were verified first)" is_worker worker2 UP
  check "every chunk has exactly 2 copies again (no over-replication)" exactly_rf "$LARGE_ID"
  check "cluster replication status is HEALTHY" is_rep HEALTHY
  curl -s -o "$FILES/large.downloaded" "$API/objects/$LARGE_ID"
  check "object still matches the original after the failure cycle" cmp -s "$FILES/large-5.5MB.bin" "$FILES/large.downloaded"
  pause
fi

# ------------------------------------------------------------------------------------ 6
if want 6; then
  ensure_large
  section "6. Data integrity: intentional corruption of stored data"
  m=$(meta "$LARGE_ID")

  say "6a. corrupt the PRIMARY copy of chunk 1 (the copy a download reads first)"
  c1=$(echo "$m" | jq -r '.chunks[1].chunkId'); s1=$(echo "$m" | jq -r '.chunks[1].checksum')
  w1=$(echo "$m" | jq -r '.chunks[1].replicas[0].workerId'); o1=$(echo "$m" | jq -r '.chunks[1].replicas[1].workerId')
  cmd "docker exec $w1 sh -c \"printf 'CORRUPTED-ON-PURPOSE' | dd of=/data/chunks/$c1 bs=1 seek=4096 conv=notrunc\""
  corrupt "$w1" "$c1"
  echo "    recorded checksum   $s1"
  echo "    on-disk now ($w1) $(disk_sha "$w1" "$c1")"
  check "stored copy on $w1 is now corrupted" [ "$(disk_sha "$w1" "$c1")" != "$s1" ]
  cmd "curl -o large.downloaded $API/objects/$LARGE_ID"
  code=$(curl -s -o "$FILES/large.downloaded" -w '%{http_code}' "$API/objects/$LARGE_ID")
  check "download succeeds (HTTP $code)" [ "$code" = 200 ]
  check "downloaded object is still identical to the original: the corrupted copy was NOT returned" \
    cmp -s "$FILES/large-5.5MB.bin" "$FILES/large.downloaded"
  restored1() { [ "$(disk_sha "$w1" "$c1")" = "$s1" ]; }
  wait_for 20 "the corrupted copy on $w1 to be rewritten from the valid copy on $o1" restored1
  check "corrupted copy on $w1 restored (on-disk sha256 matches again)" restored1
  recent_events 10 'CHUNK_CORRUPTED|CHUNK_REPAIRED' | grep "$(echo "$LARGE_ID" | cut -c1-8)" | tail -3

  say "6b. corrupt the SECONDARY copy of chunk 3 (not read by downloads) and run an integrity scan"
  c3=$(echo "$m" | jq -r '.chunks[3].chunkId'); s3=$(echo "$m" | jq -r '.chunks[3].checksum')
  w3=$(echo "$m" | jq -r '.chunks[3].replicas[1].workerId')
  corrupt "$w3" "$c3"
  cmd "docker exec $w3 ... dd ... /data/chunks/$c3   (corrupted)"
  cmd "curl -X POST $API/admin/scrub"
  curl -s -X POST "$API/admin/scrub" | jq -c '{workersScanned, chunksVerified, corruptedFound, repaired, unrepairable}' | sed 's/^/    /'
  restored3() { [ "$(disk_sha "$w3" "$c3")" = "$s3" ]; }
  wait_for 20 "the copy on $w3 to be repaired" restored3
  check "integrity scan detected and repaired the corrupted copy on $w3" restored3
  recent_events 10 'CHUNK_CORRUPTED|CHUNK_REPAIRED|SCRUB' | tail -3

  say "6c. corrupt BOTH copies of a throwaway object: no valid copy exists"
  out=$(upload "$FILES/throwaway.bin"); T=$(echo "$out" | head -n -1 | jq -r .objectId)
  tm=$(meta "$T"); tc=$(echo "$tm" | jq -r '.chunks[0].chunkId')
  for w in $(echo "$tm" | jq -r '.chunks[0].replicas[].workerId'); do corrupt "$w" "$tc"; cmd "corrupted $tc on $w"; done
  cmd "curl $API/objects/$T"
  code=$(curl -s -o "$FILES/.err.json" -w '%{http_code}' "$API/objects/$T")
  echo "    HTTP $code $(cat "$FILES/.err.json")"
  check "system refuses to return corrupted data (503 JSON error instead of bad bytes)" [ "$code" = 503 ]
  sleep 2   # repair attempts run asynchronously on the controller's maintenance thread
  recent_events 10 'CHUNK_LOST' | tail -2
  code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/objects/$T")
  check "the damaged object can still be deleted (204)" [ "$code" = 204 ]
  pause
fi

# ------------------------------------------------------------------------------------ 7
if want 7; then
  section "7. Other API operations: list, delete, errors"
  cmd "curl '$API/objects?page=0&size=5'"
  curl -s "$API/objects?page=0&size=5" | jq -c '{totalItems, totalPages, items: [.items[] | {objectId, name, size, contentType}]}' | sed 's/^/    /'
  check "list returns 200 with objectId, name, size, contentType" \
    [ "$(curl -s "$API/objects?size=1" | jq '.items[0] | has("objectId") and has("name") and has("size") and has("contentType")')" = true ]
  out=$(upload "$FILES/hello.txt"); D=$(echo "$out" | head -n -1 | jq -r .objectId)
  cmd "curl -X DELETE $API/objects/$D"
  check "DELETE returns 204 No Content" [ "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/objects/$D")" = 204 ]
  check "GET after delete returns 404" [ "$(curl -s -o /dev/null -w '%{http_code}' "$API/objects/$D")" = 404 ]
  check "DELETE of a missing object returns 404" [ "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/objects/$D")" = 404 ]
  cmd "curl -F other=x $API/objects   (no 'file' field)"
  code=$(curl -s -o "$FILES/.err.json" -w '%{http_code}' -F other=x "$API/objects"); echo "    HTTP $code $(cat "$FILES/.err.json")"
  check "upload without the 'file' field returns 400 with a JSON message" [ "$code" = 400 ]
  code=$(curl -s -o "$FILES/.err.json" -w '%{http_code}' -F "file=@$FILES/hello.txt" -F 'metadata=not-json' "$API/objects"); echo "    HTTP $code $(cat "$FILES/.err.json")"
  check "invalid metadata JSON returns 400" [ "$code" = 400 ]
fi

# ------------------------------------------------------------------------------------ summary
section "Summary"
echo "  ${G}${PASS} passed${N}, ${R}${FAIL} failed${N}"
[ -n "$SMALL_ID" ] && echo "  small object: $SMALL_ID"
[ -n "$LARGE_ID" ] && echo "  large object: $LARGE_ID"
echo "  Web UI: http://localhost:${WEB_PORT:-3000}   (click an object to see its chunk map)"
rm -f "$FILES/.headers" "$FILES/.patch.json" "$FILES/.err.json"
exit "$FAIL"

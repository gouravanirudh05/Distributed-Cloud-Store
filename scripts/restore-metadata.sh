#!/usr/bin/env bash
# Restore the metadata database from a backup taken by the pg-backup service.
#
#   scripts/restore-metadata.sh            list available backups
#   scripts/restore-metadata.sh latest     restore the newest backup
#   scripts/restore-metadata.sh <file>     restore a specific backup (name as listed)
#
# The controllers are stopped during the restore so nothing writes meanwhile, then started again.
# NOTE: the restored metadata is as old as the backup. Objects uploaded after it are no longer known (their
# chunk files are removed as orphans by the next integrity scan); objects deleted after it reappear, but
# their chunks are gone, so reading them fails with 503 (CHUNK_LOST events).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -eq 0 ]; then
  docker exec pg-backup pg-backup.sh list
  exit 0
fi
FILE="$1"
if [ "$FILE" = latest ]; then
  FILE=$(docker exec pg-backup sh -c 'ls -1t /backups/objectstore-*.dump 2>/dev/null | head -n1 | xargs -r basename')
  [ -n "$FILE" ] || { echo "no backups found"; exit 1; }
fi
docker exec pg-backup test -f "/backups/$FILE" || { echo "backup /backups/$FILE not found"; exit 1; }

echo "Restoring metadata from $FILE"
read -r -p "This replaces ALL current metadata. Type 'restore' to continue: " answer
[ "$answer" = restore ] || { echo "aborted"; exit 1; }

docker stop controller1 controller2 >/dev/null
echo "controllers stopped"
docker exec pg-backup pg_restore --clean --if-exists --no-owner --single-transaction --dbname="${PGDATABASE:-objectstore}" "/backups/$FILE"
echo "metadata restored"
docker start controller1 controller2 >/dev/null
echo "controllers started; the next integrity scan reconciles the workers with the restored metadata"

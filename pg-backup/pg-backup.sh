#!/bin/sh
# Metadata backups.
#   pg-backup.sh loop    (default) back up every BACKUP_INTERVAL_SECONDS, keep the newest BACKUP_KEEP files
#   pg-backup.sh once    take one backup now
#   pg-backup.sh list    list backups
# Connection: PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE (the pg-proxy, i.e. the current primary).
set -u
DIR=/backups
INTERVAL="${BACKUP_INTERVAL_SECONDS:-600}"
KEEP="${BACKUP_KEEP:-24}"
mkdir -p "$DIR"

backup_once() {
  ts=$(date -u +%Y%m%dT%H%M%SZ)
  f="$DIR/objectstore-$ts.dump"
  if pg_dump --format=custom --no-owner --file="$f.tmp" 2>"$DIR/.last-error"; then
    mv "$f.tmp" "$f"
    objects=$(pg_restore --list "$f" | grep -c "TABLE DATA")
    echo "$(date -u +%FT%TZ) backup OK: $(basename "$f") ($(stat -c%s "$f") bytes, $objects tables)"
    # keep only the newest $KEEP backups
    ls -1t "$DIR"/objectstore-*.dump 2>/dev/null | tail -n +"$((KEEP + 1))" | xargs -r rm -f
    return 0
  fi
  rm -f "$f.tmp"
  echo "$(date -u +%FT%TZ) backup FAILED: $(cat "$DIR/.last-error")"
  return 1
}

case "${1:-loop}" in
  once) backup_once ;;
  list) ls -1t "$DIR"/objectstore-*.dump 2>/dev/null | while read -r f; do echo "$(basename "$f")  $(stat -c%s "$f") bytes"; done ;;
  loop)
    echo "Metadata backups every ${INTERVAL}s into $DIR (keeping $KEEP), source $PGHOST:${PGPORT:-5432}/$PGDATABASE"
    sleep "${BACKUP_INITIAL_DELAY_SECONDS:-20}"
    while true; do
      backup_once
      sleep "$INTERVAL"
    done ;;
  *) echo "usage: pg-backup.sh loop|once|list"; exit 2 ;;
esac

# Periodic logical backups of the metadata database (pg_dump, custom format) into the pg-backups volume.
FROM postgres:15.10-bookworm
COPY pg-backup.sh /usr/local/bin/pg-backup.sh
RUN chmod 755 /usr/local/bin/pg-backup.sh
ENTRYPOINT ["/usr/local/bin/pg-backup.sh"]
CMD ["loop"]

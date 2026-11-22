# PostgreSQL node managed by Patroni: Patroni starts PostgreSQL, elects the primary through etcd,
# makes the other node a streaming replica of it, and promotes the replica if the primary fails.
FROM postgres:15.10-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends patroni python3-etcd curl \
    && rm -rf /var/lib/apt/lists/*
COPY patroni.yml /etc/patroni/patroni.yml
COPY post-bootstrap.sh /usr/local/bin/post-bootstrap.sh
RUN chmod 755 /usr/local/bin/post-bootstrap.sh
USER postgres
EXPOSE 5432 8008
ENTRYPOINT ["patroni", "/etc/patroni/patroni.yml"]

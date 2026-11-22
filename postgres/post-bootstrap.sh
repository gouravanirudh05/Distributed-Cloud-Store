#!/bin/sh
# Called by Patroni once, after the cluster is initialised, with a superuser connection string as $1.
set -eu
psql -v ON_ERROR_STOP=1 "$1" <<SQL
CREATE ROLE "${APP_DB_USER}" LOGIN PASSWORD '${APP_DB_PASSWORD}';
CREATE DATABASE "${APP_DB_NAME}" OWNER "${APP_DB_USER}";
SQL
echo "post-bootstrap: created database ${APP_DB_NAME} owned by ${APP_DB_USER}"

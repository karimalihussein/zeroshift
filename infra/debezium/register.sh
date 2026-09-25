#!/bin/sh
# Registers one outbox connector per service database. PUT .../config creates or updates, so this
# is safe to re-run. Runs after the services are healthy: their Flyway migrations create the
# outbox table, publication and replication slot the connector attaches to.
set -eu
connect=${CONNECT_URL:-http://kafka-connect:8083}
for db in ${OUTBOX_DATABASES:-orders payments inventory shipping}; do
  sed -e "s/@DB@/$db/g" -e "s/@HOST@/${DB_HOST:-commerce-postgres}/" \
      -e "s/@USER@/${DB_USER:-zeroshift}/" -e "s/@PASSWORD@/${DB_PASSWORD:-zeroshift_local}/" \
      /debezium/outbox-connector.json > "/tmp/$db.json"
  status=$(curl -s -o /tmp/response -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data @"/tmp/$db.json" "$connect/connectors/$db-outbox/config")
  echo "$db-outbox: HTTP $status"
  case $status in 2*) ;; *) cat /tmp/response; exit 1 ;; esac
done

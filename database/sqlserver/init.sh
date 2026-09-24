#!/bin/sh
set -eu
/opt/mssql-tools18/bin/sqlcmd -S "${MSSQL_HOST:-localhost}" -U "${MSSQL_USER:-sa}" -P "$MSSQL_SA_PASSWORD" -C -i /scripts/001_schema.sql

#!/bin/sh
# Development entrypoint used by docker-compose.override.yml. Runs from the migration-lab module
# directory; ../pom.xml is the aggregator that module inherits from.
# Runs the app with DevTools and recompiles when Java or configuration sources change; DevTools
# restarts once the compile has finished. Templates, JS and CSS are served straight from src/ by
# the dev profile, so they need neither a compile nor a restart.
#
# Flyway migrations are never hot-applied. Every restart runs Flyway, so reloading on save would
# apply a half-written migration, and the next edit would then fail checksum validation. Once a
# migration changes, reloads pause until the developer restarts the app (`docker compose restart
# app`), which applies the finished file once.
set -eu

migrations=src/main/resources/db/migration

# Everything that reaches the classpath, except the assets the dev profile reads from src/ and
# the migrations, which are watched separately.
signature() {
  find src/main \
    -path src/main/resources/static -prune -o \
    -path src/main/resources/templates -prune -o \
    -path "$migrations" -prune -o \
    -type f -exec stat -c '%n %y %s' {} + | sort | md5sum
}

migration_signature() {
  find "$migrations" -type f -exec stat -c '%n %y %s' {} + | sort | md5sum
}

# target/ is a persistent volume and resource copying never deletes, so a renamed or removed
# migration would otherwise stay on the classpath.
rm -rf "target/classes/db/migration"
mvn -B spring-boot:run -Dspring-boot.run.profiles=dev &
app=$!
trap 'kill "$app" 2>/dev/null; wait "$app"; exit 143' INT TERM

poms() { stat -c %y pom.xml ../pom.xml; }
pom=$(poms)
sources=$(signature)
schema=$(migration_signature)
while kill -0 "$app" 2>/dev/null; do
  sleep 1
  if [ "$(poms)" != "$pom" ]; then
    echo "[dev] pom.xml changed: restarting Maven with the new classpath"
    kill "$app"
    wait "$app" || true
    exec sh "$0"
  fi
  if [ -n "$schema" ] && [ "$(migration_signature)" != "$schema" ]; then
    # A compile would copy the edited migration to the classpath and the restart would apply it.
    schema=
    echo "[dev] Migration change detected: hot reload paused so an unfinished migration is never"
    echo "[dev] applied. Run 'docker compose restart app' when it is ready. Never edit a migration"
    echo "[dev] that has already been applied; add a new V<n>__*.sql instead."
  fi
  [ -z "$schema" ] && continue
  current=$(signature)
  [ "$current" = "$sources" ] && continue
  sources=$current
  echo "[dev] Source change detected: compiling"
  if mvn -B -q -o compile; then
    touch target/classes/.reloadtrigger
  else
    echo "[dev] Compile failed; the running app keeps its last good build"
  fi
done
wait "$app"

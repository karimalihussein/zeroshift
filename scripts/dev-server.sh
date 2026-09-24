#!/bin/sh
# Development entrypoint used by docker-compose.override.yml.
# Runs the app with DevTools and recompiles when Java or configuration sources change; DevTools
# restarts once the compile has finished. Templates, JS and CSS are served straight from src/ by
# the dev profile, so they need neither a compile nor a restart.
set -eu

# Everything that reaches the classpath, except the assets the dev profile reads from src/.
signature() {
  find src/main \
    -path src/main/resources/static -prune -o \
    -path src/main/resources/templates -prune -o \
    -type f -exec stat -c '%n %y %s' {} + | sort | md5sum
}

mvn -B spring-boot:run -Dspring-boot.run.profiles=dev &
app=$!
trap 'kill "$app" 2>/dev/null; wait "$app"; exit 143' INT TERM

pom=$(stat -c %y pom.xml)
sources=$(signature)
while kill -0 "$app" 2>/dev/null; do
  sleep 1
  if [ "$(stat -c %y pom.xml)" != "$pom" ]; then
    echo "[dev] pom.xml changed: restarting Maven with the new classpath"
    kill "$app"
    wait "$app" || true
    exec sh "$0"
  fi
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

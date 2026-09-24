#!/usr/bin/env sh
set -eu
API="${API:-http://localhost:3000}"
curl -fsS -X POST "$API/api/data/generate" -H 'content-type: application/json' -d '{"customers":1000,"orders":5000}'
curl -fsS -X POST "$API/api/traffic/start" -H 'content-type: application/json' -d '{"rate":10}'
curl -fsS -X POST "$API/api/migration/start" -H 'content-type: application/json' -d '{"batchSize":1000,"cdcBatchSize":250,"speed":"slow"}'
echo "Live demo started. Open http://localhost:5173"

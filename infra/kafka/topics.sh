#!/bin/sh
# Declares every lab topic explicitly (broker auto-creation is off), so partition counts and
# retention are visible decisions rather than broker defaults. Safe to re-run.
set -eu
bootstrap=${BOOTSTRAP:-kafka:9092}
topics=/opt/kafka/bin/kafka-topics.sh

create() { # name partitions [config...]
  name=$1 partitions=$2
  shift 2
  configs=""
  for c in "$@"; do configs="$configs --config $c"; done
  # shellcheck disable=SC2086
  $topics --bootstrap-server "$bootstrap" --create --if-not-exists --topic "$name" \
    --partitions "$partitions" --replication-factor 1 $configs
}

# Order events are the replayable history the read model is rebuilt from: kept forever.
create order.events 3 retention.ms=-1
for service in payment inventory shipping; do
  create "$service.commands" 3 retention.ms=604800000
  create "$service.events" 3 retention.ms=604800000
done
# Dead letters are kept for inspection and redrive; one partition keeps them in arrival order.
# The carrier's scans: the ordering lab adds partitions to it and recreates it, so it is its own topic.
create shipping.carrier-scans 3 retention.ms=86400000
for topic in order.events payment.commands payment.events inventory.commands inventory.events shipping.commands shipping.events shipping.carrier-scans; do
  create "$topic.dlt" 1 retention.ms=-1
done
# Deliberately short retention, to watch old segments get deleted and offsets become unreachable.
# Only closed segments are deleted, hence the small segment.ms.
create lab.retention-demo 1 retention.ms=60000 segment.ms=10000
# Compacted: the latest status per order id survives forever, older ones are cleaned away, and a
# null value (tombstone) deletes a key. The events-over-time lab recreates it with these settings.
create lab.order-status 1 cleanup.policy=compact segment.ms=5000 min.cleanable.dirty.ratio=0.01 \
  min.compaction.lag.ms=0 max.compaction.lag.ms=10000 delete.retention.ms=20000
# Phase 5 failure lab. The disaster-recovery lab's payment events: kept forever, because Kafka is
# the log a restored database replays from. The lab deletes and recreates it on reset.
create lab.dr.payments 3 retention.ms=-1
# The trust-boundary lab's payment events on this unauthenticated broker (where a forged event
# gets in). Its protected twin lives on kafka-secure, created by the lab with ACLs.
create lab.trust.payments 1 retention.ms=86400000
$topics --bootstrap-server "$bootstrap" --list

# ADR 017: A separate 3-node KRaft cluster for the Kafka internals lab

**Status:** Accepted

## Decision

The Phase 2 labs (quorum, leader failure, durability, unclean election, delivery semantics) run on
their own cluster, `kafka-lab-1..3`, behind the Compose profile `kafka-lab`. The event-driven
services keep their single broker.

- Each node is broker and controller (combined mode): 3 JVMs give a 3-voter Raft quorum and 3
  replicas per partition. Heaps are 192–320 MB; each node uses about 360–430 MB.
- The control plane fails nodes through **Docker's API**: kill (SIGKILL), stop (SIGTERM, Kafka's
  controlled shutdown), pause/unpause (cgroup freeze). It reaches Docker through
  `tecnativa/docker-socket-proxy` with only container endpoints enabled, never the raw socket, and
  only touches containers labelled `zeroshift.kafka-lab.node` and `zeroshift.kafka-lab.cluster`.
- Failure detection is shortened (`broker.session.timeout.ms` 6 s, `replica.lag.time.max.ms`
  10 s) so a scenario takes seconds, not minutes. Everything else is stock Kafka 4.1.
- Client connection setup gives up after 1 s: a killed node's IP stays in the JVM's DNS cache, and
  a connect to an address nobody answers would otherwise wait 10 s.
- The delivery-semantics worker is a separate JVM started by the control plane, so its crash
  (`Runtime.halt`) really skips every commit and close.
- A two-replica scenario picks its leader and follower among the nodes that are **not** the quorum
  leader, so failing them never costs the quorum its majority: each scenario shows one thing.

## Why not move the services to 3 brokers

The whole stack already uses about 7 GB of the Docker VM's 9.7 GB on an 18 GB Mac, and Debezium is
OOM-killed when memory runs short. Three brokers for everything would add about 1.3 GB for every
use of the lab, including the migration lab. More importantly, the Phase 2 scenarios kill, freeze
and repartition brokers on purpose; doing that under the saga, CDC and projection would bury each
lesson under unrelated failures, and a scenario could leave the event-driven lab broken.

## Consequences

- Default stack: unchanged. With the profile: 3 lab nodes (~1.15 GB) plus the proxy (~13 MB).
- In combined mode, stopping two of three nodes also loses the controller quorum. The lab shows
  that on purpose (*Stop two nodes*), and the min.insync.replicas scenario instead raises the
  requirement to 3 with one node down, so it never depends on a quorum loss.
- Found while building it: a paused follower still receives the answer to a fetch it sent just
  before freezing and applies it when it thaws, which hid part of the acks=1 loss. Scenarios wait
  1 s after freezing (longer than `replica.fetch.wait.max.ms`) before writing.
- `KafkaLabIT` starts the same topology with Testcontainers, including the Docker proxy, and runs
  every scenario through the same classes.

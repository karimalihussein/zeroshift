package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Durability on real brokers: what an acknowledgement is worth under acks=1 and acks=all, what
 * min.insync.replicas refuses, what retries do without idempotence, and what an unclean leader
 * election throws away. Every failure is a real kill, stop or pause of a lab node.
 */
@Component
public class DurabilityLab {
  public record Step(Instant at, String text) {}

  /**
   * @param acknowledged sequence numbers the producer was told were written
   * @param present sequence numbers in the log after the failover
   * @param lost acknowledged but not in the log
   */
  public record AcksRun(
      String acks,
      int leader,
      int follower,
      Integer newLeader,
      List<Integer> acknowledged,
      List<Integer> unacknowledged,
      List<Integer> present,
      List<Integer> lost,
      List<Step> steps,
      String verdict) {}

  public record RetriesRun(
      boolean idempotent,
      int leader,
      int pausedFollower,
      int sent,
      int copies,
      long retries,
      List<Long> offsets,
      List<Step> steps,
      String verdict) {}

  public record UncleanRun(
      String phase,
      int leader,
      int follower,
      List<String> acknowledged,
      List<String> present,
      List<String> lost,
      ClusterObserver.PartitionView partition,
      List<String> brokerLog,
      List<Step> steps,
      String verdict) {}

  public record Probe(
      String topic,
      String acks,
      boolean written,
      Integer partition,
      Long offset,
      String error,
      long ms,
      List<Integer> isr,
      Integer minInsyncReplicas) {}

  /** Longer than replica.fetch.wait.max.ms, so a fetch in flight when a follower froze is over. */
  static final long FETCH_DRAIN_MS = 1000;

  private final LabKafka kafka;
  private final LabNodes nodes;
  private final LabSetup setup;
  private final ClusterObserver observer;
  private final KafkaLabRuns runs;
  private final ReentrantLock busy = new ReentrantLock();

  public DurabilityLab(
      LabKafka kafka, LabNodes nodes, LabSetup setup, ClusterObserver observer, KafkaLabRuns runs) {
    this.kafka = kafka;
    this.nodes = nodes;
    this.setup = setup;
    this.observer = observer;
    this.runs = runs;
  }

  // ---- acks=1 vs acks=all against a leader that crashes before its follower has the records ----

  /**
   * Two replicas: leader L, follower F. F is frozen, five records are sent, L is killed, F is
   * unfrozen and, still in the ISR, becomes leader without them. Under acks=1 the five were
   * acknowledged by L alone and are gone; under acks=all they were never acknowledged, the producer
   * retries against F, and nothing acknowledged is lost. L stays down: starting it again (Recover)
   * shows it truncating the records nobody else has.
   */
  public KafkaLabRuns.Run acks(String acks) {
    boolean all =
        switch (acks) {
          case "1" -> false;
          case "all" -> true;
          default ->
              throw new ApiException(
                  HttpStatus.BAD_REQUEST, "UNKNOWN_ACKS", "acks is 1 or all, not " + acks);
        };
    return exclusively(
        () -> {
          var steps = new ArrayList<Step>();
          var s = setup.requireHealthy();
          var pair = setup.pickPair(s);
          int leader = pair.get(0), follower = pair.get(1);
          kafka.recreate(LabTopics.pinned(LabTopics.ACKS, 1, leader, follower));
          kafka
              .awaitPartition(
                  LabTopics.ACKS,
                  0,
                  p -> p.leader() != null && p.leader().id() == leader && p.isr().size() == 2,
                  Duration.ofSeconds(20))
              .orElseThrow(
                  () ->
                      new KafkaLabErrors.ScenarioFailed(
                          "lab.acks never had leader " + leader + " with both replicas in sync"));
          step(
              steps,
              "lab.acks: 1 partition, replicas ["
                  + leader
                  + ", "
                  + follower
                  + "], leader "
                  + leader
                  + ", ISR ["
                  + leader
                  + ", "
                  + follower
                  + "]");

          var acknowledged = new TreeMap<Integer, Boolean>();
          try (var producer =
              kafka.producer(
                  Map.of(
                      ProducerConfig.ACKS_CONFIG,
                      all ? "all" : "1",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      all,
                      ProducerConfig.LINGER_MS_CONFIG,
                      0,
                      ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                      5000,
                      ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                      45000,
                      ProducerConfig.CLIENT_ID_CONFIG,
                      "lab-acks-" + acks))) {
            for (int seq = 1; seq <= 5; seq++)
              producer
                  .send(new ProducerRecord<>(LabTopics.ACKS, "seq", String.valueOf(seq)))
                  .get(10, TimeUnit.SECONDS);
            step(steps, "Records 1–5 written and replicated to both (baseline)");

            nodes.act(follower, LabNodes.Action.PAUSE);
            step(
                steps,
                "Froze follower "
                    + follower
                    + ": it stops fetching, but is still in the ISR for up to replica.lag.time.max.ms (10 s)");
            // A fetch it sent just before freezing is still answered into its socket buffer and
            // applied when it thaws: let that one come back empty (replica.fetch.wait.max.ms 500).
            LabKafka.sleep(FETCH_DRAIN_MS);
            var pending = new TreeMap<Integer, Future<RecordMetadata>>();
            for (int seq = 6; seq <= 10; seq++)
              pending.put(
                  seq,
                  producer.send(new ProducerRecord<>(LabTopics.ACKS, "seq", String.valueOf(seq))));
            collect(pending, acknowledged, Duration.ofSeconds(2));
            step(
                steps,
                "Sent records 6–10 with acks="
                    + acks
                    + ": "
                    + (acknowledged.isEmpty()
                        ? "none acknowledged yet (the leader waits for the frozen follower)"
                        : "acknowledged "
                            + acknowledged.keySet()
                            + " by leader "
                            + leader
                            + " alone"));

            nodes.act(leader, LabNodes.Action.KILL);
            step(
                steps,
                "Killed leader "
                    + leader
                    + " (SIGKILL): it had records 6–10, follower "
                    + follower
                    + " never fetched them");
            nodes.act(follower, LabNodes.Action.UNPAUSE);
            step(steps, "Unfroze follower " + follower);
            var elected =
                kafka.awaitPartition(
                    LabTopics.ACKS,
                    0,
                    p -> p.leader() != null && p.leader().id() == follower,
                    Duration.ofSeconds(30));
            step(
                steps,
                elected.isPresent()
                    ? "Controller elected follower "
                        + follower
                        + " leader: it was in the ISR, so this is a clean election"
                    : "No new leader within 30 s");
            if (all) {
              collect(pending, acknowledged, Duration.ofSeconds(40));
              step(
                  steps,
                  "The producer retried the unacknowledged records against the new leader: acknowledged "
                      + acknowledged.keySet());
            }
            var present =
                kafka.readAll(LabTopics.ACKS, IsolationLevel.READ_UNCOMMITTED).stream()
                    .map(r -> Integer.valueOf(r.value()))
                    .toList();
            var acked = acknowledged.keySet().stream().filter(seq -> seq > 5).toList();
            var lost = acked.stream().filter(seq -> !present.contains(seq)).toList();
            var unacked =
                pending.keySet().stream().filter(seq -> !acknowledged.containsKey(seq)).toList();
            step(steps, "Read the partition back from leader " + follower + ": " + present);
            var verdict =
                lost.isEmpty()
                    ? "acks="
                        + acks
                        + ": nothing acknowledged was lost"
                        + (unacked.isEmpty()
                            ? ""
                            : "; "
                                + unacked
                                + " were never acknowledged, so the client knows they failed")
                    : "acks="
                        + acks
                        + ": "
                        + lost.size()
                        + " acknowledged records lost ("
                        + lost
                        + "): the leader acknowledged them alone and died before any follower had them";
            return runs.record(
                KafkaLabRuns.ACKS,
                "acks=" + acks,
                verdict,
                new AcksRun(
                    acks,
                    leader,
                    follower,
                    elected.map(p -> p.leader().id()).orElse(null),
                    acked,
                    unacked,
                    present,
                    lost,
                    steps,
                    verdict));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          } catch (ExecutionException | TimeoutException e) {
            throw new KafkaLabErrors.ScenarioFailed(
                "Baseline write failed: " + ClusterObserver.rootMessage(e), e);
          } finally {
            unpauseQuietly(follower);
          }
        });
  }

  // ---- Producer retries: duplicates without idempotence ----

  /**
   * acks=all waits for every in-sync replica. A follower is frozen for 4 s (well under the 6 s
   * broker session), the producer's request timeout is 800 ms, so it retries while its first
   * request is still waiting in the leader's purgatory. Without idempotence every retry is appended
   * again; with it the leader recognises the retry by producer id and sequence number and writes
   * the record once.
   */
  public KafkaLabRuns.Run retries(boolean idempotent) {
    return exclusively(
        () -> {
          var steps = new ArrayList<Step>();
          setup.ensureStanding();
          var info = kafka.partition(LabTopics.RETRIES, 0);
          int leader = info.leader().id();
          var s = observer.snapshot();
          int follower =
              info.replicas().stream()
                  .map(org.apache.kafka.common.Node::id)
                  .filter(
                      id ->
                          id != leader
                              && !Objects.equals(
                                  id, s.quorum() == null ? null : s.quorum().leaderId()))
                  .findFirst()
                  .orElse(
                      info.replicas().stream()
                          .map(org.apache.kafka.common.Node::id)
                          .filter(id -> id != leader)
                          .findFirst()
                          .orElseThrow());
          var marker = "retry-" + UUID.randomUUID().toString().substring(0, 8);
          long retries;
          try (var producer =
              kafka.producer(
                  Map.of(
                      ProducerConfig.ACKS_CONFIG,
                      "all",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      idempotent,
                      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                      idempotent ? 5 : 1,
                      ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                      800,
                      ProducerConfig.RETRY_BACKOFF_MS_CONFIG,
                      200,
                      ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                      20000,
                      ProducerConfig.LINGER_MS_CONFIG,
                      0,
                      ProducerConfig.CLIENT_ID_CONFIG,
                      "lab-retries-" + (idempotent ? "idempotent" : "plain")))) {
            producer.partitionsFor(
                LabTopics.RETRIES); // metadata first, so the send below is one request
            nodes.act(follower, LabNodes.Action.PAUSE);
            step(
                steps,
                "Froze follower "
                    + follower
                    + " of lab.retries (leader "
                    + leader
                    + "): acks=all now waits for it");
            LabKafka.sleep(FETCH_DRAIN_MS);
            var future = producer.send(new ProducerRecord<>(LabTopics.RETRIES, marker, marker));
            step(
                steps,
                "Sent ONE record ("
                    + marker
                    + ") with acks=all, request.timeout.ms=800, enable.idempotence="
                    + idempotent);
            LabKafka.sleep(3000);
            nodes.act(follower, LabNodes.Action.UNPAUSE);
            step(
                steps,
                "Unfroze follower "
                    + follower
                    + " after 4 s: the leader's waiting requests complete");
            try {
              future.get(25, TimeUnit.SECONDS);
              step(steps, "The producer got its acknowledgement");
            } catch (Exception e) {
              step(steps, "The producer gave up: " + ClusterObserver.rootMessage(e));
            }
            retries =
                producer.metrics().entrySet().stream()
                    .filter(
                        e ->
                            e.getKey().name().equals("record-retry-total")
                                && e.getKey().group().equals("producer-metrics"))
                    .mapToLong(e -> ((Number) e.getValue().metricValue()).longValue())
                    .sum();
          } finally {
            unpauseQuietly(follower);
          }
          var copies =
              kafka.readAll(LabTopics.RETRIES, IsolationLevel.READ_UNCOMMITTED).stream()
                  .filter(r -> marker.equals(r.value()))
                  .toList();
          step(
              steps,
              "Read lab.retries back: "
                  + copies.size()
                  + " cop"
                  + (copies.size() == 1 ? "y" : "ies")
                  + " of "
                  + marker
                  + " after "
                  + retries
                  + " retries");
          var verdict =
              copies.size() == 1
                  ? (idempotent
                      ? "Idempotent producer: " + retries + " retries, written exactly once"
                      : "Written once: the retries did not happen this time")
                  : "Plain producer: 1 send, "
                      + retries
                      + " retries, "
                      + copies.size()
                      + " copies in the log. Each retry was appended again";
          return runs.record(
              KafkaLabRuns.RETRIES,
              idempotent ? "idempotent" : "plain",
              verdict,
              new RetriesRun(
                  idempotent,
                  leader,
                  follower,
                  1,
                  copies.size(),
                  retries,
                  copies.stream().map(LabKafka.Read::offset).toList(),
                  steps,
                  verdict));
        });
  }

  // ---- The last in-sync replica dies: offline, then a clean wait or an unclean election ----

  /**
   * Two replicas, min.insync.replicas=1. The follower is stopped, so the leader is the whole ISR
   * and keeps accepting acks=all writes. Then the leader crashes and the stale follower comes back:
   * the partition is offline, because the only replica with every acknowledged record is down.
   */
  public KafkaLabRuns.Run uncleanBreak() {
    return exclusively(
        () -> {
          var steps = new ArrayList<Step>();
          var s = setup.requireHealthy();
          var pair = setup.pickPair(s);
          int leader = pair.get(0), follower = pair.get(1);
          kafka.recreate(LabTopics.pinned(LabTopics.UNCLEAN, 1, leader, follower));
          kafka
              .awaitPartition(
                  LabTopics.UNCLEAN,
                  0,
                  p -> p.leader() != null && p.leader().id() == leader && p.isr().size() == 2,
                  Duration.ofSeconds(20))
              .orElseThrow(
                  () ->
                      new KafkaLabErrors.ScenarioFailed(
                          "lab.unclean never had both replicas in sync"));
          step(
              steps,
              "lab.unclean: replicas ["
                  + leader
                  + ", "
                  + follower
                  + "], min.insync.replicas=1, unclean.leader.election.enable=false");
          var acknowledged = new ArrayList<String>();
          try (var producer =
              kafka.producer(
                  Map.of(
                      ProducerConfig.ACKS_CONFIG,
                      "all",
                      ProducerConfig.CLIENT_ID_CONFIG,
                      "lab-unclean"))) {
            send(producer, "both-have-this", acknowledged);
            step(steps, "Wrote \"both-have-this\" with acks=all: on both replicas");
            nodes.act(follower, LabNodes.Action.STOP);
            kafka.awaitPartition(
                LabTopics.UNCLEAN, 0, p -> p.isr().size() == 1, Duration.ofSeconds(20));
            step(
                steps,
                "Stopped follower "
                    + follower
                    + " (controlled shutdown): ISR shrinks to ["
                    + leader
                    + "]");
            for (int i = 1; i <= 3; i++) send(producer, "only-leader-has-" + i, acknowledged);
            step(
                steps,
                "Wrote only-leader-has-1..3 with acks=all: acknowledged, because the ISR ["
                    + leader
                    + "] meets min.insync.replicas=1");
          }
          nodes.act(leader, LabNodes.Action.KILL);
          step(
              steps,
              "Killed leader " + leader + ": the only replica with every acknowledged record");
          nodes.act(follower, LabNodes.Action.START);
          step(
              steps,
              "Started follower " + follower + " again: alive, but missing 3 acknowledged records");
          var offline =
              kafka.awaitPartition(
                  LabTopics.UNCLEAN,
                  0,
                  p -> p.leader() == null || p.leader().isEmpty(),
                  Duration.ofSeconds(30));
          var view = partitionView(LabTopics.UNCLEAN);
          step(
              steps,
              offline.isPresent()
                  ? "Partition offline: no leader. ISR "
                      + view.isr()
                      + ", ELR "
                      + view.elr()
                      + ": Kafka will not hand leadership to a replica that may be missing acknowledged writes"
                  : "Partition still has a leader: " + view.leader());
          var verdict =
              "Offline: "
                  + acknowledged.size()
                  + " acknowledged records, and the only replica holding all of them is down. Wait for it, or elect the stale follower and lose data.";
          return runs.record(
              KafkaLabRuns.UNCLEAN,
              "break",
              verdict,
              new UncleanRun(
                  "offline",
                  leader,
                  follower,
                  acknowledged,
                  List.of(),
                  List.of(),
                  view,
                  List.of(),
                  steps,
                  verdict));
        });
  }

  /** The explicitly dangerous fix: make the out-of-sync follower leader. */
  public KafkaLabRuns.Run uncleanElect() {
    return exclusively(
        () -> {
          var last = lastUnclean();
          var steps = new ArrayList<Step>();
          var result =
              kafka.await(
                  kafka
                      .admin()
                      .electLeaders(
                          ElectionType.UNCLEAN,
                          java.util.Set.of(new TopicPartition(LabTopics.UNCLEAN, 0)))
                      .partitions());
          var error = result.values().iterator().next();
          step(
              steps,
              error.isPresent()
                  ? "Unclean election refused: " + error.get().getMessage()
                  : "Admin.electLeaders(UNCLEAN): the controller made an out-of-sync replica leader");
          kafka.awaitPartition(
              LabTopics.UNCLEAN,
              0,
              p -> p.leader() != null && !p.leader().isEmpty(),
              Duration.ofSeconds(15));
          return readBack(last, "unclean-election", steps, brokenAt());
        });
  }

  /** After the old leader is back (the safe path) or after an unclean election: what survived. */
  public KafkaLabRuns.Run uncleanCheck() {
    return exclusively(
        () -> {
          var last = lastUnclean();
          var steps = new ArrayList<Step>();
          var view = partitionView(LabTopics.UNCLEAN);
          if (view.leader() == null)
            throw new ApiException(
                HttpStatus.CONFLICT,
                "PARTITION_OFFLINE",
                "lab.unclean is still offline: start node "
                    + last.path("leader").asInt()
                    + ", or elect uncleanly");
          step(steps, "lab.unclean has leader " + view.leader() + ", ISR " + view.isr());
          return readBack(last, "check", steps, brokenAt());
        });
  }

  private KafkaLabRuns.Run readBack(
      tools.jackson.databind.JsonNode last, String phase, List<Step> steps, Instant logsSince) {
    int leader = last.path("leader").asInt(), follower = last.path("follower").asInt();
    var acknowledged = new ArrayList<String>();
    last.path("acknowledged").forEach(v -> acknowledged.add(v.asString()));
    var present =
        kafka.readAll(LabTopics.UNCLEAN, IsolationLevel.READ_UNCOMMITTED).stream()
            .map(LabKafka.Read::value)
            .toList();
    var lost = acknowledged.stream().filter(v -> !present.contains(v)).toList();
    step(steps, "Read lab.unclean back: " + present);
    var brokerLog = new ArrayList<String>();
    for (int node : List.of(leader, follower))
      try {
        nodes
            .logLines(node, logsSince, "lab.unclean-0", "Truncat")
            .forEach(line -> brokerLog.add("node " + node + ": " + line));
        nodes
            .logLines(node, logsSince, "lab.unclean-0", "nclean leader")
            .forEach(line -> brokerLog.add("node " + node + ": " + line));
      } catch (RuntimeException e) {
        brokerLog.add("node " + node + ": log unavailable (" + e.getMessage() + ")");
      }
    var view = partitionView(LabTopics.UNCLEAN);
    var verdict =
        lost.isEmpty()
            ? "Nothing lost: the replica with every acknowledged record led again"
            : lost.size()
                + " acknowledged records lost for good "
                + lost
                + ": the unclean leader never had them"
                + (view.replicas().stream().allMatch(view.isr()::contains)
                    ? ", and the old leader truncated them when it rejoined"
                    : "");
    return runs.record(
        KafkaLabRuns.UNCLEAN,
        phase,
        verdict,
        new UncleanRun(
            phase, leader, follower, acknowledged, present, lost, view, brokerLog, steps, verdict));
  }

  /** When the last break ran: broker log lines from then on belong to this scenario. */
  private Instant brokenAt() {
    return runs.recent(KafkaLabRuns.UNCLEAN, 20).stream()
        .filter(r -> r.mode().equals("break"))
        .map(KafkaLabRuns.Run::at)
        .findFirst()
        .orElse(Instant.now().minusSeconds(600));
  }

  private tools.jackson.databind.JsonNode lastUnclean() {
    return runs.recent(KafkaLabRuns.UNCLEAN, 20).stream()
        .filter(r -> r.mode().equals("break"))
        .findFirst()
        .orElseThrow(
            () ->
                new ApiException(
                    HttpStatus.CONFLICT,
                    "NOT_BROKEN_YET",
                    "Break it first: run the offline-partition scenario"))
        .result();
  }

  // ---- Single writes and the min.insync.replicas knob ----

  public Probe probe(String topic, String acks) {
    if (!topic.equals(LabTopics.DURABILITY) && !topic.equals(LabTopics.REPLICATED))
      throw new ApiException(
          HttpStatus.BAD_REQUEST, "NOT_A_PROBE_TOPIC", "Probe lab.durability or lab.replicated");
    if (!acks.equals("1") && !acks.equals("all") && !acks.equals("0"))
      throw new ApiException(
          HttpStatus.BAD_REQUEST, "UNKNOWN_ACKS", "acks is 0, 1 or all, not " + acks);
    var view = partitionView(topic);
    var minIsr =
        observer.snapshot().topics().stream()
            .filter(t -> t.name().equals(topic))
            .map(ClusterObserver.TopicView::minInsyncReplicas)
            .findFirst()
            .orElse(null);
    long started = System.nanoTime();
    try (var producer =
        kafka.producer(
            Map.of(
                ProducerConfig.ACKS_CONFIG, acks,
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    false, // one attempt, the broker's raw answer: no retries to be idempotent
                // about
                ProducerConfig.RETRIES_CONFIG, 0,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000,
                ProducerConfig.LINGER_MS_CONFIG, 0,
                ProducerConfig.CLIENT_ID_CONFIG, "lab-probe"))) {
      var md =
          producer
              .send(
                  new ProducerRecord<>(
                      topic, "probe", "probe acks=" + acks + " at " + Instant.now()))
              .get(8, TimeUnit.SECONDS);
      return new Probe(
          topic,
          acks,
          true,
          md.partition(),
          md.hasOffset() ? md.offset() : null,
          null,
          ms(started),
          view.isr(),
          minIsr);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | TimeoutException | RuntimeException e) {
      return new Probe(
          topic,
          acks,
          false,
          null,
          null,
          ClusterObserver.rootMessage(e),
          ms(started),
          view.isr(),
          minIsr);
    }
  }

  public ClusterObserver.TopicView minInsyncReplicas(String topic, int value) {
    if (!topic.startsWith(LabTopics.PREFIX))
      throw new ApiException(
          HttpStatus.BAD_REQUEST, "NOT_A_LAB_TOPIC", "Only lab.* topics can be changed");
    var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    kafka.await(
        kafka
            .admin()
            .incrementalAlterConfigs(
                Map.of(
                    resource,
                    List.of(
                        new AlterConfigOp(
                            new ConfigEntry(LabTopics.MIN_ISR, String.valueOf(value)),
                            AlterConfigOp.OpType.SET))))
            .all());
    return observer.observe().topics().stream()
        .filter(t -> t.name().equals(topic))
        .findFirst()
        .orElseThrow();
  }

  // ---- helpers ----

  private ClusterObserver.PartitionView partitionView(String topic) {
    var p = kafka.partition(topic, 0);
    java.util.function.Function<List<org.apache.kafka.common.Node>, List<Integer>> ids =
        l ->
            l == null
                ? List.of()
                : l.stream().map(org.apache.kafka.common.Node::id).sorted().toList();
    return new ClusterObserver.PartitionView(
        0,
        p.leader() == null || p.leader().isEmpty() ? null : p.leader().id(),
        ids.apply(p.replicas()),
        ids.apply(p.isr()),
        ids.apply(p.elr()),
        ids.apply(p.lastKnownElr()),
        null,
        null,
        null);
  }

  private void send(
      org.apache.kafka.clients.producer.KafkaProducer<String, String> producer,
      String value,
      List<String> acknowledged) {
    try {
      producer.send(new ProducerRecord<>(LabTopics.UNCLEAN, "k", value)).get(15, TimeUnit.SECONDS);
      acknowledged.add(value);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | TimeoutException e) {
      throw new KafkaLabErrors.ScenarioFailed(
          "Write of " + value + " failed: " + ClusterObserver.rootMessage(e), e);
    }
  }

  private static void collect(
      Map<Integer, Future<RecordMetadata>> pending,
      Map<Integer, Boolean> acknowledged,
      Duration within) {
    long deadline = System.nanoTime() + within.toNanos();
    for (var e : pending.entrySet()) {
      if (acknowledged.containsKey(e.getKey())) continue;
      long left = Math.max(1, (deadline - System.nanoTime()) / 1_000_000);
      try {
        e.getValue().get(left, TimeUnit.MILLISECONDS);
        acknowledged.put(e.getKey(), true);
      } catch (TimeoutException notYet) {
        // still waiting for its acknowledgement
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      } catch (ExecutionException failed) {
        // not acknowledged: the producer gave up on it
      }
    }
  }

  private void unpauseQuietly(int node) {
    try {
      if (nodes.node(node).state().equals("paused")) nodes.act(node, LabNodes.Action.UNPAUSE);
    } catch (RuntimeException ignored) {
      // best effort: the recover button does the same
    }
  }

  private static void step(List<Step> steps, String text) {
    steps.add(new Step(Instant.now(), text));
  }

  private static long ms(long started) {
    return (System.nanoTime() - started) / 1_000_000;
  }

  private <T> T exclusively(java.util.function.Supplier<T> scenario) {
    if (!busy.tryLock())
      throw new ApiException(
          HttpStatus.CONFLICT, "SCENARIO_RUNNING", "Another durability scenario is running");
    try {
      return scenario.get();
    } finally {
      busy.unlock();
    }
  }
}

package io.zeroshift.kafkalab;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Reads the lab cluster once a second (Docker for the nodes, the admin API for everything else) and
 * keeps the latest snapshot plus every change it saw between snapshots: a node dying, a new quorum
 * leader, a partition leader moving, an ISR shrinking. Nothing is inferred: each field is what
 * Docker or Kafka answered, and a part that did not answer says so.
 */
@Component
public class ClusterObserver implements SmartLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterObserver.class);
  private static final int CHANGES_KEPT = 80;

  public record NodeView(
      int id,
      String container,
      String state,
      String status,
      boolean registeredBroker,
      boolean quorumLeader,
      Long quorumLag,
      Long lastFetchAgoMs,
      int leads) {}

  public record Voter(
      int id, long logEndOffset, long lag, Long lastFetchAgoMs, Long lastCaughtUpAgoMs) {}

  public record Quorum(
      Integer leaderId, Long leaderEpoch, Long highWatermark, List<Voter> voters) {}

  public record PartitionView(
      int partition,
      Integer leader,
      List<Integer> replicas,
      List<Integer> isr,
      List<Integer> elr,
      List<Integer> lastKnownElr,
      Long earliest,
      Long highWatermark,
      Long lastStable) {
    public boolean offline() {
      return leader == null;
    }

    public boolean underReplicated() {
      return isr.size() < replicas.size();
    }
  }

  public record TopicView(
      String name,
      Integer minInsyncReplicas,
      Boolean uncleanLeaderElection,
      List<PartitionView> partitions) {}

  public record GroupPartition(String topic, int partition, Long committed, Long end, Long lag) {}

  public record GroupView(
      String groupId,
      String state,
      int members,
      Integer coordinator,
      List<GroupPartition> partitions) {}

  public record Change(Instant at, String kind, String text) {}

  /**
   * One snapshot.
   *
   * @param problems what did not answer this time, in words; empty when everything answered
   */
  public record Snapshot(
      boolean configured,
      boolean reachable,
      List<String> problems,
      List<NodeView> nodes,
      Quorum quorum,
      List<TopicView> topics,
      List<GroupView> groups,
      List<Change> changes,
      Instant at,
      long tookMs) {}

  private final KafkaLabSettings settings;
  private final LabNodes docker;
  private final LabKafka kafka;
  private final Deque<Change> changes = new ArrayDeque<>();
  private ScheduledExecutorService loop;
  private volatile Snapshot latest;

  // Previous observations, to turn snapshots into changes.
  private Map<Integer, String> lastNodeStates = Map.of();
  private Integer lastQuorumLeader;
  private Map<String, PartitionView> lastPartitions = Map.of();
  private boolean everSeen;

  public ClusterObserver(KafkaLabSettings settings, LabNodes docker, LabKafka kafka) {
    this.settings = settings;
    this.docker = docker;
    this.kafka = kafka;
    latest = empty(List.of());
  }

  public Snapshot snapshot() {
    return latest;
  }

  /** Observes now instead of waiting for the next tick: scenarios use it to read fresh state. */
  public synchronized Snapshot observe() {
    long started = System.nanoTime();
    if (!settings.configured()) return latest = empty(List.of());
    var problems = new ArrayList<String>();
    List<LabNodes.Node> containers;
    try {
      containers = docker.nodes();
    } catch (RuntimeException e) {
      problems.add(
          "Docker API proxy unreachable ("
              + rootMessage(e)
              + "): the kafka-lab profile is not running");
      return latest = empty(problems);
    }
    if (containers.isEmpty()) {
      problems.add("No lab nodes: start them with `docker compose --profile kafka-lab up -d`");
      return latest = empty(problems);
    }
    boolean anyRunning = containers.stream().anyMatch(LabNodes.Node::running);
    var admin = anyRunning ? kafka.admin() : null;

    // Phase 1: what the cluster knows about itself. Asked together, each bounded.
    var clusterNodes = admin == null ? null : admin.describeCluster().nodes();
    var quorumFuture = admin == null ? null : admin.describeMetadataQuorum().quorumInfo();
    var topicNames = admin == null ? null : admin.listTopics().names();
    var groupListing = admin == null ? null : admin.listGroups().all();

    Collection<Node> brokers = attempt(clusterNodes, "registered brokers", problems);
    QuorumInfo quorumInfo = attempt(quorumFuture, "controller quorum", problems);
    var names =
        attempt(topicNames, "topics", problems) == null
            ? List.<String>of()
            : topicNamesOf(topicNames);

    // Phase 2: the lab topics in detail.
    Map<String, TopicDescription> descriptions = Map.of();
    Map<TopicPartition, Long> earliest = Map.of(), latestOffsets = Map.of(), stable = Map.of();
    Map<String, Map<String, String>> configs = Map.of();
    if (admin != null && !names.isEmpty()) {
      var described = admin.describeTopics(names).allTopicNames();
      var configured =
          admin
              .describeConfigs(
                  names.stream()
                      .map(n -> new ConfigResource(ConfigResource.Type.TOPIC, n))
                      .toList())
              .all();
      var d = attempt(described, "topic partitions", problems);
      if (d != null) {
        descriptions = d;
        var partitions = partitionsOf(d);
        earliest =
            offsets(
                admin,
                partitions,
                OffsetSpec.earliest(),
                IsolationLevel.READ_UNCOMMITTED,
                problems);
        latestOffsets =
            offsets(
                admin, partitions, OffsetSpec.latest(), IsolationLevel.READ_UNCOMMITTED, problems);
        stable =
            offsets(
                admin, partitions, OffsetSpec.latest(), IsolationLevel.READ_COMMITTED, problems);
      }
      var c = attempt(configured, "topic configs", problems);
      if (c != null) {
        var byTopic = new HashMap<String, Map<String, String>>();
        c.forEach(
            (resource, config) -> {
              var values = new HashMap<String, String>();
              for (var key : List.of(LabTopics.MIN_ISR, LabTopics.UNCLEAN_ELECTION)) {
                var entry = config.get(key);
                if (entry != null) values.put(key, entry.value());
              }
              byTopic.put(resource.name(), values);
            });
        configs = byTopic;
      }
    }
    var groups = groups(admin, groupListing, problems);

    var topics = new ArrayList<TopicView>();
    var leads = new HashMap<Integer, Integer>();
    for (var name : names) {
      var description = descriptions.get(name);
      if (description == null) continue;
      var partitions = new ArrayList<PartitionView>();
      for (var p : description.partitions()) {
        var tp = new TopicPartition(name, p.partition());
        var leader = p.leader() == null || p.leader().isEmpty() ? null : p.leader().id();
        if (leader != null) leads.merge(leader, 1, Integer::sum);
        partitions.add(
            new PartitionView(
                p.partition(),
                leader,
                ids(p.replicas()),
                ids(p.isr()),
                ids(p.elr()),
                ids(p.lastKnownElr()),
                earliest.get(tp),
                latestOffsets.get(tp),
                stable.get(tp)));
      }
      var config = configs.getOrDefault(name, Map.of());
      topics.add(
          new TopicView(
              name,
              config.containsKey(LabTopics.MIN_ISR)
                  ? Integer.valueOf(config.get(LabTopics.MIN_ISR))
                  : null,
              config.containsKey(LabTopics.UNCLEAN_ELECTION)
                  ? Boolean.valueOf(config.get(LabTopics.UNCLEAN_ELECTION))
                  : null,
              partitions));
    }

    long now = System.currentTimeMillis();
    var quorum =
        quorumInfo == null
            ? null
            : new Quorum(
                quorumInfo.leaderId() < 0 ? null : quorumInfo.leaderId(),
                quorumInfo.leaderEpoch(),
                quorumInfo.highWatermark(),
                quorumInfo.voters().stream()
                    .map(
                        v ->
                            new Voter(
                                v.replicaId(),
                                v.logEndOffset(),
                                Math.max(0, quorumInfo.highWatermark() - v.logEndOffset()),
                                v.lastFetchTimestamp().isPresent()
                                    ? now - v.lastFetchTimestamp().getAsLong()
                                    : null,
                                v.lastCaughtUpTimestamp().isPresent()
                                    ? now - v.lastCaughtUpTimestamp().getAsLong()
                                    : null))
                    .toList());
    var registered = brokers == null ? List.<Integer>of() : brokers.stream().map(Node::id).toList();
    var nodes =
        containers.stream()
            .map(
                c -> {
                  var voter =
                      quorum == null
                          ? null
                          : quorum.voters().stream()
                              .filter(v -> v.id() == c.id())
                              .findFirst()
                              .orElse(null);
                  return new NodeView(
                      c.id(),
                      c.container(),
                      c.state(),
                      c.status(),
                      registered.contains(c.id()),
                      quorum != null && Objects.equals(quorum.leaderId(), c.id()),
                      voter == null ? null : voter.lag(),
                      voter == null ? null : voter.lastFetchAgoMs(),
                      leads.getOrDefault(c.id(), 0));
                })
            .toList();

    recordChanges(containers, quorum, topics, !descriptions.isEmpty());
    boolean reachable = brokers != null;
    return latest =
        new Snapshot(
            true,
            reachable,
            problems,
            nodes,
            quorum,
            topics,
            groups,
            List.copyOf(changes),
            Instant.now(),
            (System.nanoTime() - started) / 1_000_000);
  }

  /** The changes since the previous snapshot, newest first, as Kafka and Docker reported them. */
  private void recordChanges(
      List<LabNodes.Node> containers,
      Quorum quorum,
      List<TopicView> topics,
      boolean topicsAnswered) {
    var nodeStates = new LinkedHashMap<Integer, String>();
    containers.forEach(c -> nodeStates.put(c.id(), c.state()));
    var partitions = new LinkedHashMap<String, PartitionView>();
    topics.forEach(
        t -> t.partitions().forEach(p -> partitions.put(t.name() + "-" + p.partition(), p)));
    if (everSeen) {
      nodeStates.forEach(
          (id, state) -> {
            var before = lastNodeStates.get(id);
            if (before != null && !before.equals(state))
              add("node", "Node " + id + ": " + before + " → " + state);
          });
      var leader = quorum == null ? null : quorum.leaderId();
      if (quorum != null && !Objects.equals(leader, lastQuorumLeader))
        add(
            "quorum",
            leader == null
                ? "Controller quorum has no leader"
                : "Controller quorum leader: "
                    + (lastQuorumLeader == null ? "none" : "node " + lastQuorumLeader)
                    + " → node "
                    + leader
                    + " (epoch "
                    + quorum.leaderEpoch()
                    + ")");
      if (topicsAnswered)
        partitions.forEach(
            (name, now) -> {
              var before = lastPartitions.get(name);
              if (before == null) return;
              if (!Objects.equals(before.leader(), now.leader()))
                add(
                    "leader",
                    name
                        + " leader: "
                        + (before.leader() == null ? "none" : before.leader())
                        + " → "
                        + (now.leader() == null ? "none (offline)" : now.leader()));
              if (!before.isr().equals(now.isr()))
                add("isr", name + " ISR " + before.isr() + " → " + now.isr());
              if (!before.elr().equals(now.elr()))
                add("elr", name + " ELR " + before.elr() + " → " + now.elr());
            });
    }
    everSeen = true;
    lastNodeStates = nodeStates;
    if (quorum != null) lastQuorumLeader = quorum.leaderId();
    // A snapshot in which the cluster did not answer says nothing about partitions: keep the last
    // real observation, so the change is recorded when an answer comes.
    if (topicsAnswered) lastPartitions = partitions;
  }

  private void add(String kind, String text) {
    changes.addFirst(new Change(Instant.now(), kind, text));
    while (changes.size() > CHANGES_KEPT) changes.removeLast();
  }

  private List<GroupView> groups(
      Admin admin,
      org.apache.kafka.common.KafkaFuture<Collection<org.apache.kafka.clients.admin.GroupListing>>
          listing,
      List<String> problems) {
    if (admin == null) return List.of();
    var all = attempt(listing, "consumer groups", problems);
    if (all == null) return List.of();
    var ids =
        all.stream()
            .map(org.apache.kafka.clients.admin.GroupListing::groupId)
            .filter(id -> id.startsWith(LabTopics.PREFIX))
            .sorted()
            .toList();
    if (ids.isEmpty()) return List.of();
    Map<String, ConsumerGroupDescription> described =
        attempt(admin.describeConsumerGroups(ids).all(), "group members", problems);
    var views = new ArrayList<GroupView>();
    for (var id : ids) {
      var committed =
          attempt(
              admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata(),
              "group offsets",
              problems);
      var parts = new ArrayList<GroupPartition>();
      if (committed != null && !committed.isEmpty()) {
        var ends =
            offsets(
                admin,
                committed.keySet(),
                OffsetSpec.latest(),
                IsolationLevel.READ_UNCOMMITTED,
                problems);
        committed.entrySet().stream()
            .sorted(
                java.util.Comparator.comparing(
                    (Map.Entry<TopicPartition, ?> e) -> e.getKey().toString()))
            .forEach(
                e -> {
                  Long at = e.getValue() == null ? null : e.getValue().offset();
                  Long end = ends.get(e.getKey());
                  parts.add(
                      new GroupPartition(
                          e.getKey().topic(),
                          e.getKey().partition(),
                          at,
                          end,
                          at == null || end == null ? null : Math.max(0, end - at)));
                });
      }
      var d = described == null ? null : described.get(id);
      views.add(
          new GroupView(
              id,
              d == null ? "UNKNOWN" : d.groupState().toString(),
              d == null ? 0 : d.members().size(),
              d == null || d.coordinator() == null ? null : d.coordinator().id(),
              parts));
    }
    return views;
  }

  private Map<TopicPartition, Long> offsets(
      Admin admin,
      Collection<TopicPartition> partitions,
      OffsetSpec spec,
      IsolationLevel isolation,
      List<String> problems) {
    var request = new HashMap<TopicPartition, OffsetSpec>();
    partitions.forEach(tp -> request.put(tp, spec));
    var result = new HashMap<TopicPartition, Long>();
    // Per partition: an offline partition has no offsets, the others still do.
    var answer = admin.listOffsets(request, new ListOffsetsOptions(isolation));
    for (var tp : partitions) {
      try {
        result.put(tp, answer.partitionResult(tp).get(2, TimeUnit.SECONDS).offset());
      } catch (Exception e) {
        // No leader, or the leader is frozen: that partition has no answer this time.
      }
    }
    if (result.isEmpty() && !partitions.isEmpty()) problems.add("offsets: no partition answered");
    return result;
  }

  private <T> T attempt(
      org.apache.kafka.common.KafkaFuture<T> future, String what, List<String> problems) {
    if (future == null) return null;
    try {
      return future.get(LabKafka.CALL.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (Exception e) {
      problems.add(what + ": " + rootMessage(e));
      return null;
    }
  }

  private List<String> topicNamesOf(
      org.apache.kafka.common.KafkaFuture<java.util.Set<String>> names) {
    try {
      return names.get().stream().filter(n -> n.startsWith(LabTopics.PREFIX)).sorted().toList();
    } catch (Exception e) {
      return List.of();
    }
  }

  private static List<TopicPartition> partitionsOf(Map<String, TopicDescription> topics) {
    var all = new ArrayList<TopicPartition>();
    topics.forEach(
        (name, d) -> d.partitions().forEach(p -> all.add(new TopicPartition(name, p.partition()))));
    return all;
  }

  private static List<Integer> ids(List<Node> nodes) {
    return nodes == null ? List.of() : nodes.stream().map(Node::id).sorted().toList();
  }

  static String rootMessage(Throwable e) {
    var cause = e;
    while (cause.getCause() != null && cause.getCause() != cause) cause = cause.getCause();
    return cause.getClass().getSimpleName()
        + (cause.getMessage() == null ? "" : ": " + cause.getMessage());
  }

  private Snapshot empty(List<String> problems) {
    return new Snapshot(
        settings.configured(),
        false,
        problems,
        List.of(),
        null,
        List.of(),
        List.of(),
        List.copyOf(changes),
        Instant.now(),
        0);
  }

  // ---- A tick a second, on its own thread: a frozen broker must not stall the app's scheduler.

  @Override
  public void start() {
    if (!settings.configured()) return;
    loop =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              var t = new Thread(r, "kafka-lab-observer");
              t.setDaemon(true);
              return t;
            });
    loop.scheduleWithFixedDelay(
        () -> {
          try {
            observe();
          } catch (RuntimeException e) {
            LOG.debug("Kafka lab observation failed", e);
          }
        },
        0,
        1,
        TimeUnit.SECONDS);
  }

  @Override
  @PreDestroy
  public void stop() {
    if (loop != null) loop.shutdownNow();
    loop = null;
  }

  @Override
  public boolean isRunning() {
    return loop != null;
  }
}

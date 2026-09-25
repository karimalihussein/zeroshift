package io.zeroshift.kafkalab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.IntStream;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

/**
 * Every Kafka lab scenario against a real 3-node KRaft cluster, failed through the same Docker API
 * proxy and the same classes the control plane uses. Nothing is mocked: a kill is a SIGKILL, a
 * freeze is a cgroup pause, and each assertion reads Kafka back.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaLabIT {
  static final String CLUSTER = "it-" + UUID.randomUUID().toString().substring(0, 6);
  static final Network NETWORK = Network.newNetwork();
  static final String HOST = DockerClientFactory.instance().dockerHostIpAddress();
  static final int[] PORTS = {39192, 39193, 39194};
  static final List<GenericContainer<?>> NODES =
      IntStream.rangeClosed(1, 3).mapToObj(KafkaLabIT::node).toList();
  static final GenericContainer<?> DOCKER =
      new GenericContainer<>("tecnativa/docker-socket-proxy:v0.4.1")
          .withEnv(Map.of("CONTAINERS", "1", "POST", "1"))
          .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock")
          .withExposedPorts(2375)
          .waitingFor(Wait.forListeningPort());
  static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:17-alpine");

  static LabNodes nodes;
  static LabKafka kafka;
  static ClusterObserver observer;
  static LabSetup setup;
  static KafkaLabRuns runs;
  static Traffic traffic;
  static DurabilityLab durability;
  static DeliveryLab delivery;

  static GenericContainer<?> node(int id) {
    int port = PORTS[id - 1];
    return new GenericContainer<>("apache/kafka:4.1.0")
        .withNetwork(NETWORK)
        .withNetworkAliases("kafka-lab-" + id)
        .withLabels(
            Map.of(LabNodes.NODE_LABEL, String.valueOf(id), LabNodes.CLUSTER_LABEL, CLUSTER))
        // A fixed host port: a restarted node must come back on the address it advertises.
        .withCreateContainerCmdModifier(
            cmd ->
                cmd.getHostConfig()
                    .withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(9094))))
        .withEnv(
            Map.ofEntries(
                Map.entry("CLUSTER_ID", "zslab-it-kraft-00000001"),
                Map.entry("KAFKA_NODE_ID", String.valueOf(id)),
                Map.entry("KAFKA_PROCESS_ROLES", "broker,controller"),
                Map.entry(
                    "KAFKA_LISTENERS", "INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093"),
                Map.entry(
                    "KAFKA_ADVERTISED_LISTENERS",
                    "INTERNAL://kafka-lab-" + id + ":9092,EXTERNAL://" + HOST + ":" + port),
                Map.entry(
                    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"),
                Map.entry("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL"),
                Map.entry("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
                Map.entry(
                    "KAFKA_CONTROLLER_QUORUM_VOTERS",
                    "1@kafka-lab-1:9093,2@kafka-lab-2:9093,3@kafka-lab-3:9093"),
                Map.entry("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3"),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "2"),
                Map.entry("KAFKA_MIN_INSYNC_REPLICAS", "2"),
                Map.entry("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
                Map.entry("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
                Map.entry("KAFKA_BROKER_HEARTBEAT_INTERVAL_MS", "1000"),
                Map.entry("KAFKA_BROKER_SESSION_TIMEOUT_MS", "6000"),
                Map.entry("KAFKA_REPLICA_LAG_TIME_MAX_MS", "10000"),
                // Lab-sized internals, as in docker-compose.yml.
                Map.entry("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "3"),
                Map.entry("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "3"),
                Map.entry("KAFKA_LOG_INDEX_SIZE_MAX_BYTES", "1048576"),
                Map.entry("KAFKA_LOG_SEGMENT_BYTES", "16777216"),
                Map.entry("KAFKA_HEAP_OPTS", "-Xms192m -Xmx320m")))
        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
        .withStartupTimeout(Duration.ofMinutes(3));
  }

  @BeforeAll
  static void cluster() {
    Startables.deepStart(List.of(NODES.get(0), NODES.get(1), NODES.get(2), DOCKER, PG)).join();
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .locations("classpath:db/migration")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
        .migrate();
    var settings =
        new KafkaLabSettings(
            String.join(",", HOST + ":" + PORTS[0], HOST + ":" + PORTS[1], HOST + ":" + PORTS[2]),
            "http://" + DOCKER.getHost() + ":" + DOCKER.getMappedPort(2375),
            CLUSTER);
    nodes = new LabNodes(settings);
    kafka = new LabKafka(settings);
    observer = new ClusterObserver(settings, nodes, kafka);
    setup = new LabSetup(nodes, kafka, observer);
    runs =
        new KafkaLabRuns(
            new JdbcTemplate(
                new DriverManagerDataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())));
    traffic = new Traffic(kafka, setup, runs);
    durability = new DurabilityLab(kafka, nodes, setup, observer, runs);
    delivery = new DeliveryLab(kafka, setup, runs);
    await()
        .atMost(Duration.ofSeconds(60))
        .ignoreExceptions()
        .until(
            () -> {
              setup.ensureStanding();
              return true;
            });
  }

  /** Every scenario starts from a whole cluster, whatever the previous one left behind. */
  @org.junit.jupiter.api.BeforeEach
  void wholeCluster() {
    setup.recoverAll();
  }

  @AfterAll
  static void close() {
    kafka.close();
    NODES.forEach(GenericContainer::stop);
    DOCKER.stop();
    PG.stop();
    NETWORK.close();
  }

  @Test
  @Order(1)
  void theObserverSeesThreeNodesAQuorumAndReplicatedPartitions() {
    var s = observer.observe();
    assertThat(s.nodes()).extracting(ClusterObserver.NodeView::state).containsOnly("running");
    assertThat(s.nodes()).allMatch(ClusterObserver.NodeView::registeredBroker);
    assertThat(s.quorum().leaderId()).isNotNull();
    assertThat(s.quorum().voters()).hasSize(3);
    var replicated = topic(s, LabTopics.REPLICATED);
    assertThat(replicated.minInsyncReplicas()).isEqualTo(2);
    assertThat(replicated.partitions())
        .hasSize(3)
        .allSatisfy(p -> assertThat(p.isr()).containsExactly(1, 2, 3));
  }

  @Test
  @Order(2)
  void killingTheControllerQuorumLeaderElectsAnother() {
    int leader = observer.observe().quorum().leaderId();
    nodes.act(leader, LabNodes.Action.KILL);
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              var q = observer.observe().quorum();
              return q != null && q.leaderId() != null && q.leaderId() != leader;
            });
    assertThat(observer.snapshot().changes()).anyMatch(c -> c.kind().equals("quorum"));
    setup.recoverAll();
  }

  @Test
  @Order(3)
  void aLeaderCrashUnderTrafficLosesAndDuplicatesNothing() {
    traffic.start();
    await().atMost(Duration.ofSeconds(20)).until(() -> traffic.view().consumed() > 40);
    int leader = kafka.partition(LabTopics.REPLICATED, 0).leader().id();
    nodes.act(leader, LabNodes.Action.KILL);
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              var p = topic(observer.observe(), LabTopics.REPLICATED).partitions().getFirst();
              return p.leader() != null && p.leader() != leader && !p.isr().contains(leader);
            });
    assertThat(observer.snapshot().changes())
        .anyMatch(c -> c.kind().equals("leader") && c.text().startsWith(LabTopics.REPLICATED));
    setup.recoverAll();
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                topic(observer.observe(), LabTopics.REPLICATED).partitions().stream()
                    .allMatch(p -> p.isr().size() == 3));
    var counted = traffic.stop();
    assertThat(counted.acked()).isGreaterThan(40);
    assertThat(counted.lostAcknowledged()).isZero();
    assertThat(counted.duplicates()).isZero();
    assertThat(runs.recent(KafkaLabRuns.FAILOVER, 1)).hasSize(1);
  }

  @Test
  @Order(4)
  void acksOneLosesAcknowledgedRecordsWhenTheLeaderCrashesAndAcksAllDoesNot() {
    var one = durability.acks("1").result();
    assertThat(ints(one.path("acknowledged"))).containsExactly(6, 7, 8, 9, 10);
    assertThat(ints(one.path("lost"))).isNotEmpty();
    int crashed = one.path("leader").asInt();
    setup.recoverAll();
    // The returning leader throws away what the new leader does not have, and says so.
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                !nodes
                    .logLines(
                        crashed, java.time.Instant.now().minusSeconds(300), "lab.acks-0", "Truncat")
                    .isEmpty());

    var all = durability.acks("all").result();
    assertThat(ints(all.path("lost"))).isEmpty();
    assertThat(ints(all.path("present"))).contains(6, 7, 8, 9, 10);
    setup.recoverAll();
  }

  @Test
  @Order(5)
  void minInsyncReplicasRefusesAcksAllButNotAcksOne() {
    int follower =
        kafka.partition(LabTopics.DURABILITY, 0).replicas().stream()
            .map(org.apache.kafka.common.Node::id)
            .filter(id -> id != kafka.partition(LabTopics.DURABILITY, 0).leader().id())
            .filter(id -> !Objects.equals(id, observer.observe().quorum().leaderId()))
            .findFirst()
            .orElseThrow();
    nodes.act(follower, LabNodes.Action.STOP);
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> kafka.partition(LabTopics.DURABILITY, 0).isr().size() == 2);
    var accepted = durability.probe(LabTopics.DURABILITY, "all");
    assertThat(accepted.written()).as(accepted.error()).isTrue();

    durability.minInsyncReplicas(LabTopics.DURABILITY, 3);
    var refused = durability.probe(LabTopics.DURABILITY, "all");
    assertThat(refused.written()).isFalse();
    assertThat(refused.error()).contains("NotEnoughReplicas");
    assertThat(durability.probe(LabTopics.DURABILITY, "1").written()).isTrue();

    durability.minInsyncReplicas(LabTopics.DURABILITY, 2);
    assertThat(durability.probe(LabTopics.DURABILITY, "all").written()).isTrue();
    setup.recoverAll();
  }

  @Test
  @Order(6)
  void retriesWithoutIdempotenceDuplicateAndWithItDoNot() {
    assertThat(durability.retries(false).result().path("copies").asInt()).isGreaterThan(1);
    assertThat(durability.retries(true).result().path("copies").asInt()).isEqualTo(1);
  }

  @Test
  @Order(7)
  void theLastInSyncReplicaDyingTakesThePartitionOfflineAndAnUncleanElectionLosesData() {
    var broken = durability.uncleanBreak().result();
    assertThat(broken.path("partition").path("leader").isNull()).isTrue();
    assertThat(ints(broken.path("partition").path("elr")))
        .containsExactly(broken.path("leader").asInt());

    var elected = durability.uncleanElect().result();
    assertThat(elected.path("lost")).hasSize(3);
    assertThat(strings(elected.path("present"))).containsExactly("both-have-this");
    setup.recoverAll();
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> kafka.partition(LabTopics.UNCLEAN, 0).isr().size() == 2);
    // Still gone after the old leader is back: it truncated them.
    assertThat(durability.uncleanCheck().result().path("lost")).hasSize(3);
  }

  @Test
  @Order(8)
  void waitingForTheInSyncReplicaLosesNothing() {
    setup.reset();
    var broken = durability.uncleanBreak().result();
    nodes.act(broken.path("leader").asInt(), LabNodes.Action.START);
    await()
        .atMost(Duration.ofSeconds(45))
        .ignoreExceptions()
        .until(
            () ->
                kafka.partition(LabTopics.UNCLEAN, 0).leader() != null
                    && !kafka.partition(LabTopics.UNCLEAN, 0).leader().isEmpty());
    var checked = durability.uncleanCheck().result();
    assertThat(checked.path("lost")).isEmpty();
    assertThat(checked.path("present")).hasSize(4);
    setup.recoverAll();
  }

  @Test
  @Order(9)
  void atMostOnceLeavesGapsAtLeastOnceDuplicatesAndTransactionsDoNeither() {
    var amo = delivery.run(DeliveryWorker.Mode.AT_MOST_ONCE, true).result();
    assertThat(amo.path("workers").get(0).path("crashed").asBoolean()).isTrue();
    assertThat(ints(amo.path("committed").path("missing"))).contains(DeliveryLab.CRASH_AT);
    assertThat(ints(amo.path("committed").path("duplicated"))).isEmpty();

    var alo = delivery.run(DeliveryWorker.Mode.AT_LEAST_ONCE, true).result();
    assertThat(ints(alo.path("committed").path("missing"))).isEmpty();
    assertThat(ints(alo.path("committed").path("duplicated"))).contains(DeliveryLab.CRASH_AT);

    var eos = delivery.run(DeliveryWorker.Mode.EXACTLY_ONCE, true).result();
    assertThat(ints(eos.path("committed").path("missing"))).isEmpty();
    assertThat(ints(eos.path("committed").path("duplicated"))).isEmpty();
    // The aborted transaction's records are still in the log for read_uncommitted readers.
    assertThat(ints(eos.path("uncommitted").path("duplicated"))).contains(DeliveryLab.CRASH_AT);

    var clean = delivery.run(DeliveryWorker.Mode.AT_LEAST_ONCE, false).result();
    assertThat(ints(clean.path("committed").path("missing"))).isEmpty();
    assertThat(ints(clean.path("committed").path("duplicated"))).isEmpty();
  }

  private static ClusterObserver.TopicView topic(ClusterObserver.Snapshot s, String name) {
    return s.topics().stream().filter(t -> t.name().equals(name)).findFirst().orElseThrow();
  }

  private static List<Integer> ints(tools.jackson.databind.JsonNode array) {
    var list = new java.util.ArrayList<Integer>();
    array.forEach(n -> list.add(n.asInt()));
    return list;
  }

  private static List<String> strings(tools.jackson.databind.JsonNode array) {
    var list = new java.util.ArrayList<String>();
    array.forEach(n -> list.add(n.asString()));
    return list;
  }
}

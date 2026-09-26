package io.zeroshift.failures;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.kafkalab.KafkaLabRuns;
import io.zeroshift.racelab.application.ExperimentEngine;
import io.zeroshift.racelab.application.Experiments;
import io.zeroshift.racelab.application.RunStreams;
import io.zeroshift.racelab.experiments.Catalog;
import io.zeroshift.racelab.infrastructure.OtelTracing;
import io.zeroshift.racelab.infrastructure.postgres.PostgresLabDatabase;
import io.zeroshift.racelab.infrastructure.postgres.PostgresRunRepository;
import io.zeroshift.racelab.infrastructure.postgres.RaceLabSchema;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import tools.jackson.databind.json.JsonMapper;

/**
 * All four failure labs, stage by stage, through the same runner the page uses, against real
 * infrastructure: PostgreSQL with prepared transactions enabled, a PLAINTEXT Kafka without an
 * authorizer and a SASL + ACL Kafka. The coordinator is a real child JVM killed with SIGKILL. A
 * stage only counts as done when every claim it checked against what it measured holds.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FailureLabIT {
  static final String HOST = DockerClientFactory.instance().dockerHostIpAddress();
  static final int OPEN_PORT = 39195;
  static final int SECURE_PORT = 39196;
  static final Map<String, String> PASSWORDS =
      Map.of(
          "admin",
          "admin-it",
          "payments",
          "payments-it",
          "fulfilment",
          "fulfilment-it",
          "checkout",
          "checkout-it");

  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:17-alpine")
          .withDatabaseName("failure_lab")
          .withCommand("postgres", "-c", "max_prepared_transactions=16", "-c", "fsync=off");
  static final GenericContainer<?> OPEN = kafka(OPEN_PORT, false);
  static final GenericContainer<?> SECURE = kafka(SECURE_PORT, true);

  static FailureLabs labs;
  static TwoPhaseLab twoPhase;
  static final JsonMapper JSON = JsonMapper.builder().build();

  static GenericContainer<?> kafka(int port, boolean secure) {
    var env = new java.util.HashMap<String, String>();
    env.put("CLUSTER_ID", secure ? "zs-secure-it-000000000001" : "zs-open-it-00000000000001");
    env.put("KAFKA_NODE_ID", "1");
    env.put("KAFKA_PROCESS_ROLES", "broker,controller");
    env.put("KAFKA_LISTENERS", "INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093");
    env.put(
        "KAFKA_ADVERTISED_LISTENERS", "INTERNAL://localhost:9092,EXTERNAL://" + HOST + ":" + port);
    env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL");
    env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
    env.put("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
    env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    env.put("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "3");
    env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
    env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    env.put("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    env.put("KAFKA_HEAP_OPTS", "-Xms128m -Xmx256m");
    if (secure) {
      // As in docker-compose.yml, plus a listener the test JVM reaches through a fixed host port.
      var users =
          "user_admin=\"admin-it\" user_payments=\"payments-it\" user_fulfilment=\"fulfilment-it\" user_checkout=\"checkout-it\";";
      var jaas =
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-it\" ";
      env.put(
          "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
          "INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:SASL_PLAINTEXT");
      env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
      env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
      env.put("KAFKA_SASL_MECHANISM_CONTROLLER_PROTOCOL", "PLAIN");
      env.put("KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG", jaas + users);
      env.put("KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG", jaas + users);
      env.put(
          "KAFKA_LISTENER_NAME_CONTROLLER_PLAIN_SASL_JAAS_CONFIG",
          jaas + "user_admin=\"admin-it\";");
      env.put(
          "KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
      env.put("KAFKA_SUPER_USERS", "User:admin");
      env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");
    } else {
      env.put(
          "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
          "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
    }
    return new GenericContainer<>("apache/kafka:4.1.0")
        .withCreateContainerCmdModifier(
            cmd ->
                cmd.getHostConfig()
                    .withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(9094))))
        .withEnv(env)
        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1))
        .withStartupTimeout(Duration.ofMinutes(3));
  }

  @BeforeAll
  static void infrastructure() throws Exception {
    Startables.deepStart(List.of(PG, OPEN, SECURE)).join();
    Flyway.configure()
        .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
        .locations("classpath:db/migration")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
        .migrate();
    var settings =
        new FailureLabSettings(
            PG.getJdbcUrl(),
            PG.getUsername(),
            PG.getPassword(),
            HOST + ":" + SECURE_PORT,
            PASSWORDS);
    var events = new EventLabSettings(true, HOST + ":" + OPEN_PORT, null, Map.of(), null);
    var meters = new SimpleMeterRegistry();
    var db = new FailureDb(settings);
    var pool = RaceLabSchema.pool(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    RaceLabSchema.migrate(pool);
    var raceRuns = new PostgresRunRepository(pool);
    var engine =
        new ExperimentEngine(
            new Experiments(Catalog.all()),
            new PostgresLabDatabase(pool),
            raceRuns,
            new OtelTracing(),
            new RunStreams());
    twoPhase = new TwoPhaseLab(db);
    labs =
        new FailureLabs(
            List.of(
                twoPhase,
                new IsolationLab(engine, raceRuns),
                new DisasterRecoveryLab(db, events, meters),
                new TrustLab(db, events, settings, meters)),
            new KafkaLabRuns(
                new JdbcTemplate(
                    new DriverManagerDataSource(
                        PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()))),
            new OtelTracing(),
            meters);
  }

  @AfterAll
  static void stop() {
    for (var c : List.of(PG, OPEN, SECURE)) c.stop();
  }

  /** Runs all five stages; each must hold. Writes every stage's result for inspection. */
  static FailureLabs.Run allStages(String lab) throws Exception {
    FailureLabs.Run run = null;
    for (int n = 0; n < 5; n++) {
      try {
        run = labs.stage(lab, n);
      } catch (RuntimeException e) {
        var failed =
            labs.all().stream()
                .filter(i -> i.id().equals(lab))
                .findFirst()
                .orElseThrow()
                .run()
                .lastFailure();
        throw new AssertionError(
            lab
                + " stage "
                + (n + 1)
                + ": "
                + e.getMessage()
                + "\n"
                + (failed == null ? "" : JSON.writeValueAsString(failed.result())),
            e);
      }
    }
    var out = Path.of("target", "failure-lab-it", lab + ".json");
    Files.createDirectories(out.getParent());
    Files.writeString(out, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(run));
    assertThat(run.stages()).hasSize(5).allMatch(FailureLabs.StageResult::ok);
    return run;
  }

  @Test
  @Order(1)
  void twoPhaseCommitVersusSaga() throws Exception {
    var run = allStages("two-phase");
    var crash = run.stages().get(1).result();
    assertThat(crash.path("exitCode").asInt()).isEqualTo(137);
    assertThat(crash.path("preparedTransactions")).hasSize(2);
    assertThat(run.stages().get(2).result().path("preparedLocks").size()).isGreaterThanOrEqualTo(4);
    var blocked = run.stages().get(2).result().path("otherWork");
    assertThat(blocked.get(0).path("blockedBy").get(0).asInt()).isZero();
    assertThat(run.stages().get(3).result().path("otherWork").get(0).path("outcome").asString())
        .isEqualTo("DONE");
    assertThat(run.stages().get(4).result().path("twoPhaseRecovery")).hasSize(2);
  }

  @Test
  @Order(2)
  void aLoggedCommitDecisionIsFinishedNotRolledBack() throws Exception {
    var drill = twoPhase.startInDoubt("after-decision");
    assertThat(drill.get("decision")).isEqualTo("COMMIT");
    var actions = twoPhase.recoverInDoubt();
    assertThat(actions).hasSize(2).allMatch(a -> a.get("action").equals("COMMIT PREPARED"));
    var state = twoPhase.state();
    assertThat(state.path("preparedTransactions")).isEmpty();
    assertThat(state.path("rows").path("payments").toString())
        .contains((String) drill.get("order"));
    assertThat(state.path("rows").path("reservations").toString())
        .contains((String) drill.get("order"));
  }

  @Test
  @Order(3)
  void isolationStrategiesCompared() throws Exception {
    var run = allStages("isolation");
    var matrix = run.stages().get(4).result().path("matrix");
    assertThat(matrix.size()).isEqualTo(9);
  }

  @Test
  @Order(4)
  void disasterRecoveryOffsetGap() throws Exception {
    var run = allStages("disaster-recovery");
    assertThat(run.stages().get(2).result().path("comparedWithTopic").path("missing").asLong())
        .isEqualTo(DisasterRecoveryLab.SECOND_BATCH);
    assertThat(run.stages().get(3).result().path("rewoundToZero").path("skippedDuplicates").asInt())
        .isEqualTo(DisasterRecoveryLab.FIRST_BATCH + DisasterRecoveryLab.SECOND_BATCH);
    assertThat(run.stages().get(4).result().path("naiveRepair").path("skippedDuplicates").asInt()).isPositive();
  }

  @Test
  @Order(5)
  void trustBoundaries() throws Exception {
    var run = allStages("trust");
    assertThat(run.stages().get(1).result().path("attackerDecision").path("decision").asString())
        .isEqualTo("ALLOWED");
    assertThat(run.stages().get(3).result().path("attempts").get(2).path("error").asString())
        .isEqualTo("TopicAuthorizationException");
  }

  @Test
  @Order(6)
  void resetsLeaveNothingBehind() throws Exception {
    for (var lab : List.of("two-phase", "disaster-recovery", "trust")) labs.reset(lab);
    assertThat(twoPhase.state().path("preparedTransactions")).isEmpty();
    assertThat(labs.all()).allMatch(i -> i.run() == null || i.id().equals("isolation"));
  }
}

package io.zeroshift.platform.testing;

import io.zeroshift.contracts.Topics;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The real pipeline for integration tests: PostgreSQL with logical WAL, a Kafka broker and Debezium
 * Kafka Connect on one network, using the same connector template as Docker Compose. Started once
 * per JVM (singleton containers); each test module uses one service database.
 */
public final class CommerceStack {
  private static final Network NETWORK = Network.newNetwork();

  /** The image the lab runs: tests and jOOQ code generation use the same PostgreSQL. */
  public static final String POSTGRES_IMAGE = "postgres:17-alpine";

  public static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(POSTGRES_IMAGE)
          .withNetwork(NETWORK)
          .withNetworkAliases("postgres")
          .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10");

  public static final KafkaContainer KAFKA =
      new KafkaContainer("apache/kafka:4.1.0")
          .withNetwork(NETWORK)
          .withNetworkAliases("kafka")
          .withListener("kafka:19092")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  public static final GenericContainer<?> CONNECT =
      new GenericContainer<>("quay.io/debezium/connect:3.3")
          .withNetwork(NETWORK)
          .withExposedPorts(8083)
          .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
          .withEnv("GROUP_ID", "test-connect")
          .withEnv("CONFIG_STORAGE_TOPIC", "connect.configs")
          .withEnv("OFFSET_STORAGE_TOPIC", "connect.offsets")
          .withEnv("STATUS_STORAGE_TOPIC", "connect.status")
          .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
          .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
          .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
          .withEnv("KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
          .withEnv("VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
          .dependsOn(KAFKA)
          .waitingFor(
              Wait.forHttp("/connectors").forPort(8083).withStartupTimeout(Duration.ofMinutes(3)));

  private static boolean started;

  /** Starts everything once and creates every lab topic with three partitions. */
  public static synchronized void start() {
    if (started) return;
    POSTGRES.start();
    KAFKA.start();
    CONNECT.start();
    try (var admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      var topics = new ArrayList<NewTopic>();
      for (var topic :
          List.of(
              Topics.ORDER_EVENTS,
              Topics.PAYMENT_COMMANDS,
              Topics.PAYMENT_EVENTS,
              Topics.INVENTORY_COMMANDS,
              Topics.INVENTORY_EVENTS,
              Topics.SHIPPING_COMMANDS,
              Topics.SHIPPING_EVENTS)) {
        topics.add(new NewTopic(topic, 3, (short) 1));
        topics.add(new NewTopic(Topics.deadLetter(topic), 1, (short) 1));
      }
      admin.createTopics(topics).all().get();
    } catch (Exception e) {
      throw new IllegalStateException("Could not create topics", e);
    }
    started = true;
  }

  public static String createDatabase(String name) {
    try {
      POSTGRES.execInContainer(
          "psql",
          "-U",
          POSTGRES.getUsername(),
          "-d",
          POSTGRES.getDatabaseName(),
          "-c",
          "CREATE DATABASE " + name);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return POSTGRES.getJdbcUrl().replace("/" + POSTGRES.getDatabaseName(), "/" + name);
  }

  private static final Set<String> REGISTERED = new HashSet<>();

  /**
   * Registers the Compose connector template for {@code database}, exactly as register.sh does.
   * Call after the service started: its migrations create the publication and slot. Idempotent.
   */
  public static synchronized void registerOutboxConnector(String database) {
    if (!REGISTERED.add(database)) return;
    try {
      var template = Files.readString(Path.of("../infra/debezium/outbox-connector.json"));
      var config =
          template
              .replace("@DB@", database)
              .replace("@HOST@", "postgres")
              .replace("@USER@", POSTGRES.getUsername())
              .replace("@PASSWORD@", POSTGRES.getPassword());
      var response =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(
                          URI.create(connectUrl() + "/connectors/" + database + "-outbox/config"))
                      .header("Content-Type", "application/json")
                      .PUT(HttpRequest.BodyPublishers.ofString(config))
                      .build(),
                  HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2)
        throw new IllegalStateException("Connector rejected: " + response.body());
    } catch (java.io.IOException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String connectUrl() {
    return "http://" + CONNECT.getHost() + ":" + CONNECT.getMappedPort(8083);
  }

  /** Reads {@code topic} from the beginning until {@code count} matching records arrive. */
  public static List<ConsumerRecord<String, String>> read(
      String topic,
      Predicate<ConsumerRecord<String, String>> matches,
      int count,
      Duration timeout) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    var found = new ArrayList<ConsumerRecord<String, String>>();
    try (var consumer =
        new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
      consumer.subscribe(List.of(topic));
      long end = System.nanoTime() + timeout.toNanos();
      while (found.size() < count && System.nanoTime() < end)
        for (var record : consumer.poll(Duration.ofMillis(500)))
          if (matches.test(record)) found.add(record);
    }
    return found;
  }

  public static String header(ConsumerRecord<?, ?> record, String name) {
    var header = record.headers().lastHeader(name);
    return header == null
        ? null
        : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
  }

  private CommerceStack() {}
}

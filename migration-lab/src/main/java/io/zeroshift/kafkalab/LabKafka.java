package io.zeroshift.kafkalab;

import io.zeroshift.platform.web.ApiException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Clients for the lab cluster: one long-lived admin client, and producers and consumers made per
 * scenario with exactly the settings the scenario is about.
 */
@Component
public class LabKafka implements AutoCloseable {
  /** How long one admin call may take before the lab reports the cluster as not answering. */
  static final Duration CALL = Duration.ofSeconds(8);

  /**
   * A killed node's container is gone, but the JVM still caches its IP for a while, and a connect
   * to an address nobody answers waits for the setup timeout (10 s by default). The lab kills nodes
   * on purpose, so give up on a connection after 1 s and try another node.
   */
  static final Map<String, Object> FAST_FAILING_CONNECTIONS =
      Map.of(
          "socket.connection.setup.timeout.ms", 1000,
          "socket.connection.setup.timeout.max.ms", 3000);

  private final KafkaLabSettings settings;
  private volatile Admin admin;

  public LabKafka(KafkaLabSettings settings) {
    this.settings = settings;
  }

  public boolean configured() {
    return settings.configured();
  }

  public String bootstrap() {
    return settings.bootstrap();
  }

  public Admin admin() {
    requireConfigured();
    if (admin == null)
      synchronized (this) {
        if (admin == null) {
          var props = new HashMap<String, Object>(FAST_FAILING_CONNECTIONS);
          props.putAll(
              Map.of(
                  AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                  settings.bootstrap(),
                  AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                  1500,
                  AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                  (int) CALL.toMillis(),
                  AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                  1000,
                  AdminClientConfig.CLIENT_ID_CONFIG,
                  "kafka-lab-admin"));
          admin = Admin.create(props);
        }
      }
    return admin;
  }

  /** A producer with the common settings plus the ones the scenario is about. */
  public KafkaProducer<String, String> producer(Map<String, Object> settings) {
    requireConfigured();
    var props = new HashMap<String, Object>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.settings.bootstrap());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
    props.putAll(FAST_FAILING_CONNECTIONS);
    props.putAll(settings);
    return new KafkaProducer<>(props);
  }

  public KafkaConsumer<String, String> consumer(Map<String, Object> settings) {
    requireConfigured();
    var props = new HashMap<String, Object>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.settings.bootstrap());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
    props.putAll(FAST_FAILING_CONNECTIONS);
    props.putAll(settings);
    return new KafkaConsumer<>(props);
  }

  public <T> T await(KafkaFuture<T> future) {
    return await(future, CALL);
  }

  public <T> T await(KafkaFuture<T> future, Duration timeout) {
    try {
      return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof RuntimeException r
          ? r
          : new IllegalStateException(e.getCause());
    } catch (TimeoutException e) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "CLUSTER_NOT_ANSWERING",
          "The lab cluster did not answer within " + timeout.toSeconds() + " s");
    }
  }

  public TopicPartitionInfo partition(String topic, int partition) {
    return await(admin().describeTopics(List.of(topic)).allTopicNames())
        .get(topic)
        .partitions()
        .get(partition);
  }

  /** Polls the real partition state until {@code condition} holds; empty if it never did. */
  public java.util.Optional<TopicPartitionInfo> awaitPartition(
      String topic, int partition, Predicate<TopicPartitionInfo> condition, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    TopicPartitionInfo last = null;
    while (System.nanoTime() < deadline) {
      try {
        last = partition(topic, partition);
        if (condition.test(last)) return java.util.Optional.of(last);
      } catch (RuntimeException notYet) {
        // leader moving, metadata catching up: ask again
      }
      sleep(250);
    }
    return java.util.Optional.empty();
  }

  /** Deletes the topic if it exists, then creates it fresh and waits for every partition leader. */
  public void recreate(NewTopic topic) {
    var admin = admin();
    if (await(admin.listTopics().names()).contains(topic.name())) {
      try {
        await(admin.deleteTopics(List.of(topic.name())).all());
      } catch (UnknownTopicOrPartitionException gone) {
        // deleted meanwhile
      }
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (await(admin.listTopics().names()).contains(topic.name())) {
        if (System.nanoTime() > deadline)
          throw new KafkaLabErrors.ScenarioFailed(
              "Topic " + topic.name() + " was not deleted in 15 s");
        sleep(200);
      }
    }
    create(topic);
  }

  public void create(NewTopic topic) {
    // A just-deleted topic can briefly still be "marked for deletion": retry the create.
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    while (true) {
      try {
        await(admin().createTopics(List.of(topic)).all());
        break;
      } catch (org.apache.kafka.common.errors.TopicExistsException e) {
        if (System.nanoTime() > deadline) throw e;
        sleep(300);
      }
    }
    int partitions =
        topic.numPartitions() > 0 ? topic.numPartitions() : topic.replicasAssignments().size();
    for (int p = 0; p < partitions; p++)
      if (awaitPartition(topic.name(), p, i -> i.leader() != null, Duration.ofSeconds(15))
          .isEmpty())
        throw new KafkaLabErrors.ScenarioFailed(topic.name() + "-" + p + " got no leader in 15 s");
  }

  /** One record as read back from the log. */
  public record Read(int partition, long offset, String key, String value) {}

  /**
   * Every record of the topic, read from the beginning up to its end as {@code isolation} defines
   * it: the high watermark (read_uncommitted) or the last stable offset (read_committed).
   */
  public List<Read> readAll(String topic, IsolationLevel isolation) {
    // Right after an election, brokers can briefly answer metadata without the new leader.
    for (int attempt = 1; ; attempt++) {
      try {
        return readOnce(topic, isolation);
      } catch (org.apache.kafka.common.errors.TimeoutException e) {
        if (attempt == 3) throw e;
        sleep(1000);
      }
    }
  }

  private List<Read> readOnce(String topic, IsolationLevel isolation) {
    try (var consumer =
        consumer(
            Map.of(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                isolationName(isolation),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                500))) {
      var partitions =
          consumer.partitionsFor(topic, CALL).stream()
              .map(p -> new TopicPartition(topic, p.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      var end = consumer.endOffsets(partitions, CALL);
      var records = new ArrayList<Read>();
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (partitions.stream().anyMatch(tp -> consumer.position(tp, CALL) < end.get(tp))) {
        if (System.nanoTime() > deadline)
          throw new KafkaLabErrors.ScenarioFailed(
              "Could not read " + topic + " to its end in 15 s");
        for (var r : consumer.poll(Duration.ofMillis(300)))
          records.add(new Read(r.partition(), r.offset(), r.key(), r.value()));
      }
      return records;
    }
  }

  public Set<String> topics() {
    return await(admin().listTopics().names());
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  static String isolationName(IsolationLevel level) {
    return level == IsolationLevel.READ_COMMITTED ? "read_committed" : "read_uncommitted";
  }

  private void requireConfigured() {
    if (!settings.configured())
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE,
          "KAFKA_LAB_NOT_CONFIGURED",
          "The Kafka lab is not configured: start it with `docker compose --profile kafka-lab up -d`");
  }

  @Override
  public void close() {
    if (admin != null) admin.close(Duration.ofSeconds(2));
  }
}

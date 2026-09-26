package io.zeroshift.resilience;

import io.zeroshift.eventlab.EventLabSettings;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Two readings of Kafka for the resilience lab.
 *
 * <p><b>Lag</b>: for each consumer group in the saga, the broker's latest offsets minus the group's
 * committed offsets, summed over its partitions. Read through the AdminClient, one round trip per
 * reading for all groups.
 *
 * <p><b>Relay delay</b>: how long the outbox → Debezium → Kafka hop takes right now. A consumer
 * with no group (it joins nothing and commits nothing) follows the end of the outbox-fed topics
 * and, for every new record, compares Kafka's timestamp with the {@code occurredAt} the service
 * wrote into the outbox row. When the CDC link degrades this grows while nothing else looks wrong.
 */
@Component
public class KafkaProbe implements SmartLifecycle {
  /** The groups an order passes through, in the order it meets them. */
  public static final List<String> GROUPS =
      List.of(
          "payment-service",
          "inventory-service",
          "shipping-service",
          "order-saga",
          "order-projection");

  static final List<String> RELAYED = List.of("order.events", "payment.commands");
  private static final Pattern OCCURRED_AT = Pattern.compile("\"occurredAt\"\\s*:\\s*\"([^\"]+)\"");
  private static final Logger log = LoggerFactory.getLogger(KafkaProbe.class);

  public record Lag(Map<String, Long> byGroup, String error) {}

  private final EventLabSettings settings;
  private final List<Long> delays = new ArrayList<>(); // relay delays seen since the last drain
  private volatile boolean running;
  private volatile String relayError;
  private Admin admin;
  private Thread tail;

  public KafkaProbe(EventLabSettings settings) {
    this.settings = settings;
  }

  public Lag lag() {
    try {
      var specs = new HashMap<String, ListConsumerGroupOffsetsSpec>();
      for (var g : GROUPS) specs.put(g, new ListConsumerGroupOffsetsSpec());
      var committed = admin().listConsumerGroupOffsets(specs).all().get(3, TimeUnit.SECONDS);
      var partitions = new HashMap<TopicPartition, OffsetSpec>();
      committed
          .values()
          .forEach(m -> m.keySet().forEach(tp -> partitions.put(tp, OffsetSpec.latest())));
      var latest = admin().listOffsets(partitions).all().get(3, TimeUnit.SECONDS);
      var byGroup = new TreeMap<String, Long>();
      for (var g : GROUPS) {
        long lag = 0;
        for (var e : committed.getOrDefault(g, Map.of()).entrySet()) {
          var end = latest.get(e.getKey());
          if (end != null && e.getValue() != null)
            lag += Math.max(0, end.offset() - e.getValue().offset());
        }
        byGroup.put(g, lag);
      }
      return new Lag(byGroup, null);
    } catch (Exception e) {
      return new Lag(Map.of(), e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  /** Relay delays (ms) of the records that arrived since the last call. */
  public List<Long> drainRelayDelays() {
    synchronized (delays) {
      var copy = List.copyOf(delays);
      delays.clear();
      return copy;
    }
  }

  public String relayError() {
    return relayError;
  }

  private synchronized Admin admin() {
    if (admin == null)
      admin =
          Admin.create(
              Map.of(
                  AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrap(),
                  AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2500,
                  AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000));
    return admin;
  }

  @Override
  public void start() {
    running = true;
    tail = Thread.ofPlatform().name("relay-tail").daemon().start(this::follow);
  }

  private void follow() {
    while (running) {
      try (var consumer =
          new KafkaConsumer<String, String>(
              Map.of(
                  ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  settings.kafkaBootstrap(),
                  ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class,
                  ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class,
                  ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                  false,
                  ConsumerConfig.CLIENT_ID_CONFIG,
                  "resilience-relay-probe"))) {
        var partitions = new ArrayList<TopicPartition>();
        for (var topic : RELAYED)
          consumer
              .partitionsFor(topic, Duration.ofSeconds(5))
              .forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        relayError = null;
        while (running) {
          for (var r : consumer.poll(Duration.ofMillis(500))) {
            var m = OCCURRED_AT.matcher(r.value() == null ? "" : r.value());
            if (!m.find()) continue;
            long delay = r.timestamp() - Instant.parse(m.group(1)).toEpochMilli();
            synchronized (delays) {
              if (delays.size() < 10_000) delays.add(Math.max(0, delay));
            }
          }
        }
      } catch (RuntimeException e) {
        relayError = e.getClass().getSimpleName() + ": " + e.getMessage();
        log.debug("relay probe restarting", e);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException interrupted) {
          return;
        }
      }
    }
  }

  @Override
  public void stop() {
    running = false;
    if (tail != null) tail.interrupt();
    synchronized (this) {
      if (admin != null) admin.close(Duration.ofSeconds(1));
      admin = null;
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }
}

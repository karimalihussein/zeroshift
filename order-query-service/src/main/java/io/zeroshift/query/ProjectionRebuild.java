package io.zeroshift.query;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Rebuilds the read models from history: stop the consumer, empty the tables and this consumer's
 * idempotency records (otherwise every replayed event would be skipped as a duplicate), move the
 * group's committed offsets back to the start of the topic, start the consumer again. The rebuild
 * then shows up as consumer lag draining to zero.
 */
public final class ProjectionRebuild {
  public record Started(Instant at, Map<String, Long> resetTo) {}

  private final KafkaListenerEndpointRegistry registry;
  private final KafkaAdmin kafkaAdmin;
  private final JdbcTemplate jdbc;
  private final TransactionTemplate transactions;

  public ProjectionRebuild(
      KafkaListenerEndpointRegistry registry,
      KafkaAdmin kafkaAdmin,
      JdbcTemplate jdbc,
      TransactionTemplate transactions) {
    this.registry = registry;
    this.kafkaAdmin = kafkaAdmin;
    this.jdbc = jdbc;
    this.transactions = transactions;
  }

  public synchronized Started rebuild() throws Exception {
    var container = registry.getListenerContainer(OrderEvents.CONSUMER);
    container.stop(); // leaves the group, so its offsets may be altered
    try (var admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
      var partitions =
          admin
              .describeTopics(java.util.List.of(OrderEvents.TOPIC))
              .allTopicNames()
              .get()
              .get(OrderEvents.TOPIC)
              .partitions()
              .stream()
              .map(p -> new TopicPartition(OrderEvents.TOPIC, p.partition()))
              .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
      var earliest = admin.listOffsets(partitions).all().get();
      var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
      earliest.forEach((tp, info) -> offsets.put(tp, new OffsetAndMetadata(info.offset())));
      transactions.executeWithoutResult(
          s -> {
            jdbc.execute("TRUNCATE order_view, customer_summary");
            jdbc.update("DELETE FROM processed_message WHERE consumer=?", OrderEvents.CONSUMER);
          });
      admin.alterConsumerGroupOffsets(OrderEvents.CONSUMER, offsets).all().get();
      return new Started(
          Instant.now(),
          offsets.entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().offset())));
    } finally {
      container.start();
    }
  }
}

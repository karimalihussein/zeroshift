package io.zeroshift.query;

import io.zeroshift.platform.Inbox;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Rebuilds the read models from history: stop the consumer (it leaves the group, so its offsets may
 * move), move the group's committed offsets back to the start of the topic, then empty the tables
 * and this consumer's idempotency records (otherwise every replayed event would be skipped as a
 * duplicate), and start the consumer again. The rebuild shows up as consumer lag draining to zero.
 *
 * <p>Offsets move before anything is deleted. If the offset move fails, nothing has changed. If the
 * delete fails afterwards, the replay only meets idempotency records and skips every event, leaving
 * the read models as they were. The other order could leave them empty with the offsets still at
 * the end: nothing would ever refill them.
 */
public final class ProjectionRebuild {
  public record Started(Instant at, Map<String, Long> resetTo) {}

  private final KafkaListenerEndpointRegistry registry;
  private final KafkaAdmin kafkaAdmin;
  private final ReadModels readModels;
  private final Inbox inbox;
  private final TransactionOperations transactions;

  public ProjectionRebuild(
      KafkaListenerEndpointRegistry registry,
      KafkaAdmin kafkaAdmin,
      ReadModels readModels,
      Inbox inbox,
      TransactionOperations transactions) {
    this.registry = registry;
    this.kafkaAdmin = kafkaAdmin;
    this.readModels = readModels;
    this.inbox = inbox;
    this.transactions = transactions;
  }

  public synchronized Started rebuild() throws Exception {
    var container = registry.getListenerContainer(OrderEvents.CONSUMER);
    container.stop();
    try (var admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
      var partitions =
          admin
              .describeTopics(List.of(OrderEvents.TOPIC))
              .allTopicNames()
              .get()
              .get(OrderEvents.TOPIC)
              .partitions()
              .stream()
              .collect(
                  Collectors.toMap(
                      p -> new TopicPartition(OrderEvents.TOPIC, p.partition()),
                      p -> OffsetSpec.earliest()));
      var offsets =
          admin.listOffsets(partitions).all().get().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset())));
      admin.alterConsumerGroupOffsets(OrderEvents.CONSUMER, offsets).all().get();
      transactions.executeWithoutResult(
          tx -> {
            readModels.clear();
            inbox.forget(OrderEvents.CONSUMER);
          });
      return new Started(
          Instant.now(),
          offsets.entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().offset())));
    } finally {
      container.start();
    }
  }
}

package io.zeroshift.platform;

import io.zeroshift.contracts.MalformedMessageException;
import io.zeroshift.contracts.Topics;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.transaction.support.TransactionTemplate;

/** Imported explicitly by each service: {@code @Import(PlatformConfiguration.class)}. */
@Configuration(proxyBeanMethods = false)
@Import(LabAdminController.class)
public class PlatformConfiguration {
  /** Four retries at 0.5 s, 1 s, 2 s and 4 s, then the dead-letter topic. */
  public static final int MAX_RETRIES = 4;

  @Bean
  Outbox outbox(JdbcTemplate jdbc) {
    return new Outbox(jdbc);
  }

  @Bean
  DecisionLog decisionLog(JdbcTemplate jdbc) {
    return new DecisionLog(jdbc);
  }

  @Bean
  Faults faults(JdbcTemplate jdbc, TransactionTemplate transactions) {
    return new Faults(jdbc, transactions);
  }

  @Bean
  Inbox inbox(
      JdbcTemplate jdbc, TransactionTemplate transactions, DecisionLog decisions, Faults faults) {
    return new Inbox(jdbc, transactions, decisions, faults);
  }

  @Bean
  DefaultErrorHandler kafkaErrorHandler(KafkaOperations<?, ?> kafka, DecisionLog decisions) {
    var deadLetters =
        new DeadLetterPublishingRecoverer(
            kafka, (record, error) -> new TopicPartition(Topics.deadLetter(record.topic()), -1));
    var backOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
    backOff.setInitialInterval(500);
    backOff.setMultiplier(2);
    backOff.setMaxInterval(4_000);
    var handler =
        new DefaultErrorHandler(
            (record, error) -> {
              deadLetters.accept(record, error);
              decisions.record(
                  consumer(record),
                  record,
                  null,
                  Decision.DEAD_LETTERED,
                  Inbox.attempt(record),
                  "→ " + Topics.deadLetter(record.topic()) + ": " + rootMessage(error));
            },
            backOff);
    // No retry can fix these: dead-letter them on the first failure.
    handler.addNotRetryableExceptions(MalformedMessageException.class);
    // Called for every failed delivery, including the last one and poison records: log a retry
    // only when another delivery will really follow. The recoverer logs the dead letter.
    handler.setRetryListeners(
        (record, error, attempt) -> {
          if (attempt <= MAX_RETRIES && !causedBy(error, MalformedMessageException.class))
            decisions.record(
                consumer(record),
                record,
                null,
                Decision.RETRY_SCHEDULED,
                attempt,
                rootMessage(error));
        });
    return handler;
  }

  private static boolean causedBy(Throwable error, Class<? extends Throwable> type) {
    for (var e = error; e != null; e = e.getCause()) if (type.isInstance(e)) return true;
    return false;
  }

  @Bean
  ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ConsumerFactory<Object, Object> consumers,
      DefaultErrorHandler errorHandler) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    configurer.configure(factory, consumers);
    factory.setCommonErrorHandler(errorHandler);
    // Stamps each delivery with its attempt number, shown next to every decision.
    factory.getContainerProperties().setDeliveryAttemptHeader(true);
    return factory;
  }

  /** Consumers are named after their group; error handling runs on the consumer's thread. */
  private static String consumer(ConsumerRecord<?, ?> record) {
    return Objects.requireNonNullElse(KafkaUtils.getConsumerGroupId(), "unknown");
  }

  private static String rootMessage(Throwable error) {
    var root = error;
    while (root.getCause() != null) root = root.getCause();
    return root.getClass().getSimpleName() + ": " + root.getMessage();
  }
}

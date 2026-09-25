package io.zeroshift.platform;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.contracts.MalformedMessageException;
import io.zeroshift.contracts.Topics;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationInitializer;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.transaction.support.TransactionTemplate;

/** Imported explicitly by each service: {@code @Import(PlatformConfiguration.class)}. */
@Configuration(proxyBeanMethods = false)
@Import(LabAdminController.class)
public class PlatformConfiguration {
  /** Four retries at 0.5 s, 1 s, 2 s and 4 s, then the dead-letter topic. */
  public static final int MAX_RETRIES = 4;

  /**
   * The platform's tables (outbox, inbox, decisions, faults) are versioned apart from the service's
   * own schema, in their own history table, so either can gain a migration without the other's
   * version numbers getting in the way. Runs before any platform component is created.
   *
   * <p>Order matters: the service's own migrations (Spring Boot's Flyway, {@code serviceSchema})
   * run first, into an empty schema; the platform history then baselines at 0 over the now
   * non-empty schema and applies its migrations. The other way round, Spring Boot's Flyway would
   * refuse to start on a non-empty schema without a history table.
   */
  @Bean
  PlatformSchema platformSchema(
      DataSource dataSource,
      FlywayMigrationInitializer serviceSchema,
      @Value("${zeroshift.outbox-slot:true}") boolean outboxSlot) {
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/platform")
        .table("flyway_platform_history")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .placeholders(Map.of("outboxSlot", Boolean.toString(outboxSlot)))
        .load()
        .migrate();
    return new PlatformSchema();
  }

  /** Marker: depending on it guarantees the platform tables exist. */
  public static final class PlatformSchema {}

  @Bean
  Outbox outbox(DSLContext db, PlatformSchema schema) {
    return new Outbox(db);
  }

  @Bean
  DecisionLog decisionLog(
      DSLContext db,
      PlatformSchema schema,
      MeterRegistry meters,
      @Value("${zeroshift.instance:${HOSTNAME:local}}") String instance) {
    return new DecisionLog(db, instance, meters);
  }

  @Bean
  Faults faults(DSLContext db, TransactionTemplate transactions, PlatformSchema schema) {
    return new Faults(db, transactions);
  }

  @Bean
  Inbox inbox(
      DSLContext db, TransactionTemplate transactions, DecisionLog decisions, Faults faults) {
    return new Inbox(db, transactions, decisions, faults);
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
              // The dead letter's exception headers name the real cause, not Spring's listener
              // wrapper.
              deadLetters.accept(
                  record,
                  error instanceof ListenerExecutionFailedException
                          && error.getCause() instanceof Exception cause
                      ? cause
                      : error);
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
      DefaultErrorHandler errorHandler,
      @Value("${zeroshift.instance:${HOSTNAME:local}}") String instance) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    configurer.configure(factory, consumers);
    factory.setCommonErrorHandler(errorHandler);
    // Stamps each delivery with its attempt number, shown next to every decision.
    factory.getContainerProperties().setDeliveryAttemptHeader(true);
    // Kafka's default client id ("consumer-<group>-1") is the same on every replica. Naming the
    // instance makes the group's member list show which replica owns which partition.
    factory.setContainerCustomizer(
        container ->
            container
                .getContainerProperties()
                .setClientId(instance + "/" + container.getListenerId()));
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

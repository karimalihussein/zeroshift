package io.zeroshift.platform.testing;

import static io.zeroshift.platform.testing.CommerceStack.*;

import io.zeroshift.contracts.*;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Utils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;

/** Helpers for a service test that plays the other services over real Kafka. */
public final class ServiceTestSupport {
  public static final Duration WAIT = Duration.ofSeconds(60);

  /** Starts the stack and points the service at its own database, registering its connector. */
  public static void configure(DynamicPropertyRegistry registry, String database) {
    CommerceStack.start();
    var url = createDatabase(database);
    registry.add("spring.datasource.url", () -> url);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
  }

  public static Envelope send(KafkaTemplate<String, String> kafka, Message payload) {
    var envelope = Envelope.of(payload, UUID.randomUUID(), null);
    send(kafka, envelope);
    return envelope;
  }

  public static void send(KafkaTemplate<String, String> kafka, Envelope envelope) {
    kafka
        .send(envelope.topic(), envelope.orderId().toString(), MessageCodec.encode(envelope))
        .join();
  }

  /** The replies Debezium published for {@code orderId} on {@code topic}, decoded. */
  public static List<Envelope> replies(String topic, UUID orderId, int count) {
    return read(topic, forKey(orderId), count, WAIT).stream()
        .map(r -> MessageCodec.decode(r.value()))
        .toList();
  }

  public static Predicate<ConsumerRecord<String, String>> forKey(UUID orderId) {
    return r -> orderId.toString().equals(r.key());
  }

  /** A fresh order id whose messages land on {@code partition} of a three-partition topic. */
  public static UUID orderOnPartition(int partition) {
    while (true) {
      var id = UUID.randomUUID();
      if (Utils.toPositive(Utils.murmur2(id.toString().getBytes())) % 3 == partition) return id;
    }
  }

  private ServiceTestSupport() {}
}

package io.zeroshift.history;

import io.zeroshift.eventlab.EventLabSettings;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

/**
 * Kafka as a log you can travel in. Every read here is a consumer with no group: it assigns the
 * partitions itself, seeks to real offsets and commits nothing, so looking at history never moves
 * any service's position.
 */
@Component
public class KafkaHistory implements AutoCloseable {
  public record Record(
      String topic,
      int partition,
      long offset,
      Instant timestamp,
      String key,
      Map<String, String> headers,
      String value) {}

  /** Per partition: where a read starts, and where the log ends now. */
  public record Span(int partition, long from, long end) {
    public long records() {
      return Math.max(0, end - from);
    }
  }

  private final EventLabSettings settings;
  private Admin admin;
  private KafkaProducer<String, String> producer;

  public KafkaHistory(EventLabSettings settings) {
    this.settings = settings;
  }

  synchronized Admin admin() {
    if (admin == null)
      admin =
          Admin.create(
              Map.of(
                  AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrap(),
                  AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
                  AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 8000));
    return admin;
  }

  synchronized KafkaProducer<String, String> producer() {
    if (producer == null)
      producer =
          new KafkaProducer<>(
              Map.of(
                  ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  settings.kafkaBootstrap(),
                  ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class,
                  ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class,
                  ProducerConfig.ACKS_CONFIG,
                  "all",
                  ProducerConfig.CLIENT_ID_CONFIG,
                  "history-lab"));
    return producer;
  }

  public int partitions(String topic) throws Exception {
    return admin()
        .describeTopics(List.of(topic))
        .allTopicNames()
        .get(8, TimeUnit.SECONDS)
        .get(topic)
        .partitions()
        .size();
  }

  /** The partition Kafka's default partitioner picks for {@code key}: murmur2 of its bytes. */
  public static int partitionFor(String key, int partitions) {
    return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
  }

  /**
   * For every partition, the first offset whose record timestamp is at or after {@code at} (the
   * broker's time index: {@code ListOffsets} with a timestamp), or the log end if none is. With
   * {@code at} null, the earliest offset still in the log.
   */
  public List<Span> spans(String topic, Instant at) throws Exception {
    int n = partitions(topic);
    var start = new HashMap<TopicPartition, OffsetSpec>();
    var latest = new HashMap<TopicPartition, OffsetSpec>();
    for (int p = 0; p < n; p++) {
      var tp = new TopicPartition(topic, p);
      start.put(
          tp, at == null ? OffsetSpec.earliest() : OffsetSpec.forTimestamp(at.toEpochMilli()));
      latest.put(tp, OffsetSpec.latest());
    }
    var from = admin().listOffsets(start).all().get(8, TimeUnit.SECONDS);
    var end = admin().listOffsets(latest).all().get(8, TimeUnit.SECONDS);
    var spans = new ArrayList<Span>();
    for (int p = 0; p < n; p++) {
      var tp = new TopicPartition(topic, p);
      long last = end.get(tp).offset();
      long first = from.get(tp).offset();
      spans.add(new Span(p, first < 0 ? last : first, last));
    }
    return spans;
  }

  /**
   * Reads each span from its start to the end offset it had when the read began (records written
   * meanwhile are left for the next read), keeping those {@code keep} accepts, at most {@code
   * limit} of them.
   */
  public List<Record> read(String topic, List<Span> spans, Predicate<Record> keep, int limit) {
    var result = new ArrayList<Record>();
    stream(
        topic,
        spans,
        batch -> {
          for (var r : batch) if (result.size() < limit && keep.test(r)) result.add(r);
          return result.size() < limit;
        });
    return result;
  }

  /**
   * Hands every record of the spans to {@code batches}, one poll at a time, in offset order within
   * each partition; stops early when it returns false. Nothing is held beyond one batch.
   */
  public void stream(
      String topic, List<Span> spans, java.util.function.Function<List<Record>, Boolean> batches) {
    var remaining = new TreeMap<Integer, Span>();
    for (var s : spans) if (s.records() > 0) remaining.put(s.partition(), s);
    if (remaining.isEmpty()) return;
    try (var consumer = consumer()) {
      consumer.assign(remaining.keySet().stream().map(p -> new TopicPartition(topic, p)).toList());
      for (var s : remaining.values())
        consumer.seek(new TopicPartition(topic, s.partition()), s.from());
      long idleSince = System.nanoTime();
      while (!remaining.isEmpty()) {
        var batch = new ArrayList<Record>();
        for (var r : consumer.poll(Duration.ofMillis(300))) {
          var span = remaining.get(r.partition());
          if (span != null && r.offset() < span.end()) batch.add(record(r));
        }
        if (!batch.isEmpty()) {
          idleSince = System.nanoTime();
          if (!batches.apply(batch)) return;
        } else if (System.nanoTime() - idleSince > Duration.ofSeconds(15).toNanos())
          throw new IllegalStateException("Kafka stopped answering while reading " + topic);
        // Done with a partition once its position reaches the end; a compacted partition can end
        // in a gap, with no record at end - 1.
        for (var p : List.copyOf(remaining.keySet()))
          if (consumer.position(new TopicPartition(topic, p)) >= remaining.get(p).end())
            remaining.remove(p);
      }
    }
  }

  /** Writes one record (a null value is a tombstone) and returns where it landed. */
  public org.apache.kafka.clients.producer.RecordMetadata send(
      String topic, String key, String value) throws Exception {
    return producer()
        .send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, key, value))
        .get(10, TimeUnit.SECONDS);
  }

  private KafkaConsumer<String, String> consumer() {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            settings.kafkaBootstrap(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            false,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            2000,
            ConsumerConfig.CLIENT_ID_CONFIG,
            "history-lab-reader"));
  }

  static Record record(ConsumerRecord<String, String> r) {
    var headers = new TreeMap<String, String>();
    for (var h : r.headers())
      headers.put(h.key(), io.zeroshift.eventlab.EventTap.header(h.value()));
    return new Record(
        r.topic(),
        r.partition(),
        r.offset(),
        Instant.ofEpochMilli(r.timestamp()),
        r.key(),
        headers,
        r.value());
  }

  @Override
  public synchronized void close() {
    if (admin != null) admin.close(Duration.ofSeconds(1));
    if (producer != null) producer.close(Duration.ofSeconds(1));
  }
}

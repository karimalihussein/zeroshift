package io.zeroshift.kafkalab;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A consume-transform-produce worker, run as its own JVM so that a crash is a real one: at the
 * chosen point it calls {@link Runtime#halt}, which skips every finally block, shutdown hook,
 * offset commit and transaction commit. What survives is exactly what Kafka had at that moment.
 *
 * <p>For every input record it writes one output record. The order of "write the output" and
 * "commit the input offset" is the delivery guarantee:
 *
 * <ul>
 *   <li>AT_MOST_ONCE: commit, then write. A crash in between loses records.
 *   <li>AT_LEAST_ONCE: write, then commit. A crash in between writes records twice.
 *   <li>EXACTLY_ONCE: write and commit in one Kafka transaction. A crash aborts both.
 * </ul>
 *
 * <p>Arguments: bootstrap, mode, run id, crash-at sequence (0 = never), worker number. One line per
 * step goes to stdout for the control plane to show.
 */
public final class DeliveryWorker {
  public enum Mode {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
  }

  static final int CRASH_EXIT = 137;

  private DeliveryWorker() {}

  public static void main(String[] args) {
    var bootstrap = args[0];
    var mode = Mode.valueOf(args[1]);
    var run = args[2];
    int crashAt = Integer.parseInt(args[3]);
    var worker = args[4];

    var consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group(run));
    // Static membership: the restarted worker takes over the crashed one's assignment at once
    // instead of waiting for its session to expire.
    consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "worker");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
    consumerProps.putAll(LabKafka.FAST_FAILING_CONNECTIONS);
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    var producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    producerProps.putAll(LabKafka.FAST_FAILING_CONNECTIONS);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    if (mode == Mode.EXACTLY_ONCE)
      // The same transactional.id across restarts: the new worker fences the crashed one and
      // aborts its open transaction before doing anything.
      producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, group(run));

    try (var consumer = new KafkaConsumer<String, String>(consumerProps);
        var producer = new KafkaProducer<String, String>(producerProps)) {
      if (mode == Mode.EXACTLY_ONCE) {
        producer.initTransactions();
        say("init", "initTransactions: any transaction the previous worker left open is aborted");
      }
      consumer.subscribe(List.of(LabTopics.DELIVERY_IN));
      say(
          "start",
          "worker "
              + worker
              + " ("
              + mode
              + ") polling "
              + LabTopics.DELIVERY_IN
              + (crashAt > 0 ? ", will crash at record " + crashAt : ""));
      long idleSince = System.currentTimeMillis();
      while (true) {
        var records = consumer.poll(Duration.ofMillis(300));
        if (records.isEmpty()) {
          if (System.currentTimeMillis() - idleSince > 3000 && caughtUp(consumer)) break;
          continue;
        }
        idleSince = System.currentTimeMillis();
        switch (mode) {
          case AT_MOST_ONCE -> atMostOnce(consumer, producer, records, crashAt, worker);
          case AT_LEAST_ONCE -> atLeastOnce(consumer, producer, records, crashAt, worker);
          case EXACTLY_ONCE -> exactlyOnce(consumer, producer, records, crashAt, worker);
        }
      }
      say("done", "worker " + worker + ": input fully consumed, exiting cleanly");
    }
  }

  private static void atMostOnce(
      KafkaConsumer<String, String> consumer,
      KafkaProducer<String, String> producer,
      ConsumerRecords<String, String> records,
      int crashAt,
      String worker) {
    consumer.commitSync(next(records));
    say("commit", "committed offsets " + describe(next(records)) + " BEFORE writing any output");
    for (var r : records) {
      if (seq(r) == crashAt)
        crash("after committing record " + crashAt + "'s offset, before writing its output");
      write(producer, r, worker);
    }
  }

  private static void atLeastOnce(
      KafkaConsumer<String, String> consumer,
      KafkaProducer<String, String> producer,
      ConsumerRecords<String, String> records,
      int crashAt,
      String worker) {
    for (var r : records) {
      write(producer, r, worker);
      if (seq(r) == crashAt)
        crash("after writing record " + crashAt + "'s output, before committing its offset");
    }
    consumer.commitSync(next(records));
    say("commit", "committed offsets " + describe(next(records)) + " AFTER writing the output");
  }

  private static void exactlyOnce(
      KafkaConsumer<String, String> consumer,
      KafkaProducer<String, String> producer,
      ConsumerRecords<String, String> records,
      int crashAt,
      String worker) {
    producer.beginTransaction();
    for (var r : records) {
      producer.send(
          new ProducerRecord<>(LabTopics.DELIVERY_OUT, r.key(), r.value() + ":w" + worker));
      say(
          "write",
          "record "
              + seq(r)
              + " (p"
              + r.partition()
              + "@"
              + r.offset()
              + ") written inside the transaction");
      if (seq(r) == crashAt) {
        producer.flush(); // on the brokers, but not committed
        crash(
            "mid-transaction: record "
                + crashAt
                + "'s output is in the log but uncommitted, offsets not committed");
      }
    }
    producer.sendOffsetsToTransaction(next(records), consumer.groupMetadata());
    producer.commitTransaction();
    say(
        "commit",
        "transaction committed: outputs and offsets " + describe(next(records)) + " together");
  }

  private static void write(
      KafkaProducer<String, String> producer, ConsumerRecord<String, String> r, String worker) {
    try {
      producer
          .send(new ProducerRecord<>(LabTopics.DELIVERY_OUT, r.key(), r.value() + ":w" + worker))
          .get();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    say(
        "write",
        "record "
            + seq(r)
            + " (p"
            + r.partition()
            + "@"
            + r.offset()
            + ") written and acknowledged");
  }

  private static boolean caughtUp(KafkaConsumer<String, String> consumer) {
    var assigned = consumer.assignment();
    if (assigned.isEmpty()) return false;
    var end = consumer.endOffsets(assigned);
    return assigned.stream().allMatch(tp -> consumer.position(tp) >= end.get(tp));
  }

  private static Map<TopicPartition, OffsetAndMetadata> next(
      ConsumerRecords<String, String> records) {
    var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
    for (var tp : records.partitions()) {
      var list = records.records(tp);
      offsets.put(tp, new OffsetAndMetadata(list.getLast().offset() + 1));
    }
    return offsets;
  }

  private static String describe(Map<TopicPartition, OffsetAndMetadata> offsets) {
    var parts = new StringBuilder();
    offsets.forEach(
        (tp, o) ->
            parts
                .append(parts.isEmpty() ? "" : ", ")
                .append("p")
                .append(tp.partition())
                .append("→")
                .append(o.offset()));
    return parts.toString();
  }

  static int seq(ConsumerRecord<String, String> r) {
    var v = r.value();
    return Integer.parseInt(v.substring(v.lastIndexOf(':') + 1));
  }

  static String group(String run) {
    return "lab.delivery." + run;
  }

  private static void crash(String where) {
    say("crash", "CRASH " + where);
    System.out.flush();
    Runtime.getRuntime().halt(CRASH_EXIT);
  }

  private static void say(String kind, String text) {
    System.out.println("EVENT " + kind + " " + text);
    System.out.flush();
  }
}

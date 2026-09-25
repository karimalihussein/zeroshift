package io.zeroshift.shipping;

import io.zeroshift.contracts.CarrierEvent.ParcelScanned;
import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import io.zeroshift.contracts.Topics;
import io.zeroshift.platform.Faults;
import io.zeroshift.platform.Inbox;
import io.zeroshift.platform.web.ApiException;
import io.zeroshift.platform.web.ErrorCodes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The ordering lab's external carrier: it publishes real scan events for real shipped parcels to
 * shipping.carrier-scans, keyed the way the operator chooses, and changes the topic's partitioning.
 * What happens next is Kafka's and the tracking projection's doing, recorded per scan (partition,
 * offset, outcome).
 */
public final class Carrier {
  /** How the carrier keys its scans, which decides the partition of each one. */
  public enum Keying {
    /** Correct: one parcel, one key, one partition, one order. */
    TRACKING,
    /** A bug: every scan has its own key, so one parcel's scans spread over partitions. */
    SCAN,
    /**
     * Ordered but hot: every scan from one hub shares a key, so one partition does all the work.
     */
    HUB
  }

  /** Faults entry: "partitions:ms", e.g. "0:700" or "0,1,2:700": a slow shard. */
  public static final String SLOW = "carrier-slow";

  /** One scan as published: where Kafka put it. */
  public record Produced(
      String trackingNumber, int seq, String status, String key, int partition, long offset) {}

  public record ScanRun(Keying keying, List<Produced> produced, int partitions) {}

  public static final List<String> STATUSES =
      List.of("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED");
  static final int PARTITIONS = 3;

  private final Shipments shipments;
  private final Tracking tracking;
  private final Inbox inbox;
  private final Faults faults;
  private final KafkaTemplate<String, String> kafka;
  private final KafkaAdmin kafkaAdmin;
  private final KafkaListenerEndpointRegistry registry;
  private final TransactionTemplate transactions;

  public Carrier(
      Shipments shipments,
      Tracking tracking,
      Inbox inbox,
      Faults faults,
      KafkaTemplate<String, String> kafka,
      KafkaAdmin kafkaAdmin,
      KafkaListenerEndpointRegistry registry,
      TransactionTemplate transactions) {
    this.shipments = shipments;
    this.tracking = tracking;
    this.inbox = inbox;
    this.faults = faults;
    this.kafka = kafka;
    this.kafkaAdmin = kafkaAdmin;
    this.registry = registry;
    this.transactions = transactions;
  }

  /**
   * Publishes scans {@code fromSeq..toSeq} for the newest {@code parcels} shipped parcels, one
   * round of scans at a time across parcels, as a carrier's scanners would. Starting at scan 1
   * begins a fresh trip: those parcels' tracking is forgotten first.
   */
  public ScanRun scan(Keying mode, int parcels, int fromSeq, int toSeq) throws Exception {
    if (fromSeq > toSeq)
      throw new ApiException(
          HttpStatus.BAD_REQUEST,
          ErrorCodes.INVALID_PARAMETER,
          "fromSeq " + fromSeq + " is after toSeq " + toSeq);
    var shipped = shipments.recentScheduled(parcels);
    if (shipped.isEmpty())
      throw new ApiException(
          HttpStatus.CONFLICT,
          "NO_SHIPPED_PARCELS",
          "No shipped parcels yet: complete an order first");
    if (fromSeq == 1)
      transactions.executeWithoutResult(
          tx -> tracking.clear(shipped.stream().map(Shipments.Shipped::trackingNumber).toList()));
    var produced = new ArrayList<Produced>();
    for (int seq = fromSeq; seq <= toSeq; seq++)
      for (var parcel : shipped) {
        var hub =
            List.of("AMS", "FRA", "LHR", "CDG")
                .get(Math.floorMod(parcel.trackingNumber().hashCode() + seq, 4));
        var envelope =
            Envelope.of(
                new ParcelScanned(
                    parcel.orderId(), parcel.trackingNumber(), seq, STATUSES.get(seq - 1), hub),
                parcel.orderId(),
                null);
        var key =
            switch (mode) {
              case TRACKING -> parcel.trackingNumber();
              case SCAN -> envelope.eventId().toString();
              case HUB -> "hub-AMS";
            };
        var sent =
            kafka
                .send(Topics.CARRIER_SCANS, key, MessageCodec.encode(envelope))
                .get(10, TimeUnit.SECONDS);
        produced.add(
            new Produced(
                parcel.trackingNumber(),
                seq,
                STATUSES.get(seq - 1),
                key,
                sent.getRecordMetadata().partition(),
                sent.getRecordMetadata().offset()));
      }
    return new ScanRun(mode, produced, partitionCount());
  }

  /** Adds partitions. Kafka can only add them: existing keys may now hash to a different one. */
  public record Partitions(int partitions, Integer was) {}

  public Partitions addPartitions(int count) throws Exception {
    int current = partitionCount();
    if (count <= current)
      throw new ApiException(
          HttpStatus.BAD_REQUEST,
          "PARTITIONS_CAN_ONLY_GROW",
          "Partitions can only grow: the topic has " + current,
          Map.of("partitions", current));
    try (var admin = admin()) {
      admin
          .createPartitions(Map.of(Topics.CARRIER_SCANS, NewPartitions.increaseTo(count)))
          .all()
          .get(10, TimeUnit.SECONDS);
    }
    return new Partitions(count, current);
  }

  /**
   * Recovery from repartitioning: partitions cannot be removed, so the topic is deleted and
   * recreated with 3, and the tracking projection starts empty.
   */
  public Partitions recreateTopic() throws Exception {
    var container = registry.getListenerContainer(Tracking.CONSUMER);
    container.stop();
    try (var admin = admin()) {
      admin.deleteTopics(List.of(Topics.CARRIER_SCANS)).all().get(10, TimeUnit.SECONDS);
      for (int i = 0;
          i < 50 && admin.listTopics().names().get().contains(Topics.CARRIER_SCANS);
          i++) Thread.sleep(200);
      admin
          .createTopics(
              List.of(
                  new NewTopic(Topics.CARRIER_SCANS, PARTITIONS, (short) 1)
                      .configs(Map.of("retention.ms", "86400000"))))
          .all()
          .get(10, TimeUnit.SECONDS);
      transactions.executeWithoutResult(
          tx -> {
            tracking.clear(null);
            inbox.forget(Tracking.CONSUMER);
          });
      // A live producer still caches the old topic's partitions: a record it assigns to a
      // partition that no longer exists waits in retries. Start the next send from a new one.
      kafka.getProducerFactory().reset();
    } finally {
      container.start();
    }
    return new Partitions(PARTITIONS, null);
  }

  /** Replays every scan still on the topic into an empty tracking projection. */
  /** Returns how many partitions were rewound. */
  public int replay() throws Exception {
    var container = registry.getListenerContainer(Tracking.CONSUMER);
    container.stop(); // leaves the group, so its offsets may move
    try (var admin = admin()) {
      var partitions =
          admin
              .describeTopics(List.of(Topics.CARRIER_SCANS))
              .allTopicNames()
              .get()
              .get(Topics.CARRIER_SCANS)
              .partitions()
              .stream()
              .collect(
                  Collectors.toMap(
                      p -> new TopicPartition(Topics.CARRIER_SCANS, p.partition()),
                      p -> OffsetSpec.earliest()));
      var offsets =
          admin.listOffsets(partitions).all().get().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue().offset())));
      // Offsets first, then the data: a failure in between leaves the old projection in place.
      admin.alterConsumerGroupOffsets(Tracking.CONSUMER, offsets).all().get(10, TimeUnit.SECONDS);
      transactions.executeWithoutResult(
          tx -> {
            tracking.clear(null);
            inbox.forget(Tracking.CONSUMER);
          });
      return offsets.size();
    } finally {
      container.start();
    }
  }

  /** The armed slow-partition fault, if any. */
  public String slowFault() {
    return faults.armed().stream()
        .filter(f -> SLOW.equals(f.name()))
        .map(Faults.Fault::mode)
        .findFirst()
        .orElse(null);
  }

  /** "0,2:700" → partitions {0, 2}, 700 ms. Empty when the fault is not armed or unreadable. */
  static Optional<Map.Entry<List<Integer>, Long>> slowness(String mode) {
    try {
      var parts = mode.split(":");
      var partitions =
          java.util.Arrays.stream(parts[0].split(","))
              .map(String::trim)
              .map(Integer::valueOf)
              .toList();
      return Optional.of(Map.entry(partitions, Long.parseLong(parts[1].trim())));
    } catch (RuntimeException unreadable) {
      return Optional.empty();
    }
  }

  public int partitionCount() throws Exception {
    try (var admin = admin()) {
      return admin
          .describeTopics(List.of(Topics.CARRIER_SCANS))
          .allTopicNames()
          .get(10, TimeUnit.SECONDS)
          .get(Topics.CARRIER_SCANS)
          .partitions()
          .size();
    }
  }

  private Admin admin() {
    return Admin.create(kafkaAdmin.getConfigurationProperties());
  }

  /**
   * The tracking projection's consumer: a slow-partition fault delays records on chosen partitions.
   */
  public static class Scans {
    private final Inbox inbox;
    private final Tracking tracking;
    private final Faults faults;

    public Scans(Inbox inbox, Tracking tracking, Faults faults) {
      this.inbox = inbox;
      this.tracking = tracking;
      this.faults = faults;
    }

    @org.springframework.kafka.annotation.KafkaListener(
        id = Tracking.CONSUMER,
        topics = Topics.CARRIER_SCANS,
        concurrency = "3")
    public void onScan(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record)
        throws InterruptedException {
      var slow = faults.trigger(SLOW).flatMap(Carrier::slowness);
      if (slow.isPresent() && slow.get().getKey().contains(record.partition()))
        Thread.sleep(slow.get().getValue());
      inbox.deliver(Tracking.CONSUMER, record, envelope -> tracking.apply(envelope, record));
    }
  }
}

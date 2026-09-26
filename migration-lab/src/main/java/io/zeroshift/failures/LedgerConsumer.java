package io.zeroshift.failures;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import tools.jackson.databind.json.JsonMapper;

/**
 * The service whose state the disaster-recovery lab destroys: it applies PaymentCaptured events to
 * a ledger (one entry per event, a balance per account). Two designs of the same service:
 *
 * <ul>
 *   <li><b>NAIVE</b>: its position is the consumer group's committed offset in Kafka, committed
 *       after each database transaction. The ledger has no idea which events it holds.
 *   <li><b>SAFE</b>: its position is a row per partition in its own database, written in the same
 *       transaction as the entries; each event id is applied at most once (primary key). A backup
 *       of the database therefore carries the exact position to resume from.
 * </ul>
 *
 * A run reads until it reaches the end offsets taken when it started, then stops: the lab runs the
 * service in batches, so "down" and "up" are exact points in time.
 */
final class LedgerConsumer {
  enum Design {
    NAIVE,
    SAFE
  }

  static final String TOPIC = "lab.dr.payments";

  record Drain(
      Design design,
      Map<Integer, Long> startedAt,
      Map<Integer, Long> endedAt,
      int read,
      int applied,
      int skippedDuplicates,
      long millis) {}

  private final Design design;
  private final String bootstrap;
  private final FailureDb db;
  private final String database;
  private final JsonMapper json = JsonMapper.builder().build();

  LedgerConsumer(Design design, String bootstrap, FailureDb db, String database) {
    this.design = design;
    this.bootstrap = bootstrap;
    this.db = db;
    this.database = database;
  }

  String group() {
    return design == Design.NAIVE ? "lab-dr-naive" : "lab-dr-safe";
  }

  static void createSchema(FailureDb db, String database, Design design) throws SQLException {
    db.execute(
        database,
        "CREATE TABLE account (id text PRIMARY KEY, balance numeric(14,2) NOT NULL DEFAULT 0, entries int NOT NULL DEFAULT 0)",
        design == Design.SAFE
            ? "CREATE TABLE ledger_entry (event_id uuid PRIMARY KEY, account_id text NOT NULL, amount numeric(12,2) NOT NULL,"
                + " kafka_partition int NOT NULL, kafka_offset bigint NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
            : "CREATE TABLE ledger_entry (id bigserial PRIMARY KEY, event_id uuid NOT NULL, account_id text NOT NULL, amount numeric(12,2) NOT NULL,"
                + " kafka_partition int NOT NULL, kafka_offset bigint NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())",
        design == Design.SAFE
            ? "CREATE TABLE consumer_position (topic text NOT NULL, kafka_partition int NOT NULL, next_offset bigint NOT NULL,"
                + " updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY (topic, kafka_partition))"
            : "SELECT 1");
  }

  /** Reads from the design's own position to the current end of the topic, applying as it goes. */
  Drain drain(Duration timeout) throws Exception {
    return run(null, false, timeout);
  }

  /**
   * The naive ledger's repair: replay from explicit offsets (found by timestamp in Kafka's time
   * index), skipping every event whose id the ledger already holds, then leave the group's offsets
   * at the end.
   */
  Drain replayFrom(Map<Integer, Long> from, Duration timeout) throws Exception {
    return run(from, true, timeout);
  }

  private Drain run(Map<Integer, Long> from, boolean dedupeByEventId, Duration timeout)
      throws Exception {
    long t0 = System.nanoTime();
    try (var consumer = consumer();
        var c = db.connect(database)) {
      var partitions =
          consumer.partitionsFor(TOPIC).stream()
              .map(p -> new TopicPartition(TOPIC, p.partition()))
              .toList();
      consumer.assign(partitions);
      var end = consumer.endOffsets(partitions);
      var start = new LinkedHashMap<Integer, Long>();
      for (var tp : partitions) {
        long position;
        if (from != null) position = from.getOrDefault(tp.partition(), 0L);
        else if (design == Design.SAFE) position = storedPosition(c, tp.partition());
        else {
          var committed = consumer.committed(java.util.Set.of(tp)).get(tp);
          position = committed == null ? 0 : committed.offset();
        }
        consumer.seek(tp, position);
        start.put(tp.partition(), position);
      }
      int read = 0, applied = 0, skipped = 0;
      long deadline = System.nanoTime() + timeout.toNanos();
      while (!reached(consumer, end)) {
        if (System.nanoTime() > deadline)
          throw new IllegalStateException(design + " ledger did not reach the end offsets " + end);
        var records = consumer.poll(Duration.ofMillis(300));
        if (records.isEmpty()) continue;
        var batch = new ArrayList<ConsumerRecord<String, String>>();
        records.forEach(batch::add);
        read += batch.size();
        int[] outcome = apply(c, batch, dedupeByEventId);
        applied += outcome[0];
        skipped += outcome[1];
        // The naive design's position lives in Kafka, committed after the database commit. The
        // safe design commits too, for the dashboard only: it never reads it back.
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var tp : partitions) offsets.put(tp, new OffsetAndMetadata(consumer.position(tp)));
        consumer.commitSync(offsets);
      }
      var ended = new LinkedHashMap<Integer, Long>();
      for (var tp : partitions) ended.put(tp.partition(), consumer.position(tp));
      return new Drain(
          design, start, ended, read, applied, skipped, (System.nanoTime() - t0) / 1_000_000);
    }
  }

  private static boolean reached(
      KafkaConsumer<String, String> consumer, Map<TopicPartition, Long> end) {
    for (var e : end.entrySet()) if (consumer.position(e.getKey()) < e.getValue()) return false;
    return true;
  }

  /** One database transaction per poll: entries, balances and (safe) positions together. */
  private int[] apply(Connection c, List<ConsumerRecord<String, String>> batch, boolean dedupe)
      throws SQLException {
    int applied = 0, skipped = 0;
    c.setAutoCommit(false);
    try {
      var last = new HashMap<Integer, Long>();
      for (var r : batch) {
        var event = json.readTree(r.value());
        var id = UUID.fromString(event.path("eventId").asString());
        var account = event.path("account").asString();
        var amount = new BigDecimal(event.path("amount").asString());
        boolean insert;
        if (design == Design.SAFE) {
          try (var s =
              c.prepareStatement(
                  "INSERT INTO ledger_entry(event_id, account_id, amount, kafka_partition, kafka_offset) VALUES (?, ?, ?, ?, ?)"
                      + " ON CONFLICT (event_id) DO NOTHING")) {
            s.setObject(1, id);
            s.setString(2, account);
            s.setBigDecimal(3, amount);
            s.setInt(4, r.partition());
            s.setLong(5, r.offset());
            insert = s.executeUpdate() == 1;
          }
        } else {
          insert =
              !dedupe
                  || !FailureDb.one(
                          c, "SELECT 1 AS present FROM ledger_entry WHERE event_id = ? LIMIT 1", id)
                      .containsKey("present");
          if (insert)
            try (var s =
                c.prepareStatement(
                    "INSERT INTO ledger_entry(event_id, account_id, amount, kafka_partition, kafka_offset) VALUES (?, ?, ?, ?, ?)")) {
              s.setObject(1, id);
              s.setString(2, account);
              s.setBigDecimal(3, amount);
              s.setInt(4, r.partition());
              s.setLong(5, r.offset());
              s.executeUpdate();
            }
        }
        if (insert) {
          try (var s =
              c.prepareStatement(
                  "INSERT INTO account(id, balance, entries) VALUES (?, ?, 1)"
                      + " ON CONFLICT (id) DO UPDATE SET balance = account.balance + excluded.balance, entries = account.entries + 1")) {
            s.setString(1, account);
            s.setBigDecimal(2, amount);
            s.executeUpdate();
          }
          applied++;
        } else skipped++;
        last.put(r.partition(), r.offset() + 1);
      }
      if (design == Design.SAFE)
        for (var e : last.entrySet())
          try (var s =
              c.prepareStatement(
                  "INSERT INTO consumer_position(topic, kafka_partition, next_offset) VALUES (?, ?, ?)"
                      + " ON CONFLICT (topic, kafka_partition) DO UPDATE SET next_offset = excluded.next_offset, updated_at = clock_timestamp()")) {
            s.setString(1, TOPIC);
            s.setInt(2, e.getKey());
            s.setLong(3, e.getValue());
            s.executeUpdate();
          }
      c.commit();
    } catch (SQLException e) {
      c.rollback();
      throw e;
    } finally {
      c.setAutoCommit(true);
    }
    return new int[] {applied, skipped};
  }

  private static long storedPosition(Connection c, int partition) throws SQLException {
    var row =
        FailureDb.one(
            c,
            "SELECT next_offset FROM consumer_position WHERE topic = ? AND kafka_partition = ?",
            TOPIC,
            partition);
    return row.isEmpty() ? 0 : ((Number) row.get("next_offset")).longValue();
  }

  /** The safe design's positions as its database holds them. */
  static Map<Integer, Long> storedPositions(FailureDb db, String database) throws SQLException {
    var out = new LinkedHashMap<Integer, Long>();
    try (var c = db.connect(database)) {
      for (var r :
          FailureDb.rows(
              c,
              "SELECT kafka_partition, next_offset FROM consumer_position WHERE topic = ? ORDER BY kafka_partition",
              TOPIC))
        out.put(
            ((Number) r.get("kafka_partition")).intValue(),
            ((Number) r.get("next_offset")).longValue());
    }
    return out;
  }

  /** Sets the safe design's stored positions (a deliberate rewind, to show idempotent replay). */
  static void rewind(FailureDb db, String database, Map<Integer, Long> to) throws SQLException {
    try (var c = db.connect(database);
        var s =
            c.prepareStatement(
                "UPDATE consumer_position SET next_offset = ?, updated_at = clock_timestamp() WHERE topic = ? AND kafka_partition = ?")) {
      for (var e : to.entrySet()) {
        s.setLong(1, e.getValue());
        s.setString(2, TOPIC);
        s.setInt(3, e.getKey());
        s.executeUpdate();
      }
    }
  }

  private KafkaConsumer<String, String> consumer() {
    var p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, group());
    p.put(
        ConsumerConfig.CLIENT_ID_CONFIG,
        group() + "-" + UUID.randomUUID().toString().substring(0, 6));
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");
    return new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer());
  }
}

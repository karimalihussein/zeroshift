package io.zeroshift.failures;

import static io.zeroshift.failures.Checks.check;
import static io.zeroshift.failures.Checks.compare;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.eventlab.EventLabSettings;
import io.zeroshift.failures.LedgerConsumer.Design;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Back up a Kafka-fed database, keep processing, lose the database, restore the backup. Kafka's
 * offsets have moved on while the restored database went back in time: the gap between them is
 * exactly the business effects the restore lost. Which design survives depends on where the
 * consumer keeps its position.
 */
@Component
public class DisasterRecoveryLab implements FailureLab {
  static final String NAIVE_DB = "fl_ledger_naive";
  static final String SAFE_DB = "fl_ledger_safe";
  static final String BACKUP_SUFFIX = "_backup";
  static final int FIRST_BATCH = 40;
  static final int SECOND_BATCH = 25;
  static final List<String> ACCOUNTS = List.of("acct-1", "acct-2", "acct-3", "acct-4", "acct-5");
  static final Duration DRAIN = Duration.ofSeconds(60);
  /** How far before the backup the naive repair starts replaying. */
  static final Duration REPLAY_MARGIN = Duration.ofSeconds(60);

  private final FailureDb db;
  private final EventLabSettings kafka;
  private final JsonMapper json = JsonMapper.builder().build();
  private final AtomicLong rpoSeconds = new AtomicLong(-1);
  private final AtomicLong rpoEvents = new AtomicLong(-1);
  private final AtomicLong rtoMillisSafe = new AtomicLong(-1);
  private final AtomicLong rtoMillisNaive = new AtomicLong(-1);

  public DisasterRecoveryLab(FailureDb db, EventLabSettings kafka, MeterRegistry meters) {
    this.db = db;
    this.kafka = kafka;
    meters.gauge("zeroshift.failure_lab.dr.rpo_seconds", rpoSeconds);
    meters.gauge("zeroshift.failure_lab.dr.rpo_events", rpoEvents);
    meters.gauge(
        "zeroshift.failure_lab.dr.rto_millis",
        List.of(io.micrometer.core.instrument.Tag.of("design", "safe")),
        rtoMillisSafe);
    meters.gauge(
        "zeroshift.failure_lab.dr.rto_millis",
        List.of(io.micrometer.core.instrument.Tag.of("design", "naive")),
        rtoMillisNaive);
  }

  @Override
  public String id() {
    return "disaster-recovery";
  }

  @Override
  public String title() {
    return "Disaster recovery";
  }

  @Override
  public String summary() {
    return "Two ledgers consume the same PaymentCaptured events from "
        + LedgerConsumer.TOPIC
        + ". Both"
        + " databases are backed up, "
        + SECOND_BATCH
        + " more events are processed, both databases are"
        + " dropped and restored from the backup. Kafka still remembers the newer offsets.";
  }

  @Override
  public String naive() {
    return "Back up the database and let Kafka's consumer group remember the position. After a restore the"
        + " group resumes where it was before the disaster, past every event the backup does not contain.";
  }

  @Override
  public String correct() {
    return "Keep the position in the database, in the same transaction as the effects, and apply each event"
        + " id once. A restored backup carries its own position; replay from there recovers everything,"
        + " and replaying too much is harmless.";
  }

  @Override
  public List<String> plan() {
    return List.of(
        "Recreate the topic, both ledger databases and consumer groups. Publish "
            + FIRST_BATCH
            + " PaymentCaptured events, let both ledgers process them, check both against the topic, then snapshot both databases (CREATE DATABASE … TEMPLATE) and record every position at that moment.",
        "Publish "
            + SECOND_BATCH
            + " more events and let both ledgers process them (Kafka's committed offsets move on). Then the disaster: the service is down and both databases are dropped.",
        "Restore the naive ledger from its backup and start it. It resumes from the group's committed offset in Kafka, which is already at the end: it reads nothing. Compare with the topic: the missing events are the gap between Kafka's offset and the restored data. Measure RPO.",
        "Restore the safe ledger and start it. It resumes from the positions stored inside the backup and replays exactly what the backup missed. Then rewind it to offset 0 on purpose: every event is delivered again and none is applied twice. Measure RTO.",
        "Repair the naive ledger: find the instant one minute before the backup in Kafka's time index, replay from there and skip event ids already in the ledger. Prove both ledgers now hold every event exactly once and every balance matches the topic.");
  }

  @Override
  public List<String> requires() {
    return List.of(
        "failure-postgres", "the event lab's Kafka (topic " + LedgerConsumer.TOPIC + ")");
  }

  @Override
  public ObjectNode run(int stage, ObjectNode memo, Trace trace) throws Exception {
    db.requireConfigured();
    var result = json.createObjectNode();
    var naive = new LedgerConsumer(Design.NAIVE, kafka.kafkaBootstrap(), db, NAIVE_DB);
    var safe = new LedgerConsumer(Design.SAFE, kafka.kafkaBootstrap(), db, SAFE_DB);
    switch (stage) {
      case 0 -> {
        result.set("reset", reset());
        result.set("published", json.valueToTree(publish(FIRST_BATCH)));
        result.set("naiveRun", json.valueToTree(naive.drain(DRAIN)));
        result.set("safeRun", json.valueToTree(safe.drain(DRAIN)));
        var truth = truth();
        check(
            result,
            "naive ledger = topic",
            "0 missing, 0 duplicates",
            diff(truth, NAIVE_DB).get("summary"),
            diff(truth, NAIVE_DB).get("summary").equals("0 missing, 0 duplicates"));
        check(
            result,
            "safe ledger = topic",
            "0 missing, 0 duplicates",
            diff(truth, SAFE_DB).get("summary"),
            diff(truth, SAFE_DB).get("summary").equals("0 missing, 0 duplicates"));
        long t0 = System.nanoTime();
        var backupAt = Instant.now();
        db.copy(NAIVE_DB, NAIVE_DB + BACKUP_SUFFIX);
        db.copy(SAFE_DB, SAFE_DB + BACKUP_SUFFIX);
        var backup = new LinkedHashMap<String, Object>();
        backup.put("at", backupAt.toString());
        backup.put(
            "method",
            "CREATE DATABASE … TEMPLATE … STRATEGY FILE_COPY (writers stopped while it copies)");
        backup.put("millis", (System.nanoTime() - t0) / 1_000_000);
        backup.put("naiveBytes", db.size(NAIVE_DB + BACKUP_SUFFIX));
        backup.put("safeBytes", db.size(SAFE_DB + BACKUP_SUFFIX));
        backup.put("topicEndOffsets", endOffsets());
        backup.put("naiveGroupOffsetsInKafka", groupOffsets(naive.group()));
        backup.put(
            "safePositionsInsideBackup",
            LedgerConsumer.storedPositions(db, SAFE_DB + BACKUP_SUFFIX));
        result.set("backup", json.valueToTree(backup));
        memo.put("backupAt", backupAt.toString());
        trace.event("backup", Map.of("at", backupAt.toString()));
      }
      case 1 -> {
        result.set("published", json.valueToTree(publish(SECOND_BATCH)));
        result.set("naiveRun", json.valueToTree(naive.drain(DRAIN)));
        result.set("safeRun", json.valueToTree(safe.drain(DRAIN)));
        result.set("topicEndOffsets", json.valueToTree(endOffsets()));
        result.set("naiveGroupOffsetsInKafka", json.valueToTree(groupOffsets(naive.group())));
        result.set(
            "safePositionsInDatabase",
            json.valueToTree(LedgerConsumer.storedPositions(db, SAFE_DB)));
        var disasterAt = Instant.now();
        db.drop(NAIVE_DB);
        db.drop(SAFE_DB);
        memo.put("disasterAt", disasterAt.toString());
        trace.event("disaster", Map.of("dropped", NAIVE_DB + ", " + SAFE_DB));
        result.put("disasterAt", disasterAt.toString());
        result.put("disaster", "DROP DATABASE " + NAIVE_DB + " and " + SAFE_DB + " WITH (FORCE)");
        check(result, "naive database exists", false, db.exists(NAIVE_DB));
        check(result, "safe database exists", false, db.exists(SAFE_DB));
        check(
            result,
            "backups exist",
            true,
            db.exists(NAIVE_DB + BACKUP_SUFFIX) && db.exists(SAFE_DB + BACKUP_SUFFIX));
      }
      case 2 -> {
        long t0 = System.nanoTime();
        db.copy(NAIVE_DB + BACKUP_SUFFIX, NAIVE_DB);
        result.put("restoreMillis", (System.nanoTime() - t0) / 1_000_000);
        var groupOffsets = groupOffsets(naive.group());
        var implied = impliedPositions(NAIVE_DB);
        var run = naive.drain(DRAIN);
        result.set("naiveRun", json.valueToTree(run));
        var gap = new TreeMap<Integer, Map<String, Long>>();
        long gapTotal = 0;
        for (var e : groupOffsets.entrySet()) {
          long inDb = implied.getOrDefault(e.getKey(), 0L);
          gap.put(
              e.getKey(),
              Map.of(
                  "kafkaCommitted",
                  e.getValue(),
                  "restoredDataReaches",
                  inDb,
                  "gap",
                  e.getValue() - inDb));
          gapTotal += e.getValue() - inDb;
        }
        result.set("offsetGap", json.valueToTree(gap));
        var truth = truth();
        var d = diff(truth, NAIVE_DB);
        result.set("comparedWithTopic", json.valueToTree(d));
        var backupAt = Instant.parse(memo.path("backupAt").asString());
        var disasterAt = Instant.parse(memo.path("disasterAt").asString());
        long rpoSec = Duration.between(backupAt, disasterAt).toSeconds();
        rpoSeconds.set(rpoSec);
        rpoEvents.set((long) d.get("missing"));
        var rpo = new LinkedHashMap<String, Object>();
        rpo.put("backupAt", backupAt.toString());
        rpo.put("disasterAt", disasterAt.toString());
        rpo.put("dataLossWindowSeconds", rpoSec);
        rpo.put("eventsInWindow", SECOND_BATCH);
        rpo.put("effectsLostAfterRestore", d.get("missing"));
        rpo.put("amountLost", d.get("missingAmount"));
        result.set("rpo", json.valueToTree(rpo));
        check(result, "naive ledger read after restore", 0, run.read());
        check(
            result, "events missing from the naive ledger", (long) SECOND_BATCH, d.get("missing"));
        check(
            result, "offset gap (Kafka committed − restored data)", (long) SECOND_BATCH, gapTotal);
        compare(
            result,
            "Resumes from",
            "Kafka's committed offset ("
                + sum(groupOffsets)
                + " in total): after the backup's data",
            null);
        compare(
            result,
            "Effects missing after restore",
            d.get("missing") + " events, " + d.get("missingAmount"),
            null);
        compare(
            result,
            "RPO (data-loss window)",
            rpoSec + " s and " + SECOND_BATCH + " events, lost for good",
            null);
      }
      case 3 -> {
        long t0 = System.nanoTime();
        db.copy(SAFE_DB + BACKUP_SUFFIX, SAFE_DB);
        long restoreMs = (System.nanoTime() - t0) / 1_000_000;
        var positions = LedgerConsumer.storedPositions(db, SAFE_DB);
        var run = safe.drain(DRAIN);
        var truth = truth();
        var d = diff(truth, SAFE_DB);
        var verifiedAt = Instant.now();
        long rto =
            Duration.between(Instant.parse(memo.path("disasterAt").asString()), verifiedAt)
                .toMillis();
        rtoMillisSafe.set(rto);
        result.put("restoreMillis", restoreMs);
        result.set("resumedFromPositionsInBackup", json.valueToTree(positions));
        result.set("safeRun", json.valueToTree(run));
        result.set("comparedWithTopic", json.valueToTree(d));
        check(result, "events replayed from the backup's positions", SECOND_BATCH, run.applied());
        check(
            result,
            "safe ledger = topic",
            "0 missing, 0 duplicates",
            d.get("summary"),
            d.get("summary").equals("0 missing, 0 duplicates"));
        var zero = new HashMap<Integer, Long>();
        LedgerConsumer.storedPositions(db, SAFE_DB).keySet().forEach(p -> zero.put(p, 0L));
        LedgerConsumer.rewind(db, SAFE_DB, zero);
        var replay = safe.drain(DRAIN);
        result.set("rewoundToZero", json.valueToTree(replay));
        var after = diff(truth(), SAFE_DB);
        check(
            result,
            "events delivered again after rewinding to 0",
            FIRST_BATCH + SECOND_BATCH,
            replay.read());
        check(result, "applied twice", 0, replay.applied());
        check(
            result,
            "skipped as duplicates",
            FIRST_BATCH + SECOND_BATCH,
            replay.skippedDuplicates());
        check(
            result,
            "safe ledger still = topic",
            "0 missing, 0 duplicates",
            after.get("summary"),
            after.get("summary").equals("0 missing, 0 duplicates"));
        var rtoOut = new LinkedHashMap<String, Object>();
        rtoOut.put("disasterAt", memo.path("disasterAt").asString());
        rtoOut.put("verifiedAt", verifiedAt.toString());
        rtoOut.put("rtoMillis", rto);
        rtoOut.put("restoreMillis", restoreMs);
        rtoOut.put("replayMillis", run.millis());
        rtoOut.put(
            "note",
            "RTO counts from the DROP to a verified ledger, including the time this lab waited between stages");
        result.set("rto", json.valueToTree(rtoOut));
        trace.event("safe ledger recovered", Map.of("rtoMillis", String.valueOf(rto)));
        compare(
            result,
            "Resumes from",
            null,
            "the position stored in the backup itself (" + sum(positions) + " in total)");
        compare(
            result,
            "Effects missing after restore",
            null,
            "0: the " + SECOND_BATCH + " events were replayed from Kafka");
        compare(
            result,
            "RPO (data-loss window)",
            null,
            "0 events: Kafka retained everything since the backup");
        compare(
            result,
            "Replaying too much",
            null,
            (FIRST_BATCH + SECOND_BATCH) + " redelivered, 0 applied twice");
      }
      case 4 -> {
        var backupAt = Instant.parse(memo.path("backupAt").asString());
        // The naive ledger does not know its position, so start well before the backup: the time
        // index is per record timestamp, and replaying events the backup already holds is safe
        // only because each event id is applied once.
        var replayFrom = backupAt.minus(REPLAY_MARGIN);
        var from = offsetsForTime(replayFrom);
        var run = naive.replayFrom(from, DRAIN);
        long rto =
            Duration.between(Instant.parse(memo.path("disasterAt").asString()), Instant.now())
                .toMillis();
        rtoMillisNaive.set(rto);
        result.set(
            "replayFromTimeIndex",
            json.valueToTree(
                Map.of("instant", backupAt.minusSeconds(2).toString(), "offsets", from)));
        result.set("naiveRepair", json.valueToTree(run));
        var truth = truth();
        var naiveDiff = diff(truth, NAIVE_DB);
        var safeDiff = diff(truth, SAFE_DB);
        result.set("naiveComparedWithTopic", json.valueToTree(naiveDiff));
        result.set("safeComparedWithTopic", json.valueToTree(safeDiff));
        result.set("balances", json.valueToTree(balances(truth)));
        result.put("naiveRtoMillis", rto);
        check(result, "naive repair applied the missing events", SECOND_BATCH, run.applied());
        check(result, "events the backup already held, replayed and skipped by id", "at least 1", run.skippedDuplicates(), run.skippedDuplicates() > 0);
        check(
            result,
            "naive ledger = topic",
            "0 missing, 0 duplicates",
            naiveDiff.get("summary"),
            naiveDiff.get("summary").equals("0 missing, 0 duplicates"));
        check(
            result,
            "safe ledger = topic",
            "0 missing, 0 duplicates",
            safeDiff.get("summary"),
            safeDiff.get("summary").equals("0 missing, 0 duplicates"));
        check(
            result,
            "every balance matches the topic in both ledgers",
            true,
            naiveDiff.get("balancesMatch").equals(true)
                && safeDiff.get("balancesMatch").equals(true));
        compare(
            result,
            "Recovery",
            "manual: find the backup's instant in Kafka's time index, replay with a margin and skip known event ids ("
                + run.skippedDuplicates()
                + " skipped), RTO "
                + rto
                + " ms",
            "automatic: restore and start; RTO " + rtoMillisSafe.get() + " ms");
      }
      default -> throw new IllegalArgumentException("No stage " + stage);
    }
    return result;
  }

  // ---- Kafka
  // ---------------------------------------------------------------------------------------

  private Admin admin() {
    var p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.kafkaBootstrap());
    p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    return Admin.create(p);
  }

  private void recreateTopic() throws Exception {
    try (var admin = admin()) {
      try {
        admin.deleteTopics(List.of(LedgerConsumer.TOPIC)).all().get(15, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) throw e;
      }
      for (var group : List.of("lab-dr-naive", "lab-dr-safe"))
        try {
          admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof GroupIdNotFoundException)) throw e;
        }
      long deadline = System.currentTimeMillis() + 30_000;
      while (true) {
        try {
          admin
              .createTopics(
                  List.of(
                      new NewTopic(LedgerConsumer.TOPIC, 3, (short) 1)
                          .configs(Map.of("retention.ms", "-1"))))
              .all()
              .get(15, TimeUnit.SECONDS);
          break;
        } catch (ExecutionException e) {
          // Deletion is asynchronous: the name stays taken for a moment.
          if (System.currentTimeMillis() > deadline) throw e;
          Thread.sleep(500);
        }
      }
    }
  }

  private Map<String, Object> publish(int count) throws Exception {
    var p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.kafkaBootstrap());
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    p.put(ProducerConfig.CLIENT_ID_CONFIG, "lab-dr-payments");
    var random = ThreadLocalRandom.current();
    var total = BigDecimal.ZERO;
    var perPartition = new TreeMap<Integer, long[]>();
    try (var producer = new KafkaProducer<>(p, new StringSerializer(), new StringSerializer())) {
      var futures =
          new ArrayList<
              java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>>();
      for (int i = 0; i < count; i++) {
        var account = ACCOUNTS.get(random.nextInt(ACCOUNTS.size()));
        var amount = BigDecimal.valueOf(random.nextInt(100, 10_000)).movePointLeft(2);
        total = total.add(amount);
        var event =
            json.createObjectNode()
                .put("eventId", UUID.randomUUID().toString())
                .put("type", "PaymentCaptured")
                .put("account", account)
                .put("amount", amount.toPlainString())
                .put("occurredAt", Instant.now().toString());
        futures.add(
            producer.send(new ProducerRecord<>(LedgerConsumer.TOPIC, account, event.toString())));
      }
      for (var f : futures) {
        var m = f.get(15, TimeUnit.SECONDS);
        perPartition.merge(
            m.partition(),
            new long[] {m.offset(), m.offset()},
            (a, b) -> new long[] {Math.min(a[0], b[0]), Math.max(a[1], b[1])});
      }
    }
    var out = new LinkedHashMap<String, Object>();
    out.put("events", count);
    out.put("amount", total);
    var offsets = new TreeMap<Integer, String>();
    perPartition.forEach((k, v) -> offsets.put(k, v[0] + "–" + v[1]));
    out.put("offsetsPerPartition", offsets);
    return out;
  }

  Map<Integer, Long> endOffsets() throws Exception {
    try (var admin = admin()) {
      var partitions =
          admin
              .describeTopics(List.of(LedgerConsumer.TOPIC))
              .allTopicNames()
              .get(10, TimeUnit.SECONDS)
              .get(LedgerConsumer.TOPIC)
              .partitions();
      var request = new HashMap<TopicPartition, OffsetSpec>();
      for (var p : partitions)
        request.put(new TopicPartition(LedgerConsumer.TOPIC, p.partition()), OffsetSpec.latest());
      var out = new TreeMap<Integer, Long>();
      admin
          .listOffsets(request)
          .all()
          .get(10, TimeUnit.SECONDS)
          .forEach((tp, info) -> out.put(tp.partition(), info.offset()));
      return out;
    }
  }

  Map<Integer, Long> groupOffsets(String group) throws Exception {
    try (var admin = admin()) {
      var out = new TreeMap<Integer, Long>();
      admin
          .listConsumerGroupOffsets(group)
          .partitionsToOffsetAndMetadata()
          .get(10, TimeUnit.SECONDS)
          .forEach(
              (tp, om) -> {
                if (om != null && tp.topic().equals(LedgerConsumer.TOPIC))
                  out.put(tp.partition(), om.offset());
              });
      return out;
    }
  }

  private Map<Integer, Long> offsetsForTime(Instant at) throws Exception {
    try (var admin = admin()) {
      var request = new HashMap<TopicPartition, OffsetSpec>();
      for (var p : endOffsets().keySet())
        request.put(
            new TopicPartition(LedgerConsumer.TOPIC, p),
            OffsetSpec.forTimestamp(at.toEpochMilli()));
      var out = new TreeMap<Integer, Long>();
      var ends = endOffsets();
      admin
          .listOffsets(request)
          .all()
          .get(10, TimeUnit.SECONDS)
          // -1: no record at or after the instant in that partition, so nothing to replay there.
          .forEach(
              (tp, info) ->
                  out.put(
                      tp.partition(),
                      info.offset() < 0 ? ends.get(tp.partition()) : info.offset()));
      return out;
    }
  }

  /** Every event on the topic, read from offset 0 by a consumer with no group: the truth. */
  record Truth(Map<String, BigDecimal> amounts, Map<String, String> accounts) {}

  Truth truth() {
    var p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.kafkaBootstrap());
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    var amounts = new LinkedHashMap<String, BigDecimal>();
    var accounts = new LinkedHashMap<String, String>();
    try (var consumer =
        new KafkaConsumer<>(p, new StringDeserializer(), new StringDeserializer())) {
      var partitions =
          consumer.partitionsFor(LedgerConsumer.TOPIC).stream()
              .map(i -> new TopicPartition(LedgerConsumer.TOPIC, i.partition()))
              .toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      var end = consumer.endOffsets(partitions);
      long deadline = System.currentTimeMillis() + 30_000;
      while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
        if (System.currentTimeMillis() > deadline)
          throw new IllegalStateException("Could not read " + LedgerConsumer.TOPIC + " to its end");
        for (var r : consumer.poll(Duration.ofMillis(300))) {
          var e = json.readTree(r.value());
          amounts.put(e.path("eventId").asString(), new BigDecimal(e.path("amount").asString()));
          accounts.put(e.path("eventId").asString(), e.path("account").asString());
        }
      }
    }
    return new Truth(amounts, accounts);
  }

  /** A ledger against the topic: missing, duplicated and unknown event ids, and balances. */
  Map<String, Object> diff(Truth truth, String database) throws SQLException {
    var counts = new HashMap<String, Long>();
    var balances = new TreeMap<String, BigDecimal>();
    try (var c = db.connect(database)) {
      for (var r :
          FailureDb.rows(
              c, "SELECT event_id::text AS id, count(*) AS n FROM ledger_entry GROUP BY event_id"))
        counts.put((String) r.get("id"), ((Number) r.get("n")).longValue());
      for (var r : FailureDb.rows(c, "SELECT id, balance FROM account"))
        balances.put((String) r.get("id"), (BigDecimal) r.get("balance"));
    }
    long missing = 0, duplicates = 0, unknown = 0;
    var missingAmount = BigDecimal.ZERO;
    for (var e : truth.amounts().entrySet()) {
      long n = counts.getOrDefault(e.getKey(), 0L);
      if (n == 0) {
        missing++;
        missingAmount = missingAmount.add(e.getValue());
      } else if (n > 1) duplicates += n - 1;
    }
    for (var id : counts.keySet()) if (!truth.amounts().containsKey(id)) unknown++;
    var expected = expectedBalances(truth);
    boolean balancesMatch = true;
    for (var account : expected.keySet())
      if (balances.getOrDefault(account, BigDecimal.ZERO).compareTo(expected.get(account)) != 0)
        balancesMatch = false;
    var out = new LinkedHashMap<String, Object>();
    out.put("database", database);
    out.put("eventsOnTopic", (long) truth.amounts().size());
    out.put("entriesInLedger", counts.values().stream().mapToLong(Long::longValue).sum());
    out.put("missing", missing);
    out.put("missingAmount", missingAmount.setScale(2, RoundingMode.UNNECESSARY));
    out.put("duplicates", duplicates);
    out.put("unknown", unknown);
    out.put("balancesMatch", balancesMatch);
    out.put("summary", missing + " missing, " + duplicates + " duplicates");
    return out;
  }

  private static TreeMap<String, BigDecimal> expectedBalances(Truth truth) {
    var expected = new TreeMap<String, BigDecimal>();
    truth
        .amounts()
        .forEach((id, amount) -> expected.merge(truth.accounts().get(id), amount, BigDecimal::add));
    return expected;
  }

  private List<Map<String, Object>> balances(Truth truth) throws SQLException {
    var expected = expectedBalances(truth);
    var rows = new ArrayList<Map<String, Object>>();
    var naive = new HashMap<String, BigDecimal>();
    var safe = new HashMap<String, BigDecimal>();
    for (var entry : List.of(Map.entry(NAIVE_DB, naive), Map.entry(SAFE_DB, safe)))
      try (var c = db.connect(entry.getKey())) {
        for (var r : FailureDb.rows(c, "SELECT id, balance FROM account"))
          entry.getValue().put((String) r.get("id"), (BigDecimal) r.get("balance"));
      }
    for (var account : expected.keySet()) {
      var row = new LinkedHashMap<String, Object>();
      row.put("account", account);
      row.put("topic", expected.get(account));
      row.put("naive", naive.get(account));
      row.put("safe", safe.get(account));
      row.put(
          "match",
          expected.get(account).compareTo(naive.getOrDefault(account, BigDecimal.ZERO)) == 0
              && expected.get(account).compareTo(safe.getOrDefault(account, BigDecimal.ZERO)) == 0);
      rows.add(row);
    }
    return rows;
  }

  /** How far a ledger's own data reaches per partition: the highest applied offset + 1. */
  private Map<Integer, Long> impliedPositions(String database) throws SQLException {
    var out = new TreeMap<Integer, Long>();
    try (var c = db.connect(database)) {
      for (var r :
          FailureDb.rows(
              c,
              "SELECT kafka_partition, max(kafka_offset) + 1 AS reaches FROM ledger_entry GROUP BY kafka_partition"))
        out.put(
            ((Number) r.get("kafka_partition")).intValue(),
            ((Number) r.get("reaches")).longValue());
    }
    return out;
  }

  private static long sum(Map<Integer, Long> offsets) {
    return offsets.values().stream().mapToLong(Long::longValue).sum();
  }

  // ---- inspector and reset ----------------------------------------------------------------------

  @Override
  public ObjectNode state() throws Exception {
    var state = json.createObjectNode();
    var databases = state.putArray("databases");
    for (var name : List.of(NAIVE_DB, SAFE_DB, NAIVE_DB + BACKUP_SUFFIX, SAFE_DB + BACKUP_SUFFIX)) {
      boolean exists = db.exists(name);
      databases
          .addObject()
          .put("database", name)
          .put("exists", exists)
          .put("bytes", exists ? db.size(name) : 0);
    }
    try {
      var positions = json.createObjectNode();
      positions.set("topicEndOffsets", json.valueToTree(endOffsets()));
      positions.set("naiveGroupOffsetsInKafka", json.valueToTree(groupOffsets("lab-dr-naive")));
      positions.set(
          "safeGroupOffsetsInKafka (display only)", json.valueToTree(groupOffsets("lab-dr-safe")));
      if (db.exists(SAFE_DB))
        positions.set(
            "safePositionsInDatabase",
            json.valueToTree(LedgerConsumer.storedPositions(db, SAFE_DB)));
      if (db.exists(SAFE_DB + BACKUP_SUFFIX))
        positions.set(
            "safePositionsInBackup",
            json.valueToTree(LedgerConsumer.storedPositions(db, SAFE_DB + BACKUP_SUFFIX)));
      if (db.exists(NAIVE_DB))
        positions.set("naiveDataReaches", json.valueToTree(impliedPositions(NAIVE_DB)));
      state.set("positions", positions);
      var truth = truth();
      var ledgers = state.putArray("ledgers");
      for (var name : List.of(NAIVE_DB, SAFE_DB))
        if (db.exists(name)) ledgers.add(json.valueToTree(diff(truth, name)));
    } catch (Exception e) {
      state.put("kafka", "unavailable: " + e.getMessage());
    }
    state.put("rpoSeconds", rpoSeconds.get());
    state.put("rpoEvents", rpoEvents.get());
    state.put("rtoMillisSafe", rtoMillisSafe.get());
    state.put("rtoMillisNaive", rtoMillisNaive.get());
    return state;
  }

  @Override
  public ObjectNode reset() throws Exception {
    db.requireConfigured();
    for (var name : List.of(NAIVE_DB, SAFE_DB, NAIVE_DB + BACKUP_SUFFIX, SAFE_DB + BACKUP_SUFFIX))
      db.drop(name);
    recreateTopic();
    db.create(NAIVE_DB);
    db.create(SAFE_DB);
    LedgerConsumer.createSchema(db, NAIVE_DB, Design.NAIVE);
    LedgerConsumer.createSchema(db, SAFE_DB, Design.SAFE);
    for (var gauge : List.of(rpoSeconds, rpoEvents, rtoMillisSafe, rtoMillisNaive)) gauge.set(-1);
    return json.createObjectNode()
        .put("topic", LedgerConsumer.TOPIC + " deleted and recreated (3 partitions, kept forever)")
        .put("groups", "lab-dr-naive and lab-dr-safe deleted")
        .put("databases", NAIVE_DB + " and " + SAFE_DB + " recreated empty; backups dropped");
  }
}

package io.zeroshift.eventlab;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * The control plane's own consumer group. A plain poll loop, written out so the mechanics are
 * visible: poll, store each record with its partition and offset, then commit. Stored before
 * committed, so a crash re-reads records rather than losing them; the primary key makes the
 * re-insert harmless. It is at-least-once with an idempotent write, like every service here.
 */
@Component
public class EventTap implements SmartLifecycle {
  public static final String GROUP = "control-plane-tap";
  static final Pattern TOPICS = Pattern.compile("(order|payment|inventory|shipping)\\..+|lab\\..+");
  private static final Logger log = LoggerFactory.getLogger(EventTap.class);
  private static final int RETAINED = 20_000;
  private final EventLabSettings settings;
  private final JdbcTemplate jdbc;
  private final JsonMapper json = JsonMapper.builder().build();
  private volatile boolean running;
  private volatile String status = "not started";
  private volatile Instant lastPoll;
  private Thread thread;

  public EventTap(EventLabSettings settings, JdbcTemplate jdbc) {
    this.settings = settings;
    this.jdbc = jdbc;
  }

  public Map<String, Object> status() {
    var stored =
        jdbc.queryForMap("SELECT COUNT(*) AS records, MAX(tapped_at) AS last FROM event_tap");
    var result = new LinkedHashMap<String, Object>(stored);
    result.put("status", status);
    result.put("lastPoll", lastPoll);
    return result;
  }

  @Override
  public void start() {
    if (!settings.enabled()) {
      status = "disabled";
      return;
    }
    running = true;
    thread = Thread.ofPlatform().name("event-tap").daemon().start(this::loop);
  }

  @Override
  public void stop() {
    running = false;
    if (thread != null) thread.interrupt();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  private void loop() {
    while (running) {
      try (var consumer = consumer()) {
        consumer.subscribe(TOPICS);
        status = "consuming";
        while (running) {
          var records = consumer.poll(Duration.ofSeconds(1));
          lastPoll = Instant.now();
          for (var record : records) store(record);
          if (!records.isEmpty()) consumer.commitSync();
        }
      } catch (org.apache.kafka.common.errors.InterruptException e) {
        return;
      } catch (RuntimeException e) {
        status = "retrying: " + e.getMessage();
        log.warn("Event tap failed, retrying in 5 s: {}", e.toString());
        sleep();
      }
    }
  }

  private KafkaConsumer<String, String> consumer() {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.kafkaBootstrap());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "5000"); // notice new topics quickly
    return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
  }

  private void store(ConsumerRecord<String, String> record) {
    var headers = new LinkedHashMap<String, String>();
    for (var h : record.headers()) headers.put(h.key(), header(h.value()));
    UUID eventId = null;
    String type = headers.get("eventType");
    try {
      var node = json.readTree(record.value());
      if (node.hasNonNull("eventId")) eventId = UUID.fromString(node.get("eventId").asString());
      if (type == null && node.hasNonNull("type")) type = node.get("type").asString();
    } catch (RuntimeException notAnEnvelope) {
      // Poison messages are tapped too: that is how the DLQ panel shows them.
    }
    jdbc.update(
        "INSERT INTO event_tap(topic,kafka_partition,kafka_offset,record_key,event_id,type,headers,value,"
            + "kafka_timestamp) VALUES(?,?,?,?,?,?,?::jsonb,?,?) ON CONFLICT DO NOTHING",
        record.topic(),
        record.partition(),
        record.offset(),
        clean(record.key()),
        eventId,
        type,
        json.writeValueAsString(headers),
        clean(record.value()),
        new Timestamp(record.timestamp()));
    if (record.offset() % 500 == 0)
      jdbc.update(
          "DELETE FROM event_tap WHERE tapped_at < (SELECT MIN(tapped_at) FROM (SELECT tapped_at FROM"
              + " event_tap ORDER BY tapped_at DESC LIMIT ?) newest)",
          RETAINED);
  }

  /**
   * Header values are bytes. Debezium's are text; Spring's retry and dead-letter headers are
   * big-endian numbers (4-byte partitions and attempts, 8-byte offsets and timestamps).
   */
  public static String header(byte[] value) {
    if (value == null) return null;
    var text = new String(value, StandardCharsets.UTF_8);
    if (text.chars().noneMatch(c -> c < 0x20 && c != '\t' && c != '\n' && c != '\r' || c == 0xFFFD))
      return text;
    if (value.length == 4) return Integer.toString(java.nio.ByteBuffer.wrap(value).getInt());
    if (value.length == 8) return Long.toString(java.nio.ByteBuffer.wrap(value).getLong());
    return "0x" + java.util.HexFormat.of().formatHex(value);
  }

  /** PostgreSQL text cannot hold NUL; a record carrying one must not stall the tap. */
  static String clean(String value) {
    return value == null ? null : value.replace('\u0000', '\uFFFD');
  }

  private void sleep() {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running = false;
    }
  }
}

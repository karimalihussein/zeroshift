package io.zeroshift.platform;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;

/** Durable record of every delivery outcome. Joins the caller's transaction when there is one. */
public final class DecisionLog {
  private final JdbcTemplate jdbc;
  private final String instance;
  private final MeterRegistry meters;

  public DecisionLog(JdbcTemplate jdbc, String instance, MeterRegistry meters) {
    this.jdbc = jdbc;
    this.instance = instance;
    this.meters = meters;
  }

  public void record(
      String consumer,
      ConsumerRecord<?, ?> record,
      Envelope envelope,
      Decision decision,
      int attempt,
      String detail) {
    if (envelope == null) envelope = tryDecode(record);
    jdbc.update(
        "INSERT INTO consumer_decision(consumer,event_id,order_id,type,topic,kafka_partition,"
            + "kafka_offset,decision,attempt,detail,trace_id,instance) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        consumer,
        envelope == null ? null : envelope.eventId(),
        envelope == null ? keyOf(record) : envelope.orderId().toString(),
        envelope == null ? null : envelope.type(),
        record.topic(),
        record.partition(),
        record.offset(),
        decision.name(),
        attempt,
        detail == null ? "" : detail.substring(0, Math.min(detail.length(), 1000)),
        Traces.traceId(),
        instance);
    // Counted even if the surrounding transaction later rolls back: the delivery still happened.
    meters
        .counter("zeroshift.consumer.decisions", "consumer", consumer, "decision", decision.name())
        .increment();
  }

  /** Retries and dead letters are logged before (or without) a successful decode. */
  private static Envelope tryDecode(ConsumerRecord<?, ?> record) {
    try {
      return record.value() instanceof String json ? MessageCodec.decode(json) : null;
    } catch (RuntimeException unreadable) {
      return null;
    }
  }

  private static String keyOf(ConsumerRecord<?, ?> record) {
    return record.key() == null ? null : record.key().toString();
  }
}

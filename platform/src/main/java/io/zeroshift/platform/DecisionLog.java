package io.zeroshift.platform;

import static io.zeroshift.platform.db.Tables.CONSUMER_DECISION;

import io.micrometer.core.instrument.MeterRegistry;
import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jooq.DSLContext;

/** Durable record of every delivery outcome. Joins the caller's transaction when there is one. */
public final class DecisionLog {
  private final DSLContext db;
  private final String instance;
  private final MeterRegistry meters;

  public DecisionLog(DSLContext db, String instance, MeterRegistry meters) {
    this.db = db;
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
    var d = CONSUMER_DECISION;
    db.insertInto(d)
        .set(d.CONSUMER, consumer)
        .set(d.EVENT_ID, envelope == null ? null : envelope.eventId())
        .set(d.ORDER_ID, envelope == null ? keyOf(record) : envelope.orderId().toString())
        .set(d.TYPE, envelope == null ? null : envelope.type())
        .set(d.TOPIC, record.topic())
        .set(d.KAFKA_PARTITION, record.partition())
        .set(d.KAFKA_OFFSET, record.offset())
        .set(d.DECISION, decision.name())
        .set(d.ATTEMPT, attempt)
        .set(d.DETAIL, detail == null ? "" : detail.substring(0, Math.min(detail.length(), 1000)))
        .set(d.TRACE_ID, Traces.traceId())
        .set(d.INSTANCE, instance)
        .execute();
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

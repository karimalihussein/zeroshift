package io.zeroshift.platform;

import io.zeroshift.contracts.Envelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;

/** Durable record of every delivery outcome. Joins the caller's transaction when there is one. */
public final class DecisionLog {
  private final JdbcTemplate jdbc;

  public DecisionLog(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public void record(
      String consumer,
      ConsumerRecord<?, ?> record,
      Envelope envelope,
      Decision decision,
      int attempt,
      String detail) {
    jdbc.update(
        "INSERT INTO consumer_decision(consumer,event_id,order_id,type,topic,kafka_partition,"
            + "kafka_offset,decision,attempt,detail,trace_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
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
        Traces.traceId());
  }

  private static String keyOf(ConsumerRecord<?, ?> record) {
    return record.key() == null ? null : record.key().toString();
  }
}

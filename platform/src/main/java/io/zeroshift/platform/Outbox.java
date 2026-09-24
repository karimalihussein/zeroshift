package io.zeroshift.platform;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Records a message for publication. It is published only if the surrounding transaction commits,
 * and then exactly once to the outbox (Debezium may still publish it more than once to Kafka, which
 * consumers absorb through {@link Inbox}).
 */
public final class Outbox {
  private final JdbcTemplate jdbc;

  public Outbox(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Envelope append(Envelope envelope) {
    // Outside a transaction the row could commit without the state change it announces: the
    // dual-write problem the outbox exists to prevent.
    if (!TransactionSynchronizationManager.isActualTransactionActive())
      throw new IllegalStateException("Outbox writes must share the state change's transaction");
    jdbc.update(
        "INSERT INTO outbox(id,topic,aggregate_type,aggregate_id,type,schema_version,correlation_id,"
            + "causation_id,traceparent,payload) VALUES(?,?,'Order',?,?,?,?,?,?,?::jsonb)",
        envelope.eventId(),
        envelope.topic(),
        envelope.orderId().toString(),
        envelope.type(),
        envelope.schemaVersion(),
        envelope.correlationId(),
        envelope.causationId(),
        Traces.traceparent(),
        MessageCodec.encode(envelope));
    return envelope;
  }
}

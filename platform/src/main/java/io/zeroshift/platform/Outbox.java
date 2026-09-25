package io.zeroshift.platform;

import static io.zeroshift.platform.db.Tables.OUTBOX;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Records a message for publication. It is published only if the surrounding transaction commits,
 * and then exactly once to the outbox (Debezium may still publish it more than once to Kafka, which
 * consumers absorb through {@link Inbox}).
 */
public final class Outbox {
  private final DSLContext db;

  public Outbox(DSLContext db) {
    this.db = db;
  }

  public Envelope append(Envelope envelope) {
    // Outside a transaction the row could commit without the state change it announces: the
    // dual-write problem the outbox exists to prevent.
    if (!TransactionSynchronizationManager.isActualTransactionActive())
      throw new IllegalStateException("Outbox writes must share the state change's transaction");
    db.insertInto(OUTBOX)
        .set(OUTBOX.ID, envelope.eventId())
        .set(OUTBOX.TOPIC, envelope.topic())
        .set(OUTBOX.AGGREGATE_TYPE, "Order") // every message in this lab is about one order
        .set(OUTBOX.AGGREGATE_ID, envelope.orderId().toString())
        .set(OUTBOX.TYPE, envelope.type())
        .set(OUTBOX.SCHEMA_VERSION, envelope.schemaVersion())
        .set(OUTBOX.CORRELATION_ID, envelope.correlationId())
        .set(OUTBOX.CAUSATION_ID, envelope.causationId())
        .set(OUTBOX.TRACEPARENT, Traces.traceparent())
        .set(OUTBOX.PAYLOAD, JSONB.valueOf(MessageCodec.encode(envelope)))
        .execute();
    return envelope;
  }
}

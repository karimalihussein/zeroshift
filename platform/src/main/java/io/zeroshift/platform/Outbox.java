package io.zeroshift.platform;

import static io.zeroshift.platform.db.Tables.OUTBOX;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.max;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import tools.jackson.databind.JsonNode;

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

  /** One outbox row; {@code payload} is the full envelope as JSON. */
  public record Row(
      UUID id,
      String topic,
      String aggregateId,
      String type,
      int schemaVersion,
      UUID correlationId,
      UUID causationId,
      String traceparent,
      JsonNode payload,
      OffsetDateTime createdAt) {}

  /**
   * The service's side of change data capture. {@code lagBytes} is WAL the connector has not yet
   * confirmed: outbox inserts committed here but not yet on Kafka (plus unrelated WAL traffic).
   */
  public record Status(
      String slot, boolean slotActive, Long lagBytes, long rows, OffsetDateTime lastInsertAt) {}

  /** Newest first, at most {@code limit + 1} (the extra one tells a page there is more). */
  public List<Row> recent(String aggregateId, int limit) {
    return db.selectFrom(OUTBOX)
        .where(aggregateId == null ? DSL.noCondition() : OUTBOX.AGGREGATE_ID.eq(aggregateId))
        .orderBy(OUTBOX.CREATED_AT.desc())
        .limit(limit + 1)
        .fetch(
            r ->
                new Row(
                    r.getId(),
                    r.getTopic(),
                    r.getAggregateId(),
                    r.getType(),
                    r.getSchemaVersion(),
                    r.getCorrelationId(),
                    r.getCausationId(),
                    r.getTraceparent(),
                    MessageCodec.json().readTree(r.getPayload().data()),
                    r.getCreatedAt()));
  }

  public Status status() {
    // A system view, not our schema: plain SQL.
    var slot =
        db.fetchOptional(
            "SELECT slot_name, active,"
                + " pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS lag"
                + " FROM pg_replication_slots WHERE slot_name = current_database() || '_outbox'");
    var table = db.select(count(), max(OUTBOX.CREATED_AT)).from(OUTBOX).fetchSingle();
    return new Status(
        slot.map(r -> r.get("slot_name", String.class)).orElse(null),
        slot.map(r -> r.get("active", Boolean.class)).orElse(false),
        slot.map(r -> r.get("lag", Long.class)).orElse(null),
        table.value1(),
        table.value2());
  }
}

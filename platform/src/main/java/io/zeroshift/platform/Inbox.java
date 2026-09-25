package io.zeroshift.platform;

import static io.zeroshift.platform.db.Tables.PROCESSED_MESSAGE;

import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.MessageCodec;
import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The idempotent-consumer gate. For each delivery, in one database transaction: claim the event id
 * for this consumer, run the handler (state change + outbox messages), record the decision. A
 * redelivery finds the claim and changes nothing. The Kafka offset is committed only after that
 * transaction, so a crash in between redelivers the record: at-least-once delivery, exactly-once
 * effect.
 */
public final class Inbox {
  private static final Logger log = LoggerFactory.getLogger(Inbox.class);
  private final DSLContext db;
  private final TransactionTemplate transactions;
  private final DecisionLog decisions;
  private final Faults faults;

  public Inbox(
      DSLContext db, TransactionTemplate transactions, DecisionLog decisions, Faults faults) {
    this.db = db;
    this.transactions = transactions;
    this.decisions = decisions;
    this.faults = faults;
  }

  public Handled deliver(
      String consumer, ConsumerRecord<String, String> record, Function<Envelope, Handled> handler) {
    // Unreadable records throw MalformedMessageException: never retried, dead-lettered at once.
    var envelope = MessageCodec.decode(record.value());
    int attempt = attempt(record);
    if (faults.trigger(Faults.TRANSIENT_ERROR).isPresent())
      throw new TransientFailure("Injected transient failure (" + envelope.type() + ")");
    var handled =
        transactions.execute(
            s -> {
              // The claim. A concurrent delivery of the same event (a rebalance mid-handling)
              // blocks
              // here on the primary key until this transaction ends, then finds the row.
              boolean first =
                  db.insertInto(PROCESSED_MESSAGE)
                          .set(PROCESSED_MESSAGE.CONSUMER, consumer)
                          .set(PROCESSED_MESSAGE.EVENT_ID, envelope.eventId())
                          .set(PROCESSED_MESSAGE.TOPIC, record.topic())
                          .set(PROCESSED_MESSAGE.KAFKA_PARTITION, record.partition())
                          .set(PROCESSED_MESSAGE.KAFKA_OFFSET, record.offset())
                          .onConflictDoNothing()
                          .execute()
                      == 1;
              var result =
                  first
                      ? handler.apply(envelope)
                      : new Handled(
                          Decision.DUPLICATE_SKIPPED,
                          envelope.type() + " " + envelope.eventId() + " was already processed");
              decisions.record(
                  consumer, record, envelope, result.decision(), attempt, result.detail());
              return result;
            });
    log.info(
        "{} {} {}-{}@{} order={} event={}: {}",
        consumer,
        handled.decision(),
        record.topic(),
        record.partition(),
        record.offset(),
        envelope.orderId(),
        envelope.eventId(),
        handled.detail());
    if (faults.trigger(Faults.CRASH_AFTER_COMMIT).isPresent())
      Crash.now(
          "committed "
              + envelope.type()
              + " at offset "
              + record.offset()
              + " but crashing before the offset commit");
    return handled;
  }

  /**
   * Drops every idempotency record of {@code consumer}, so a replay from offset 0 is applied again
   * instead of skipped. Only for a consumer whose effects were emptied in the same transaction (a
   * read-model rebuild): anything else would apply its events twice.
   */
  public void forget(String consumer) {
    db.deleteFrom(PROCESSED_MESSAGE).where(PROCESSED_MESSAGE.CONSUMER.eq(consumer)).execute();
  }

  /** Spring Kafka's delivery counter for this record (1 on first delivery). */
  static int attempt(ConsumerRecord<?, ?> record) {
    var header = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
    return header == null || header.value().length != 4
        ? 1
        : ByteBuffer.wrap(header.value()).getInt();
  }

  /** A failure worth retrying: the same record may succeed on a later delivery. */
  public static final class TransientFailure extends RuntimeException {
    public TransientFailure(String message) {
      super(message);
    }
  }
}

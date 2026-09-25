package io.zeroshift.shipping;

import static io.zeroshift.shipping.db.Tables.LAB_SETTING;
import static io.zeroshift.shipping.db.Tables.TRACKING;
import static io.zeroshift.shipping.db.Tables.TRACKING_SCAN;

import io.zeroshift.contracts.CarrierEvent.ParcelScanned;
import io.zeroshift.contracts.Envelope;
import io.zeroshift.platform.Decision;
import io.zeroshift.platform.Handled;
import io.zeroshift.platform.PostgresClock;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jooq.DSLContext;

/**
 * Parcel tracking, projected from the carrier's scans. The naive projection is last-write-wins:
 * whatever scan arrives last sets the status, which is only right if scans arrive in order. With
 * the sequence guard on, a scan older than the one already applied is refused: the projection is
 * then correct whatever order Kafka delivers in, at the cost of trusting the carrier's sequence.
 */
public final class Tracking {
  public static final String CONSUMER = "carrier-tracking";

  /** lab_setting name: "on" makes the projection refuse scans older than the last applied. */
  public static final String GUARD = "sequence-guard";

  private final DSLContext db;

  public Tracking(DSLContext db) {
    this.db = db;
  }

  /** Runs inside the inbox's transaction, with the scan's idempotency record. */
  public Handled apply(Envelope envelope, ConsumerRecord<?, ?> record) {
    if (!(envelope.payload() instanceof ParcelScanned scan))
      return Handled.ignored("Not a carrier scan: " + envelope.type());
    var t = TRACKING;
    // Row lock: scans of one parcel handled at once (different partitions) take turns here. That
    // stops lost updates, not reordering: whichever commits last still wins without the guard.
    var current =
        db.selectFrom(t)
            .where(t.TRACKING_NUMBER.eq(scan.trackingNumber()))
            .forUpdate()
            .fetchOptional();
    String outcome;
    String detail;
    if (current.isEmpty()) {
      db.insertInto(t)
          .set(t.TRACKING_NUMBER, scan.trackingNumber())
          .set(t.ORDER_ID, scan.orderId())
          .set(t.STATUS, scan.status())
          .set(t.LAST_SEQ, scan.seq())
          .set(t.SCANS_APPLIED, 1)
          .execute();
      outcome = "APPLIED";
      detail =
          scan.trackingNumber() + " → " + scan.status() + " (scan " + scan.seq() + ", first seen)";
    } else if (scan.seq() > current.get().getLastSeq()) {
      db.update(t)
          .set(t.STATUS, scan.status())
          .set(t.LAST_SEQ, scan.seq())
          .set(t.SCANS_APPLIED, t.SCANS_APPLIED.plus(1))
          .set(t.UPDATED_AT, PostgresClock.NOW)
          .where(t.TRACKING_NUMBER.eq(scan.trackingNumber()))
          .execute();
      outcome = "APPLIED";
      detail = scan.trackingNumber() + " → " + scan.status() + " (scan " + scan.seq() + ")";
    } else if (guarded()) {
      db.update(t)
          .set(t.STALE_SKIPPED, t.STALE_SKIPPED.plus(1))
          .where(t.TRACKING_NUMBER.eq(scan.trackingNumber()))
          .execute();
      outcome = "STALE_SKIPPED";
      detail =
          "Scan "
              + scan.seq()
              + " ("
              + scan.status()
              + ") is older than applied scan "
              + current.get().getLastSeq()
              + ": refused by the sequence guard";
    } else {
      db.update(t)
          .set(t.STATUS, scan.status())
          .set(t.LAST_SEQ, scan.seq())
          .set(t.SCANS_APPLIED, t.SCANS_APPLIED.plus(1))
          .set(t.REGRESSIONS, t.REGRESSIONS.plus(1))
          .set(t.UPDATED_AT, PostgresClock.NOW)
          .where(t.TRACKING_NUMBER.eq(scan.trackingNumber()))
          .execute();
      outcome = "REGRESSED";
      detail =
          "Out of order: scan "
              + scan.seq()
              + " arrived after scan "
              + current.get().getLastSeq()
              + " and moved "
              + scan.trackingNumber()
              + " back from "
              + current.get().getStatus()
              + " to "
              + scan.status();
    }
    db.insertInto(TRACKING_SCAN)
        .set(TRACKING_SCAN.TRACKING_NUMBER, scan.trackingNumber())
        .set(TRACKING_SCAN.SEQ, scan.seq())
        .set(TRACKING_SCAN.STATUS, scan.status())
        .set(TRACKING_SCAN.RECORD_KEY, record.key() == null ? null : record.key().toString())
        .set(TRACKING_SCAN.KAFKA_PARTITION, record.partition())
        .set(TRACKING_SCAN.KAFKA_OFFSET, record.offset())
        .set(TRACKING_SCAN.OUTCOME, outcome)
        .execute();
    return new Handled(
        outcome.equals("STALE_SKIPPED") ? Decision.IGNORED : Decision.PROCESSED, detail);
  }

  public boolean guarded() {
    return "on"
        .equals(
            db.select(LAB_SETTING.VALUE)
                .from(LAB_SETTING)
                .where(LAB_SETTING.NAME.eq(GUARD))
                .fetchOne(LAB_SETTING.VALUE));
  }

  public void guard(boolean on) {
    db.insertInto(LAB_SETTING)
        .set(LAB_SETTING.NAME, GUARD)
        .set(LAB_SETTING.VALUE, on ? "on" : "off")
        .onConflict(LAB_SETTING.NAME)
        .doUpdate()
        .set(LAB_SETTING.VALUE, on ? "on" : "off")
        .execute();
  }

  /**
   * Forgets the given parcels' tracking (a fresh trip), or everything when {@code parcels} is null.
   */
  public void clear(List<String> parcels) {
    if (parcels == null) {
      db.truncate(TRACKING_SCAN).execute();
      db.truncate(TRACKING).execute();
      return;
    }
    db.deleteFrom(TRACKING_SCAN).where(TRACKING_SCAN.TRACKING_NUMBER.in(parcels)).execute();
    db.deleteFrom(TRACKING).where(TRACKING.TRACKING_NUMBER.in(parcels)).execute();
  }

  /** For the control plane: parcels newest first, and the scans as they were handled. */
  public Map<String, Object> view(int parcels, int scans) {
    return Map.of(
        "guard", guarded(),
        "parcels",
            db.selectFrom(TRACKING).orderBy(TRACKING.UPDATED_AT.desc()).limit(parcels).fetchMaps(),
        "scans",
            db.selectFrom(TRACKING_SCAN).orderBy(TRACKING_SCAN.ID.desc()).limit(scans).fetchMaps());
  }
}

package io.zeroshift.shipping;

import static io.zeroshift.shipping.db.Tables.SHIPMENT;

import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;

/**
 * The shipment table: one row per order, written once, so a repeated command gets the same answer.
 */
public final class Shipments {
  public record Shipment(boolean scheduled, String trackingNumber, String reason) {}

  private final DSLContext db;

  public Shipments(DSLContext db) {
    this.db = db;
  }

  public Optional<Shipment> find(UUID orderId) {
    return db.select(SHIPMENT.STATUS, SHIPMENT.TRACKING_NUMBER, SHIPMENT.REASON)
        .from(SHIPMENT)
        .where(SHIPMENT.ORDER_ID.eq(orderId))
        .fetchOptional(r -> new Shipment("SCHEDULED".equals(r.value1()), r.value2(), r.value3()));
  }

  public void scheduled(UUID orderId, String trackingNumber, String carrier) {
    db.insertInto(SHIPMENT)
        .set(SHIPMENT.ORDER_ID, orderId)
        .set(SHIPMENT.STATUS, "SCHEDULED")
        .set(SHIPMENT.TRACKING_NUMBER, trackingNumber)
        .set(SHIPMENT.CARRIER, carrier)
        .execute();
  }

  public void failed(UUID orderId, String reason) {
    db.insertInto(SHIPMENT)
        .set(SHIPMENT.ORDER_ID, orderId)
        .set(SHIPMENT.STATUS, "FAILED")
        .set(SHIPMENT.REASON, reason)
        .execute();
  }
}

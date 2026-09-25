package io.zeroshift.contracts;

import java.util.UUID;

/**
 * What the external carrier reports about a shipped parcel. {@code seq} is the carrier's own scan
 * sequence for that parcel (1 picked up, 2 in transit, 3 out for delivery, 4 delivered): the only
 * thing that says which scan is newer once they arrive out of order.
 */
public sealed interface CarrierEvent extends Message {
  record ParcelScanned(UUID orderId, String trackingNumber, int seq, String status, String hub)
      implements CarrierEvent {}
}

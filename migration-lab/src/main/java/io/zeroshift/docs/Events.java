package io.zeroshift.docs;

import io.zeroshift.contracts.Contracts;
import io.zeroshift.contracts.Envelope;
import io.zeroshift.contracts.OrderLine;
import io.zeroshift.docs.Model.EventDoc;
import io.zeroshift.docs.Model.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Event pages built from {@link Contracts} and the payload record components. */
final class Events {
  private Events() {}

  static List<EventDoc> all(List<String> headers) {
    var docs = new ArrayList<EventDoc>();
    for (var contract : Contracts.all()) {
      var type = contract.javaType();
      var fields = fields(type);
      var payload = example(type, 2);
      docs.add(
          new EventDoc(
              contract.type(),
              type.getEnclosingClass().getSimpleName(),
              contract.topic(),
              contract.version(),
              type.getName(),
              fields,
              payload,
              envelope(contract.type(), contract.version(), payload),
              producers(contract.type()),
              consumers(contract.topic()),
              contract.type().equals("ParcelScanned")
                  ? "trackingNumber, eventId, or hub-AMS, chosen by the carrier lab keying"
                  : "orderId",
              ordering(contract.topic()),
              "Consumers insert the eventId into processed_message in the same transaction as the work. A redelivery of the same eventId is an already-handled duplicate.",
              "The listener error handler retries four times (500 ms, 1 s, 2 s, 4 s). MalformedMessageException is not retried.",
              contract.topic() + ".dlt",
              upcasters(contract),
              headers));
    }
    return List.copyOf(docs);
  }

  static List<String> headersFromConnector(String json) {
    var key = "\"transforms.outbox.table.fields.additional.placement\"";
    var start = json.indexOf(key);
    if (start < 0) throw new IllegalArgumentException("Outbox connector has no header placement");
    var colon = json.indexOf(':', start);
    var quote = json.indexOf('"', colon + 1);
    var end = json.indexOf('"', quote + 1);
    var placement = json.substring(quote + 1, end);
    var headers = new ArrayList<String>();
    for (var part : placement.split(",")) {
      var bits = part.trim().split(":");
      headers.add(bits[bits.length - 1]);
    }
    return List.copyOf(headers);
  }

  private static List<String> upcasters(Contracts.Contract contract) {
    if (contract.upcasters().isEmpty()) return List.of();
    if (!contract.type().equals("OrderPlaced"))
      throw new IllegalStateException(
          "Document the upcaster for " + contract.type() + " in Events.upcasters");
    return List.of(
        "v1 → v2: a missing currency is set to USD. v2 payloads already contain currency. Readers ignore fields they do not know; renaming a field needs a new upcaster here.",
        "The commerce fields (customerName, subtotal, discount, taxRate, tax, voucherCode, invoiceNumber; on each line productId, name, subtotal, discount, total) were added inside v2, which is backward compatible. A payload without them decodes with neutral values: subtotal = total, discount and tax 0.00, tax rate 0, the customer id as the name, and per line subtotal = total = unitPrice × quantity and name = sku.",
        "Writing as v1 (the history lab's previous deployment) removes currency and every commerce field, and refuses a currency other than USD.");
  }

  private static List<String> producers(String type) {
    return switch (type) {
      case "OrderPlaced",
          "OrderPaymentAuthorized",
          "OrderStockReserved",
          "OrderShipped",
          "OrderCancelled",
          "AuthorizePayment",
          "RefundPayment",
          "ReserveStock",
          "ReleaseStock",
          "ScheduleShipment" ->
          List.of("order-service");
      case "PaymentAuthorized", "PaymentDeclined", "PaymentRefunded" -> List.of("payment-service");
      case "StockReserved", "StockRejected", "StockReleased" -> List.of("inventory-service");
      case "ShipmentScheduled", "ShipmentFailed" -> List.of("shipping-service");
      case "ParcelScanned" -> List.of("shipping-service (carrier lab, direct to Kafka)");
      default -> throw new IllegalStateException("No producer recorded for " + type);
    };
  }

  static List<String> consumers(String topic) {
    var groups = new ArrayList<String>();
    switch (topic) {
      case "order.events" -> groups.add("order-projection");
      case "payment.commands" -> groups.add("payment-service");
      case "inventory.commands" -> groups.add("inventory-service");
      case "shipping.commands" -> groups.add("shipping-service");
      case "payment.events", "inventory.events", "shipping.events" -> groups.add("order-saga");
      case "shipping.carrier-scans" -> groups.add("carrier-tracking");
      default -> {}
    }
    if (topic.startsWith("order.")
        || topic.startsWith("payment.")
        || topic.startsWith("inventory.")
        || topic.startsWith("shipping.")
        || topic.startsWith("lab.")) groups.add("control-plane-tap");
    return List.copyOf(groups);
  }

  private static String ordering(String topic) {
    if (topic.equals("shipping.carrier-scans"))
      return "Order is per partition, and the partition follows the message key. The carrier lab can key by tracking number (one parcel, one partition), by event id (scans of one parcel spread out), or by hub (one hot partition). The tracking projection's sequence guard decides which scan is newer.";
    return "Kafka orders records inside one partition. The key is the order id, so one order's messages stay in one partition of a topic. Messages on different topics are not ordered against each other; the saga and the event stream use optimistic versions when two replies arrive together.";
  }

  private static List<Field> fields(Class<?> type) {
    var fields = new ArrayList<Field>();
    for (var component : type.getRecordComponents()) {
      fields.add(
          new Field(
              component.getName(),
              typeName(component.getGenericType()),
              true,
              component.getName().equals("orderId") ? "Aggregate and partition key." : ""));
    }
    return List.copyOf(fields);
  }

  private static String typeName(Type type) {
    if (type instanceof ParameterizedType parameterized) {
      var raw = ((Class<?>) parameterized.getRawType()).getSimpleName();
      var arg = typeName(parameterized.getActualTypeArguments()[0]);
      return raw + "<" + arg + ">";
    }
    if (type instanceof Class<?> clazz) return clazz.getSimpleName();
    return type.getTypeName();
  }

  private static String envelope(String type, int version, String payload) {
    var indented = payload.replace("\n", "\n  ");
    return """
        {
          "eventId": "3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44",
          "type": "%s",
          "schemaVersion": %d,
          "correlationId": "8a2e1c44-1b6f-4d0a-9e77-2c5b8f0d11aa",
          "causationId": null,
          "occurredAt": "2026-09-26T00:00:00Z",
          "payload": %s
        }
        """
        .formatted(type, version, indented);
  }

  private static String example(Class<?> type, int indent) {
    if (!type.isRecord()) return sample(type, type);
    var pad = " ".repeat(indent);
    var lines = new ArrayList<String>();
    for (var component : type.getRecordComponents())
      lines.add(pad + "\"" + component.getName() + "\": " + value(type, component));
    return "{\n" + String.join(",\n", lines) + "\n" + " ".repeat(indent - 2) + "}";
  }

  /**
   * One consistent order in the examples: 2 × SKU-CABLE at 12.99 with WELCOME10 (10%) and 8% tax.
   * Amounts are scale 2 and total = subtotal − discount + tax, as Money and OrderPlaced require.
   */
  private static final Map<String, String> ORDER =
      Map.of(
          "subtotal", "25.98",
          "discount", "2.60",
          "taxRate", "0.0800",
          "tax", "1.87",
          "total", "25.25",
          "amount", "25.25");

  private static final Map<String, String> LINE =
      Map.of(
          "unitPrice", "12.99",
          "subtotal", "25.98",
          "discount", "2.60",
          "total", "23.38");

  private static final Map<String, String> KNOWN =
      Map.of(
          "customerId", "\"5b0e6f4c-2d1a-4c3e-9f7b-1a2b3c4d5e6f\"",
          "customerName", "\"Amara Okafor\"",
          "currency", "\"USD\"",
          "sku", "\"SKU-CABLE\"",
          "name", "\"USB-C cable, 2 m\"",
          "voucherCode", "\"WELCOME10\"",
          "invoiceNumber", "\"INV-2026-000042\"",
          "idempotencyKey", "\"order:3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44:authorize\"");

  private static String value(Class<?> owner, RecordComponent component) {
    var name = component.getName();
    var raw = component.getType();
    if (raw == BigDecimal.class) {
      var amount = (owner == OrderLine.class ? LINE : ORDER).get(name);
      if (amount != null) return amount;
    }
    if (raw == String.class && KNOWN.containsKey(name)) return KNOWN.get(name);
    if (raw == int.class && name.equals("quantity")) return "2";
    return sample(component.getGenericType(), raw);
  }

  private static String sample(Type generic, Class<?> raw) {
    if (generic instanceof ParameterizedType parameterized
        && parameterized.getRawType() == List.class) {
      var arg = parameterized.getActualTypeArguments()[0];
      if (arg instanceof Class<?> clazz && clazz.isRecord())
        return "[\n" + example(clazz, 6) + "\n    ]";
      return "[" + sample(arg, arg instanceof Class<?> c ? c : Object.class) + "]";
    }
    if (raw == String.class) return "\"example\"";
    if (raw == UUID.class) return "\"3f1c0b2e-7a4d-4e1a-9c3b-6d8e2f0a1b44\"";
    if (raw == int.class || raw == Integer.class || raw == long.class || raw == Long.class)
      return "1";
    if (raw == BigDecimal.class) return "25.25";
    if (raw == boolean.class || raw == Boolean.class) return "true";
    if (raw == Instant.class) return "\"2026-09-26T00:00:00Z\"";
    if (raw.isRecord()) return example(raw, 4);
    return "null";
  }

  static String envelopeType() {
    return Envelope.class.getSimpleName();
  }
}

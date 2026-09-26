package io.zeroshift.history;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.contracts.MessageCodec;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.node.ObjectNode;

class SchemaReadersTest {
  static final String V2 =
      "{\"eventId\":\"11111111-1111-1111-1111-111111111111\",\"type\":\"OrderPlaced\",\"schemaVersion\":2,"
          + "\"correlationId\":\"22222222-2222-2222-2222-222222222222\",\"causationId\":null,"
          + "\"occurredAt\":\"2026-01-10T09:00:00Z\",\"payload\":{\"orderId\":\"00000000-0000-0000-0000-00000000000a\","
          + "\"customerId\":\"c\",\"lines\":[{\"sku\":\"SKU-1\",\"quantity\":2,\"unitPrice\":10.50}],\"total\":21.00,\"currency\":\"USD\"}}";
  static final String V1 =
      V2.replace("\"schemaVersion\":2", "\"schemaVersion\":1").replace(",\"currency\":\"USD\"", "");

  @Test
  void theMatrixOfReadersAndWriters() {
    var v3 = SchemaReaders.asV3((ObjectNode) MessageCodec.json().readTree(V2)).toString();
    assertThat(read(V1))
        .containsEntry("v1 reader (previous release)", true)
        .containsEntry("v2 reader (deployed)", true)
        .containsEntry("v3 reader (proposed)", true);
    assertThat(read(V2))
        .containsEntry("v1 reader (previous release)", false)
        .containsEntry("v2 reader (deployed)", true)
        .containsEntry("v3 reader (proposed)", true);
    assertThat(read(v3))
        .containsEntry("v1 reader (previous release)", false)
        .containsEntry("v2 reader (deployed)", false)
        .containsEntry("v2 without the version check", false)
        .containsEntry("v3 reader (proposed)", true);
  }

  @Test
  void theDeployedReaderSaysWhyAndTheTolerantOneMisleads() {
    var v3 = SchemaReaders.asV3((ObjectNode) MessageCodec.json().readTree(V2)).toString();
    var readings = SchemaReaders.readAll(v3);
    assertThat(readings.get(1).outcome()).contains("v3 is newer than this consumer understands");
    assertThat(readings.get(2).outcome()).contains("unitPrice must not be negative");
  }

  @Test
  void v3CarriesExactIntegerCents() {
    var v3 = SchemaReaders.asV3((ObjectNode) MessageCodec.json().readTree(V2));
    assertThat(v3.path("schemaVersion").asInt()).isEqualTo(3);
    assertThat(v3.path("payload").path("totalMinor").asLong()).isEqualTo(2100);
    assertThat(v3.path("payload").path("lines").get(0).path("unitPriceMinor").asLong())
        .isEqualTo(1050);
    assertThat(v3.path("payload").has("total")).isFalse();
  }

  @Test
  void findsAKeysPartitionAsTheProducerDoes() {
    for (var key : new String[] {"a", "order-1", "00000000-0000-0000-0000-00000000000a"})
      assertThat(KafkaHistory.partitionFor(key, 3))
          .isEqualTo(BuiltInPartitioner.partitionForKey(key.getBytes(), 3));
  }

  private static Map<String, Boolean> read(String record) {
    return SchemaReaders.readAll(record).stream()
        .collect(Collectors.toMap(SchemaReaders.Reading::reader, SchemaReaders.Reading::read));
  }
}

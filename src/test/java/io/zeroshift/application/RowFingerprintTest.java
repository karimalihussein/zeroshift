package io.zeroshift.application;

import static org.assertj.core.api.Assertions.*;

import io.zeroshift.domain.*;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class RowFingerprintTest {
  private ValidationResult.Fingerprint hash(Row row) {
    var hash = new RowFingerprint();
    hash.add(row);
    return hash.finish();
  }

  @Test
  void normalizesDecimalScaleWithoutLosingPrecision() {
    assertThat(hash(new Row.Order(1, 2, new BigDecimal("12.3400"), "NEW")))
        .isEqualTo(hash(new Row.Order(1, 2, new BigDecimal("12.34"), "NEW")));
    assertThat(hash(new Row.Order(1, 2, new BigDecimal("12.3401"), "NEW")))
        .isNotEqualTo(hash(new Row.Order(1, 2, new BigDecimal("12.34"), "NEW")));
  }

  @Test
  void distinguishesNullEmptyDelimitersAndUnicode() {
    assertThat(hash(new Row.Customer(1, "عميل\n,\"", null, true)))
        .isNotEqualTo(hash(new Row.Customer(1, "عميل\n,\"", "", true)));
    assertThat(hash(new Row.Customer(1, "a|b", "c", true)))
        .isNotEqualTo(hash(new Row.Customer(1, "a", "b|c", true)));
  }

  @Test
  void includesEveryColumnAndOrder() {
    var a = new RowFingerprint();
    a.add(new Row.Customer(1, "a", null, true));
    a.add(new Row.Customer(2, "b", null, true));
    var b = new RowFingerprint();
    b.add(new Row.Customer(2, "b", null, true));
    b.add(new Row.Customer(1, "a", null, true));
    assertThat(a.finish()).isNotEqualTo(b.finish());
  }
}

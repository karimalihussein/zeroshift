package io.zeroshift.application;

import io.zeroshift.domain.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.HexFormat;

/** Length-prefixed UTF-8 distinguishes null, empty, embedded delimiters and Unicode. */
public final class RowFingerprint {
  private final MessageDigest digest;
  private long count;

  public RowFingerprint() {
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("JDK requires SHA-256", e);
    }
  }

  public void add(Row row) {
    field(Long.toString(row.id()));
    switch (row) {
      case Row.Customer c -> {
        field(c.name());
        field(c.email());
        field(Boolean.toString(c.active()));
      }
      case Row.Order o -> {
        field(Long.toString(o.customerId()));
        field(o.amount().stripTrailingZeros().toPlainString());
        field(o.status());
      }
    }
    count++;
  }

  private void field(String value) {
    if (value == null) {
      digest.update(ByteBuffer.allocate(4).putInt(-1).array());
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
    digest.update(bytes);
  }

  public ValidationResult.Fingerprint finish() {
    return new ValidationResult.Fingerprint(count, HexFormat.of().formatHex(digest.digest()));
  }
}

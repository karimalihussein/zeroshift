package io.zeroshift.domain;

import java.util.List;

public record ValidationResult(boolean matches, List<TableResult> tables) {
  public record Fingerprint(long count, String sha256) {}

  public record TableResult(Table table, Fingerprint source, Fingerprint target) {
    public boolean matches() {
      return source.equals(target);
    }
  }

  public static ValidationResult of(List<TableResult> tables) {
    return new ValidationResult(
        tables.stream().allMatch(TableResult::matches), List.copyOf(tables));
  }
}

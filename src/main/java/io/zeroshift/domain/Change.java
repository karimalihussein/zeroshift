package io.zeroshift.domain;

public record Change(long id, Operation operation, Row row, long version) {
  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  public Change {
    if (operation != Operation.DELETE && row == null)
      throw new IllegalArgumentException("Upsert needs a row");
    if (row != null && row.id() != id)
      throw new IllegalArgumentException("Change key does not match row");
  }
}

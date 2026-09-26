package io.zeroshift.racelab.domain;

/** How an experiment's requests protect (or fail to protect) the data they share. */
public enum Mode {
  UNSAFE("Unsafe", "Read, decide in the application, write back. Nothing stops a second request."),
  ATOMIC(
      "Atomic update",
      "One conditional UPDATE: the database re-checks the condition on the row it locks."),
  PESSIMISTIC(
      "Pessimistic lock",
      "SELECT … FOR UPDATE: the first request locks the row, the others wait for it."),
  OPTIMISTIC(
      "Optimistic lock",
      "Read a version, write only if it is unchanged; a stale writer is rejected and may retry."),
  SERIALIZABLE(
      "Serializable",
      "The unsafe code under SERIALIZABLE isolation: PostgreSQL aborts a conflicting transaction."),
  ORDERED_LOCKS(
      "Consistent lock order",
      "Every transaction locks rows in the same order, so no wait-for cycle can form.");

  private final String label;
  private final String summary;

  Mode(String label, String summary) {
    this.label = label;
    this.summary = summary;
  }

  public String label() {
    return label;
  }

  public String summary() {
    return summary;
  }
}

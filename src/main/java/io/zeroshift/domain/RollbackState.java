package io.zeroshift.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * Durable reverse-sync progress. {@code captureSince} is set in the cutover transaction; without
 * it, post-cutover writes cannot be proven complete and rollback is refused.
 */
public record RollbackState(
    Instant captureSince,
    long baselineVersion,
    Instant startedAt,
    Instant freezeAt,
    Instant completedAt,
    long applied,
    long conflicts,
    String validation,
    boolean validationPassed,
    long verifiedRows) {
  public boolean captureActive() {
    return captureSince != null;
  }

  public Long durationMillis() {
    return startedAt == null || completedAt == null
        ? null
        : Math.max(0, Duration.between(startedAt, completedAt).toMillis());
  }

  public Long freezeMillis() {
    return freezeAt == null || completedAt == null
        ? null
        : Math.max(0, Duration.between(freezeAt, completedAt).toMillis());
  }
}

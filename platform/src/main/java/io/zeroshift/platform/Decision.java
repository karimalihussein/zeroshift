package io.zeroshift.platform;

/** What a consumer did with one delivery of one record. */
public enum Decision {
  /** First delivery: effects committed with the idempotency record. */
  PROCESSED,
  /** Already processed (redelivery, replay or duplicate publish): no effect. */
  DUPLICATE_SKIPPED,
  /** Valid and new, but no longer relevant (e.g. a reply to a saga that has moved on). */
  IGNORED,
  /** Failed; the record will be delivered again after a backoff. */
  RETRY_SCHEDULED,
  /** Retries exhausted or unprocessable: parked on the dead-letter topic. */
  DEAD_LETTERED
}

package io.zeroshift.platform.web;

/**
 * Header names that more than one module reads or writes. Only {@link #REQUEST_ID} is handled
 * generically (by {@link RequestIdFilter}); the others belong to specific operations and stay with
 * them: they are named here so the service and its clients cannot drift apart.
 */
public final class ApiHeaders {
  /** Identifies one HTTP request end to end: accepted from the caller, or generated. */
  public static final String REQUEST_ID = "X-Request-Id";

  /** POST /orders: makes a retried order request return the first answer. */
  public static final String IDEMPOTENCY_KEY = "Idempotency-Key";

  /** POST /orders: true when the answer was replayed for a known Idempotency-Key. */
  public static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

  /** Read-model reads: the version the projection had applied when it answered. */
  public static final String PROJECTED_VERSION = "X-Projected-Version";

  /** Read-model reads with a consistency token: how long the answer waited for the projection. */
  public static final String WAITED_MS = "X-Waited-Ms";

  private ApiHeaders() {}
}

package io.zeroshift.domain;

import java.time.Instant;

/** One durable operator-log line. {@code id} grows monotonically with insertion order. */
public record LogEvent(long id, Instant at, String message) {}

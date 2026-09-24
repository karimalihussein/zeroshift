package io.zeroshift.domain;

/** Durable counters of committed simulated operations; the rate covers the last ~1s window. */
public record TrafficMetrics(
    boolean running,
    Primary target,
    long total,
    long inserts,
    long updates,
    long deletes,
    long reads,
    long errors,
    long sqlServerOperations,
    long postgresOperations,
    double operationsPerSecond) {}

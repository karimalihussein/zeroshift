package io.zeroshift.domain;

/** The real row affected or observed by one simulated application operation. */
public record TrafficOperationOutcome(Table table, Long recordId, String details) {}

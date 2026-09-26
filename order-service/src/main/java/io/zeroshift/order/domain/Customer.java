package io.zeroshift.order.domain;

import java.time.Instant;
import java.util.UUID;

/** Who orders. Orders copy the name they were placed under; later edits never rewrite them. */
public record Customer(UUID id, String name, String email, String phone, Instant createdAt) {}

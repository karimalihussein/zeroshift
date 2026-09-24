package io.zeroshift.domain;

public record LiveExperiment(
    long id, long orderId, TrafficOperation operation, OrderRecord before, OrderRecord after) {}

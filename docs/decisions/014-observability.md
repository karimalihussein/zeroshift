# ADR 014: OpenTelemetry agent, Tempo, Loki and Prometheus

**Status:** Accepted

## Decision

Every service JVM runs the OpenTelemetry Java agent, attached through `JAVA_TOOL_OPTIONS` in Compose.
Traces and logs go over OTLP to an OpenTelemetry Collector, which forwards traces to Tempo and logs
to Loki. Prometheus scrapes each service's Micrometer endpoint and receives Tempo's service-graph
metrics. Grafana is provisioned with the three data sources, cross-linked by trace id, and one
dashboard.

## Why

- The agent instruments HTTP, JDBC and Kafka with no code changes. The outbox stores the current
  `traceparent`; Debezium copies it into a Kafka header; the consuming service's agent continues
  the trace. One order is one trace across all five services.
- Micrometer already exposes Kafka consumer lag, Resilience4j and HikariCP metrics, so the agent's
  metrics exporter is turned off rather than duplicating them.
- Loki is the default log store because it costs a few hundred MB. OpenSearch is available as an
  optional overlay (`docker-compose.opensearch.yml`) for full-text search, at about 2 GB more.

## Consequences

- The control plane polls every service once a second. Those reads carry a `traceparent` with the
  sampled flag off, so parent-based sampling drops them and Tempo holds only real traffic.
- The scheduling instrumentation is off: the once-a-second scanner would otherwise start a trace
  every second.
- Running a service outside Docker has no agent: `Traces.traceparent()` returns null and the outbox
  stores no trace context. Everything else works.

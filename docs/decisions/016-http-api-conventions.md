# ADR 016: One small HTTP convention across the services

**Status:** Accepted

## Decision

A `platform-web` module holds only what every service's HTTP layer shares. It carries no business
types; each service keeps its own request and response records next to its controllers.

| Concern | Convention |
|---|---|
| Single resource or command result | The typed record itself, with the correct status: `202 OrderAccepted`, `200 OrderView`, `204` for a delete. No envelope. |
| Collections | `ApiResponse<List<T>>`: `{"data": [...], "meta": {"count", "limit", "hasMore"}}`. A paged read fetches `limit + 1` rows; `ApiResponse.page` trims the extra one and sets `hasMore`. |
| Errors | `ApiError`, served as `application/problem+json` (RFC 9457): `type`, `title`, `status`, `detail`, `instance`, plus a stable `code`, the `requestId`, the `traceId`, field `errors[]` and a `context` object for machine-readable facts. |
| Error codes | `SCREAMING_SNAKE`, never reworded once shipped. Shared: `VALIDATION_FAILED`, `MALFORMED_REQUEST`, `INVALID_PARAMETER`, `NOT_FOUND`, `METHOD_NOT_ALLOWED`, `UNSUPPORTED_MEDIA_TYPE`, `CONFLICT`, `BAD_GATEWAY`, `INTERNAL_ERROR`. Service-specific: `ORDER_NOT_FOUND`, `ORDER_RULE_VIOLATION`, `IDEMPOTENCY_KEY_REUSED`, `CONCURRENT_UPDATE`, `ORDER_NOT_PROJECTED`, `READ_MODEL_BEHIND`, `UNKNOWN_CONSUMER`, `UNKNOWN_GATEWAY_MODE`, `UNKNOWN_KEYING`, `PARTITIONS_CAN_ONLY_GROW`, `NO_SHIPPED_PARCELS`, `INVALID_ACTION`. |
| Exception → HTTP | `ApiExceptionHandler` (shared, lowest precedence) maps framework errors: bean validation, method-parameter validation, unreadable bodies, missing or mistyped parameters, routing errors (keep their 404/405/415), `ApiException`; anything else is a logged 500 whose body never shows the cause. Each service adds one `@RestControllerAdvice` at `SERVICE_ADVICE` precedence for its **domain** exceptions (`OrderApiErrors`, `LabApiErrors`), so domain code never imports web types. |
| Validation | Jakarta constraints on request records (`@Valid`) and on `@RequestParam`/`@RequestHeader`/`@PathVariable` (Spring's built-in method validation, no `@Validated`). |
| Request id | `RequestIdFilter`: accepts a well-formed `X-Request-Id` (`[A-Za-z0-9._:-]{1,100}`), otherwise generates one; puts it in the MDC (`request_id`), echoes it in the response and in every error. `RequestIdPropagation` forwards it on outbound `RestClient` calls (the control plane → services). |
| Tracing | Left to the OpenTelemetry agent; errors carry its `trace_id` so a failed request is one click from its trace. |
| Headers | Names in `ApiHeaders`. Only genuinely cross-cutting ones live in a filter. Operation-specific headers stay with their operation: `Idempotency-Key`/`Idempotent-Replayed` on `POST /orders`, `X-Projected-Version`/`X-Waited-Ms` on a consistency-token read. |
| Authentication | None: a local lab. Not simulated. |

Controllers only bind, validate and delegate. Work that used to live in them moved out:
the read-your-writes wait (`ReadYourWrites`), the WireMock admin calls (`GatewaySimulator`), the
carrier's Kafka publishing and topic administration (`Carrier`), the order views and the
slow-answer fault (`OrderViews`, `SlowAnswers`).

JSON field names are camelCase everywhere, because responses are records rather than
`fetchMaps()` rows or jOOQ `formatJSON` strings.

## Why

- Before, errors came in four shapes (Spring's default, `ProblemDetail`, `{"error": ...}`, a
  hand-built JSON string in the query service), with no stable code: a client could only match
  on English text. The control plane's read-your-writes lab had to read a 409's facts from headers.
  Now `code` is what a client branches on and `context` holds the numbers (`requiredVersion`,
  `projectedVersion`, `waitedMs`).
- Most endpoints returned `Map<String,Object>` or database rows, so the API's shape was the
  schema's shape (snake_case, leaking column names) and nothing checked it at compile time.
- Wrapping only collections keeps single-resource answers plain HTTP: the status says what
  happened, the body is the resource. A list needs somewhere to say it was cut short.

## Consequences

- The control plane unwraps `data` (`LabServices.data`) so its own aggregate endpoints, which pass
  several services' answers through, keep plain arrays for the browser.
- `platform-web` is a dependency of `platform` (every event-driven service) and of `migration-lab`.
  In the Docker dev mode, `scripts/dev-server.sh` installs it into the container's Maven repository
  before `spring-boot:run`.
- Tests: `ApiConventionsTest` (the shared behaviour), `LabApiErrorsTest`, and a contract test in
  `OrderServiceIT`, `OrderQueryIT` and `ShippingServiceIT`.

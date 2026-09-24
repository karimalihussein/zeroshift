-- Durable counters for the live traffic simulator; the rate covers a ~1s window.
CREATE TABLE IF NOT EXISTS traffic_metrics(
 id INT PRIMARY KEY CHECK(id=1), inserts BIGINT NOT NULL DEFAULT 0, updates BIGINT NOT NULL DEFAULT 0,
 deletes BIGINT NOT NULL DEFAULT 0, reads BIGINT NOT NULL DEFAULT 0, errors BIGINT NOT NULL DEFAULT 0,
 consecutive_errors INT NOT NULL DEFAULT 0, sql_server_ops BIGINT NOT NULL DEFAULT 0, postgres_ops BIGINT NOT NULL DEFAULT 0,
 window_started TIMESTAMPTZ NOT NULL DEFAULT now(), window_ops BIGINT NOT NULL DEFAULT 0, ops_per_second DOUBLE PRECISION NOT NULL DEFAULT 0);
INSERT INTO traffic_metrics(id) VALUES(1) ON CONFLICT DO NOTHING;

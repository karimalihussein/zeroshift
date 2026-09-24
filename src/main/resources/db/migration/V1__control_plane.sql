-- ZeroShift control plane: durable migration state and the operator log.
-- IF NOT EXISTS keeps V1 safe on databases created before Flyway (see spring.flyway.baseline-*).
CREATE TABLE IF NOT EXISTS migration_state(
 id INT PRIMARY KEY CHECK(id=1), stage TEXT NOT NULL DEFAULT 'IDLE', status TEXT NOT NULL DEFAULT 'IDLE',
 primary_db TEXT NOT NULL DEFAULT 'SQL_SERVER', current_table TEXT NOT NULL DEFAULT 'CUSTOMERS',
 last_id BIGINT NOT NULL DEFAULT 0, customer_bound BIGINT NOT NULL DEFAULT 0, order_bound BIGINT NOT NULL DEFAULT 0,
 version BIGINT NOT NULL DEFAULT 0, copied BIGINT NOT NULL DEFAULT 0, expected BIGINT NOT NULL DEFAULT 0,
 batches BIGINT NOT NULL DEFAULT 0, applied BIGINT NOT NULL DEFAULT 0, traffic BOOLEAN NOT NULL DEFAULT FALSE,
 crash_requested BOOLEAN NOT NULL DEFAULT FALSE, traffic_step BIGINT NOT NULL DEFAULT 0, validation TEXT NOT NULL DEFAULT 'Not checked', error TEXT NOT NULL DEFAULT '',
 checkpoint TIMESTAMPTZ, rows_per_second DOUBLE PRECISION NOT NULL DEFAULT 0);
INSERT INTO migration_state(id) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS migration_log(id BIGSERIAL PRIMARY KEY, at TIMESTAMPTZ NOT NULL DEFAULT now(), message TEXT NOT NULL);

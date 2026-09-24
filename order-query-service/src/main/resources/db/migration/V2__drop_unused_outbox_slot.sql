-- Databases created before slot creation became opt-in got a slot no connector reads, which makes
-- PostgreSQL keep WAL indefinitely. Drop it if present and idle.
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
WHERE slot_name = current_database() || '_outbox' AND NOT active;

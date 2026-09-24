-- Target business tables are part of the migration itself, not the control plane: the engine
-- creates them bare, builds indexes/constraints in PREPARE, and drops those again on Reset.
CREATE TABLE IF NOT EXISTS customers(id BIGSERIAL PRIMARY KEY, name VARCHAR(180) NOT NULL, email VARCHAR(220), active BOOLEAN NOT NULL);
CREATE TABLE IF NOT EXISTS orders(id BIGSERIAL PRIMARY KEY, customer_id BIGINT NOT NULL, amount NUMERIC(19,4) NOT NULL, status VARCHAR(24) NOT NULL);

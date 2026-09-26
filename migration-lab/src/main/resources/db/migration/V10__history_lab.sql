-- Events-over-time lab: a projection the control plane builds from order.events, in two versions.
-- Its Kafka position is stored here, in the same transaction as the rows it produced, so the
-- projection is exactly where its offsets say it is, and a rebuild is: delete rows, reset offsets.
CREATE TABLE IF NOT EXISTS history_projection_offset(
  projection TEXT NOT NULL,
  kafka_partition INT NOT NULL,
  next_offset BIGINT NOT NULL,
  PRIMARY KEY(projection, kafka_partition));

-- Sales per SKU: units, revenue and how many orders contributed.
CREATE TABLE IF NOT EXISTS history_sku_sales(
  projection TEXT NOT NULL,
  sku TEXT NOT NULL,
  units BIGINT NOT NULL DEFAULT 0,
  revenue NUMERIC(16,2) NOT NULL DEFAULT 0,
  orders BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY(projection, sku));

-- The fixed projection holds an order's lines until the order ships (or forgets them if it is
-- cancelled); the first version counted them the moment the order was placed.
CREATE TABLE IF NOT EXISTS history_pending_order(
  projection TEXT NOT NULL,
  order_id UUID NOT NULL,
  lines JSONB NOT NULL,
  PRIMARY KEY(projection, order_id));

-- Records a projection could not read (an event from a newer writer, a malformed record): skipped
-- and listed, never silently dropped.
CREATE TABLE IF NOT EXISTS history_projection_skip(
  projection TEXT NOT NULL,
  kafka_partition INT NOT NULL,
  kafka_offset BIGINT NOT NULL,
  reason TEXT NOT NULL,
  PRIMARY KEY(projection, kafka_partition, kafka_offset));

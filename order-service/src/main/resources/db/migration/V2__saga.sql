CREATE TABLE saga(
  order_id UUID PRIMARY KEY,
  correlation_id UUID NOT NULL,
  state TEXT NOT NULL,
  deadline TIMESTAMPTZ,
  pending TEXT[] NOT NULL DEFAULT '{}',
  failure_reason TEXT,
  compensations TEXT[] NOT NULL DEFAULT '{}',
  version INT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL);
CREATE INDEX saga_due ON saga(deadline) WHERE deadline IS NOT NULL;
CREATE INDEX saga_recent ON saga(started_at DESC);

CREATE TABLE saga_transition(
  id BIGSERIAL PRIMARY KEY,
  order_id UUID NOT NULL REFERENCES saga(order_id),
  from_state TEXT,
  to_state TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  trigger_event_id UUID,
  detail TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX saga_transition_order ON saga_transition(order_id, id);

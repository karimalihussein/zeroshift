-- Experiment runs of the event lab's learning labs (client retry, read-your-writes): what the
-- control plane did as a client, and what each request got back, kept for review.
CREATE TABLE IF NOT EXISTS lab_run(
  id BIGSERIAL PRIMARY KEY,
  lab TEXT NOT NULL,
  mode TEXT NOT NULL,
  summary TEXT NOT NULL,
  result JSONB NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE INDEX IF NOT EXISTS lab_run_lab ON lab_run(lab, id DESC);

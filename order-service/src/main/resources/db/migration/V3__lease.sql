-- A lease: a lock with an expiry, so a crashed holder cannot keep it forever. token is a fencing
-- token: it increases every time ownership changes hands, so work stamped with an older token is
-- recognisably from a previous (possibly paused, not dead) owner.
CREATE TABLE lease(
  name TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  token BIGINT NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  expires_at TIMESTAMPTZ NOT NULL);

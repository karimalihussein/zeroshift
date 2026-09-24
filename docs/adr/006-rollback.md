# ADR 006: Honest rollback simulation

**Decision:** Track target-only writes and mark direct rollback unsafe once any exist. Do not pretend a connection-string reversal is recovery.

Full reverse replication would obscure the main lesson and substantially enlarge the lab. A production plan should choose bidirectional/reverse capture or a controlled reconciliation procedure before cutover.

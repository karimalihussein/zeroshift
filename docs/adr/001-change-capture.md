# ADR 001: Transactional change log

**Decision:** Use SQL Server triggers to append row images and operation metadata to `migration_change_log` in the mutating transaction.

**Why:** Native CDC packaging/agent behavior varies in local containers. The trigger log is deterministic, inspectable, supports all operations, and makes the correctness invariant teachable. Timestamp-only polling was rejected because it cannot provide strict ordering. Production systems should reevaluate native CDC or log-based tooling and measure trigger overhead.

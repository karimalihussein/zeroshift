# ADR 002: Change-ID snapshot boundary

**Decision:** Read the maximum committed change ID before copying and replay all IDs above it.

Because log insertion shares the source transaction, an in-flight transaction cannot silently fall before the boundary and outside the later copy. Copy/replay overlap is resolved by idempotent target operations. Enabling capture after copy starts was rejected because it creates a loss window.

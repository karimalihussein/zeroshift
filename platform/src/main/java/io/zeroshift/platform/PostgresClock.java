package io.zeroshift.platform;

import java.time.Duration;
import java.time.OffsetDateTime;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

/**
 * PostgreSQL's wall clock. jOOQ's {@code currentOffsetDateTime()} renders {@code
 * current_timestamp}, which is frozen at the start of the transaction; {@code clock_timestamp()} is
 * the real time at each call, which is what lease expiries and "recorded at" columns mean.
 */
public final class PostgresClock {
  public static final Field<OffsetDateTime> NOW =
      DSL.field("clock_timestamp()", SQLDataType.TIMESTAMPWITHTIMEZONE);

  /** {@code NOW + interval}, for an expiry measured on the database's clock. */
  public static Field<OffsetDateTime> nowPlus(Duration duration) {
    return DSL.field(
        "clock_timestamp() + {0}::interval",
        SQLDataType.TIMESTAMPWITHTIMEZONE, DSL.val(duration.toMillis() + " milliseconds"));
  }

  private PostgresClock() {}
}

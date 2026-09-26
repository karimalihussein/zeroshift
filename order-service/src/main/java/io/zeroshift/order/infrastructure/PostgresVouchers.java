package io.zeroshift.order.infrastructure;

import static io.zeroshift.order.db.Tables.VOUCHER;

import io.zeroshift.order.application.Vouchers;
import io.zeroshift.order.db.tables.records.VoucherRecord;
import io.zeroshift.order.domain.Voucher;
import io.zeroshift.platform.PostgresClock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;

public final class PostgresVouchers implements Vouchers {
  private final DSLContext db;

  public PostgresVouchers(DSLContext db) {
    this.db = db;
  }

  @Override
  public Optional<Voucher> find(String code) {
    return db.selectFrom(VOUCHER)
        .where(VOUCHER.CODE.eq(code))
        .fetchOptional(PostgresVouchers::voucher);
  }

  @Override
  public List<Voucher> all() {
    return db.selectFrom(VOUCHER).orderBy(VOUCHER.CODE).fetch(PostgresVouchers::voucher);
  }

  /**
   * The race for the last use is decided by the row lock: a second redeemer waits for the first to
   * commit, then re-evaluates the condition against the new usage_count and matches nothing.
   */
  @Override
  public boolean redeem(UUID id) {
    return db.update(VOUCHER)
            .set(VOUCHER.USAGE_COUNT, VOUCHER.USAGE_COUNT.plus(1))
            .set(VOUCHER.VERSION, VOUCHER.VERSION.plus(1))
            .set(VOUCHER.UPDATED_AT, PostgresClock.NOW)
            .where(VOUCHER.ID.eq(id))
            .and(VOUCHER.ACTIVE)
            .and(VOUCHER.VALID_FROM.le(PostgresClock.NOW))
            .and(VOUCHER.VALID_UNTIL.isNull().or(VOUCHER.VALID_UNTIL.gt(PostgresClock.NOW)))
            .and(VOUCHER.USAGE_LIMIT.isNull().or(VOUCHER.USAGE_COUNT.lt(VOUCHER.USAGE_LIMIT)))
            .execute()
        == 1;
  }

  private static Voucher voucher(VoucherRecord r) {
    return new Voucher(
        r.getId(),
        r.getCode(),
        Voucher.DiscountType.valueOf(r.getDiscountType()),
        r.getValue(),
        r.getMinimumAmount(),
        r.getMaximumDiscount(),
        r.getUsageLimit(),
        r.getUsageCount(),
        r.getValidFrom().toInstant(),
        r.getValidUntil() == null ? null : r.getValidUntil().toInstant(),
        r.getActive());
  }
}

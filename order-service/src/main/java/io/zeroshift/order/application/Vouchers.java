package io.zeroshift.order.application;

import io.zeroshift.order.domain.Voucher;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface Vouchers {
  /** {@code code} already upper case. */
  Optional<Voucher> find(String code);

  List<Voucher> all();

  /**
   * Takes one use in the caller's transaction, if the voucher is still active, valid and not used
   * up. One conditional update: two placements racing for the last use cannot both get it.
   */
  boolean redeem(UUID id);
}

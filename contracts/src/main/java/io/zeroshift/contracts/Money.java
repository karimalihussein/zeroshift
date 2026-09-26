package io.zeroshift.contracts;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Money rules shared by every service: two decimal places, rounded half-even, never a float. The
 * lab sells in one currency per deployment, but every amount still travels with its currency.
 */
public final class Money {
  public static final int SCALE = 2;
  public static final RoundingMode ROUNDING = RoundingMode.HALF_EVEN;
  public static final BigDecimal ZERO = BigDecimal.ZERO.setScale(SCALE);

  private Money() {}

  /** {@code amount} at scale 2, rounded half-even. */
  public static BigDecimal of(BigDecimal amount) {
    return amount.setScale(SCALE, ROUNDING);
  }

  public static BigDecimal of(String amount) {
    return of(new BigDecimal(amount));
  }

  /** {@code amount} must already be an exact amount of cents (a price, a stored total). */
  public static BigDecimal exact(BigDecimal amount) {
    try {
      return amount.setScale(SCALE, RoundingMode.UNNECESSARY);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(amount + " has more than " + SCALE + " decimals", e);
    }
  }

  public static boolean isCurrency(String code) {
    return code != null && code.matches("[A-Z]{3}");
  }

  /**
   * Splits {@code total} across {@code weights} in proportion, in whole cents, so the parts always
   * add up to {@code total}: each part is rounded down, then the leftover cents go to the parts
   * with the largest remainders (ties to the earlier part). Deterministic for the same inputs.
   */
  public static List<BigDecimal> allocate(BigDecimal total, List<BigDecimal> weights) {
    long cents = exact(total).movePointRight(SCALE).longValueExact();
    var sum = weights.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
    var parts = new long[weights.size()];
    var remainders = new BigDecimal[weights.size()];
    long given = 0;
    for (int i = 0; i < weights.size(); i++) {
      if (sum.signum() == 0) {
        remainders[i] = BigDecimal.ZERO;
        continue;
      }
      var exactShare = BigDecimal.valueOf(cents).multiply(weights.get(i));
      var quotient = exactShare.divideToIntegralValue(sum);
      parts[i] = quotient.longValueExact();
      remainders[i] = exactShare.subtract(quotient.multiply(sum));
      given += parts[i];
    }
    if (sum.signum() == 0 && !weights.isEmpty()) parts[0] = cents;
    else
      for (long left = cents - given; left > 0; left--) {
        int best = 0;
        for (int i = 1; i < remainders.length; i++)
          if (remainders[i].compareTo(remainders[best]) > 0) best = i;
        parts[best]++;
        remainders[best] = BigDecimal.valueOf(-1);
      }
    var out = new ArrayList<BigDecimal>(parts.length);
    for (long p : parts) out.add(BigDecimal.valueOf(p, SCALE));
    return out;
  }
}

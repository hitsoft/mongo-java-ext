package com.hitsoft.types;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Extended type for strict working with Currency values
 */
public class Currency implements Comparable<Currency> {

  BigDecimal val;
  private static final int PRECISION = 4;
  private static final RoundingMode mode = RoundingMode.HALF_UP;

  private Currency() {
    val = BigDecimal.valueOf(0, PRECISION).setScale(PRECISION, mode);
  }

  private Currency(long value) {
    val = BigDecimal.valueOf(value, PRECISION).setScale(PRECISION, mode);
  }

  private Currency(BigDecimal value) {
    val = value.setScale(PRECISION, mode);
  }

  public static Currency valueOf(long value) {
    return new Currency(value);
  }

  public long longValue() {
    return val.movePointRight(PRECISION).longValue();
  }

  public Currency add(Currency value) {
    if (value != null)
      return new Currency(val.add(value.val));
    else
      return new Currency(val);
  }

  public Currency subtract(Currency value) {
    if (value != null)
      return new Currency(val.subtract(value.val));
    else
      return new Currency(val);
  }

  public Currency multiply(Currency value) {
    if (value != null)
      return new Currency(val.multiply(value.val));
    else
      return null;
  }

  public Currency multiply(long value) {
    return new Currency(val.multiply(BigDecimal.valueOf(value)));
  }

  public Currency multiply(int value) {
    return new Currency(val.multiply(BigDecimal.valueOf(value)));
  }

  public Currency divide(int value) {
    return new Currency(val.divide(BigDecimal.valueOf(value)));
  }

  public Currency divide(long value) {
    return new Currency(val.divide(BigDecimal.valueOf(value)));
  }

  public Currency divide(Currency value) {
    if (value != null)
      return new Currency(val.divide(value.val));
    else
      return null;
  }

  public int compareTo(Currency value) {
    if (value == null)
      return val.compareTo(null);
    else
      return val.compareTo(value.val);
  }

  @Override
  public String toString() {
    return val.toString();
  }

  public static Currency valueOf(double value) {
    return valueOf(Math.round(value * 10000));
  }
}

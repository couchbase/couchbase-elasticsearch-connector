package com.couchbase.connector.elasticsearch.io;

import org.junit.Test;

import java.math.BigInteger;

import static java.math.BigInteger.ONE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class DefaultDocumentTransformerTest {
  private static final BigInteger MAX_UNSIGNED_LONG = new BigInteger("2").pow(64).subtract(ONE);
  private static final BigInteger MAX_SIGNED_LONG = new BigInteger("2").pow(63).subtract(ONE);

  @Test
  public void getCounterValue() {
    assertCounter(0L, "0");
    assertCounter(1L, "1");
    assertCounter(Long.MAX_VALUE, MAX_SIGNED_LONG);
    assertCounter(Long.MIN_VALUE, MAX_SIGNED_LONG.add(ONE));
    assertCounter(-1L, MAX_UNSIGNED_LONG);
  }

  @Test
  public void getCounterValueOutsideRange() {
    assertCounter(null, MAX_UNSIGNED_LONG.add(ONE));
    assertCounter(null, "-1");
  }

  @Test
  public void getCounterValueNotIntegral() {
    assertCounter(null, "1.0");
    assertCounter(null, "-1.0");
  }

  @Test
  public void getCounterValueInvalidJson() {
    assertCounter(null, "1 true");
    assertCounter(null, "xyz");
    assertCounter(null, "");
    assertCounter(null, " ");
    assertCounter(null, "{}");
    assertCounter(null, "1 2");
    assertCounter(null, "\"abc\"");
    assertCounter(null, "\"1\"");
    assertCounter(null, "\"1\"");
    assertCounter(null, "1a");
  }

  private static void assertCounter(Long expected, String json) {
    assertEquals(expected, DefaultDocumentTransformer.getCounterValue(json.getBytes(UTF_8)));
  }

  private static void assertCounter(Long expected, BigInteger value) {
    assertCounter(expected, value.toString());
  }
}

package com.couchbase.connector.elasticsearch.util;

import com.couchbase.connector.util.QuietPeriodExecutor;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class QuietPeriodExecutorTest {

  @Test
  public void test() throws Exception {
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    QuietPeriodExecutor executor = new QuietPeriodExecutor(Duration.ofSeconds(3), service);

    AtomicInteger counter = new AtomicInteger();

    executor.schedule(counter::incrementAndGet);
    executor.schedule(counter::incrementAndGet);

    assertEquals(0, counter.get());
    SECONDS.sleep(1);
    assertTrue(executor.getFluxDuration(SECONDS) >= 1);
    SECONDS.sleep(3);
    assertEquals(1, counter.get());

    assertEquals(0, executor.getFluxDuration(NANOSECONDS));

    executor.schedule(counter::incrementAndGet);
    SECONDS.sleep(4);
    assertEquals(2, counter.get());
  }
}

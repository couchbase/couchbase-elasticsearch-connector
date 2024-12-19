package com.couchbase.connector.util;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.dcp.core.utils.DefaultObjectMapper;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.couchbase.connector.util.ListHelper.chunks;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class ListHelperTest {
  @Test(expected = IllegalArgumentException.class)
  public void chunkIntoZero() throws Exception {
    chunks(ImmutableList.of(1, 2, 3, 4, 5), 0);
  }

  @Test
  public void chunkIntoVarious() throws Exception {
    check(0, 1, "[[]]");
    check(0, 2, "[[],[]]");
    check(4, 2, "[[1,2],[3,4]]");
    check(3, 1, "[[1,2,3]]");
    check(3, 2, "[[1,2],[3]]");
    check(3, 3, "[[1],[2],[3]]");
    check(3, 5, "[[1],[2],[3],[],[]]");
    check(5, 3, "[[1,2],[3,4],[5]]");
  }

  private static void check(int listSize, int numChunks, String expectedJson) throws IOException {
    final List<Integer> list = IntStream.range(1, listSize + 1).boxed().collect(toList());
    List<List<Integer>> expected = DefaultObjectMapper.readValue(expectedJson, new TypeReference<List<List<Integer>>>() {
    });
    assertEquals(expected, chunks(list, numChunks));
  }
}

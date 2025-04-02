/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.format.olympia.tree;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.format.olympia.ObjectKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestTreeUtil {

  @Test
  public void testVectorBinarySearch() {
    try (BufferAllocator allocator = new RootAllocator();
        VarCharVector targetVector = new VarCharVector("target", allocator);
        VarCharVector searchKey = new VarCharVector("search", allocator)) {

      searchKey.allocateNew();
      searchKey.setValueCount(1);

      targetVector.allocateNew();
      targetVector.setNull(0);
      targetVector.setSafe(1, "a".getBytes());
      targetVector.setSafe(2, "c".getBytes());
      targetVector.setSafe(3, "e".getBytes());
      targetVector.setSafe(4, "g".getBytes());
      targetVector.setValueCount(5);

      VectorValueComparator<VarCharVector> comparator = new NodeVarCharComparator();

      // Search for exact match
      searchKey.setSafe(0, "c".getBytes());
      int result = TreeUtil.vectorBinarySearch(targetVector, comparator, searchKey, 0, 0, 4);
      assertThat(result).isEqualTo(2);

      // Search for value that is in left child
      searchKey.setSafe(0, "0".getBytes());
      result = TreeUtil.vectorBinarySearch(targetVector, comparator, searchKey, 0, 0, 4);
      assertThat(result).isEqualTo(-2);
      assertThat(-(result) - 2).isEqualTo(0);

      // Search for value between keys (between 'a' and 'c')
      searchKey.setSafe(0, "b".getBytes());
      result = TreeUtil.vectorBinarySearch(targetVector, comparator, searchKey, 0, 0, 4);
      assertThat(result).isEqualTo(-3);
      assertThat(-(result) - 2).isEqualTo(1);

      // Search for value between keys (between 'e' and 'g')
      searchKey.setSafe(0, "f".getBytes());
      result = TreeUtil.vectorBinarySearch(targetVector, comparator, searchKey, 0, 0, 4);
      assertThat(result).isEqualTo(-5);
      assertThat(-(result) - 2).isEqualTo(3);

      // Search for value in rightmost child (after 'g')
      searchKey.setSafe(0, "h".getBytes());
      result = TreeUtil.vectorBinarySearch(targetVector, comparator, searchKey, 0, 0, 4);
      assertThat(result).isEqualTo(-6);
      assertThat(-(result) - 2).isEqualTo(4);

      // Test search with range
      searchKey.setSafe(0, "d".getBytes());
      result = TreeUtil.vectorBinarySearch(targetVector, comparator, searchKey, 0, 2, 3);
      assertThat(result).isEqualTo(-4);
      assertThat(-(result) - 2).isEqualTo(2);
    }
  }

  @Test
  public void testCreateSearchKey() {
    try (BufferAllocator allocator = new RootAllocator()) {
      String testKey = "testKey";
      try (VarCharVector vector = TreeUtil.createSearchKey(allocator, testKey)) {
        assertThat(vector.getValueCount()).isEqualTo(1);
        assertThat(new String(vector.get(0), StandardCharsets.UTF_8)).isEqualTo(testKey);
      }

      byte[] testBytes = "testBytes".getBytes(StandardCharsets.UTF_8);
      try (VarCharVector vector = TreeUtil.createSearchKey(allocator, testBytes)) {
        assertThat(vector.getValueCount()).isEqualTo(1);
        assertThat(vector.get(0)).isEqualTo(testBytes);
      }
    }
  }

  @Test
  public void testIsSystemKey() {
    assertThat(TreeUtil.isSystemKey(ObjectKeys.CREATED_AT_MILLIS)).isTrue();
    assertThat(TreeUtil.isSystemKey(ObjectKeys.NUMBER_OF_KEYS)).isTrue();
    assertThat(TreeUtil.isSystemKey(ObjectKeys.CATALOG_DEFINITION)).isTrue();
    assertThat(TreeUtil.isSystemKey(ObjectKeys.PREVIOUS_ROOT_NODE)).isTrue();
    assertThat(TreeUtil.isSystemKey(ObjectKeys.ROLLBACK_FROM_ROOT_NODE)).isTrue();
    assertThat(TreeUtil.isSystemKey("key")).isFalse();
  }

  @ParameterizedTest
  @CsvSource({"null, null, 0", "null, b, -1", "a, null, 1", "a, b, -1", "b, a, 1", "a, a, 0"})
  public void testCompareKeys(String key1, String key2, int expectedResult) {
    String sliceKey = "null".equals(key1) ? null : key1;
    String pendingKey = "null".equals(key2) ? null : key2;

    assertThat(TreeUtil.compareKeys(sliceKey, pendingKey)).isEqualTo(expectedResult);
  }
}

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

import java.nio.charset.StandardCharsets;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.format.olympia.ObjectKeys;

public class TreeUtil {

  private TreeUtil() {}

  /**
   * Performs a binary search in a B-tree node vector. This method accounts for the B-tree node
   * structure where the first element is null (representing the leftmost child pointer). Similar to
   * the Arrays.BinarySearch method.
   *
   * <p>B-tree node structure: [null | k1 | k2 | k3 | ... | kn] Child pointers: 0 1 2 3 ... n For
   * example, in node [null | a | c | e | g]: - Exact match "c" returns 2 (positive index where
   * found) - Search "0" returns -2 (child point 0, before 'a')
   *
   * @param targetVector The vector containing the B-tree node's keys
   * @param comparator The comparator for comparing keys
   * @param keyVector The vector containing the search key
   * @param keyIndex The index of the search key in keyVector
   * @param startIndex The index of the first element (inclusive) to be searched
   * @param endIndex The index of the last element (exclusive) to be searched
   * @return index of the search key if found (>= 0); otherwise, (-(insertion point) - 1).
   */
  public static int vectorBinarySearch(
      VarCharVector targetVector,
      VectorValueComparator<VarCharVector> comparator,
      VarCharVector keyVector,
      int keyIndex,
      int startIndex,
      int endIndex) {
    comparator.attachVectors(keyVector, targetVector);
    int low = startIndex;
    int high = Math.min(targetVector.getValueCount() - 1, endIndex);
    while (low <= high) {
      int mid = (low + high) >>> 1;

      int cmp = comparator.compare(keyIndex, mid);
      if (cmp < 0) {
        high = mid - 1;
      } else if (cmp > 0) {
        low = mid + 1;
      } else {
        return mid;
      }
    }
    return -(low + 1);
  }

  public static VarCharVector createSearchKey(BufferAllocator allocator, String key) {
    return createSearchKey(allocator, key.getBytes(StandardCharsets.UTF_8));
  }

  public static VarCharVector createSearchKey(BufferAllocator allocator, byte[] key) {
    VarCharVector searchKeyVector = new VarCharVector("searchKey", allocator);
    searchKeyVector.allocateNew(1);
    searchKeyVector.setSafe(0, key);
    searchKeyVector.setValueCount(1);
    return searchKeyVector;
  }

  // TODO: Replace this with a byte comparison against ArrowBufPointers
  public static boolean isSystemKey(String key) {
    return key.equals(ObjectKeys.CREATED_AT_MILLIS)
        || key.equals(ObjectKeys.NUMBER_OF_KEYS)
        || key.equals(ObjectKeys.CATALOG_DEFINITION)
        || key.equals(ObjectKeys.PREVIOUS_ROOT_NODE)
        || key.equals(ObjectKeys.ROLLBACK_FROM_ROOT_NODE);
  }

  public static int compareKeys(String sliceKey, String pendingKey) {
    if (sliceKey == null && pendingKey == null) {
      return 0;
    } else if (sliceKey == null) {
      return -1;
    } else if (pendingKey == null) {
      return 1;
    } else {
      return sliceKey.compareTo(pendingKey);
    }
  }
}

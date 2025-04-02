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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.format.olympia.ObjectKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestNodeVarCharComparator {

  private BufferAllocator allocator;
  private VarCharVector vector1;
  private VarCharVector vector2;
  private NodeVarCharComparator comparator;

  @BeforeEach
  public void before() {
    allocator = new RootAllocator();
    vector1 = new VarCharVector("vector1", allocator);
    vector2 = new VarCharVector("vector2", allocator);
    comparator = new NodeVarCharComparator();
    vector1.allocateNew();
    vector2.allocateNew();
  }

  @AfterEach
  public void after() {
    vector1.close();
    vector2.close();
    allocator.close();
  }

  @Test
  public void testCompareRegularKeys() {
    vector1.setSafe(0, "a".getBytes());
    vector1.setSafe(1, "b".getBytes());
    vector1.setValueCount(2);

    vector2.setSafe(0, "b".getBytes());
    vector2.setSafe(1, "a".getBytes());
    vector2.setValueCount(2);

    comparator.attachVectors(vector1, vector2);

    // Test comparisons
    assertThat(comparator.compare(0, 0)).isLessThan(0);
    assertThat(comparator.compare(1, 1)).isGreaterThan(0);
    assertThat(comparator.compare(0, 1)).isEqualTo(0);
  }

  @Test
  public void testCompareWithNullsComeBeforeTreeKeys() {
    vector1.setSafe(0, "a".getBytes());
    vector1.setValueCount(2);

    vector2.setSafe(0, "b".getBytes());
    vector2.setValueCount(2);

    comparator.attachVectors(vector1, vector2);

    assertThat(comparator.compare(0, 1)).isGreaterThan(0);
    assertThat(comparator.compare(1, 0)).isLessThan(0);
    assertThat(comparator.compare(1, 1)).isEqualTo(0);
  }

  @Test
  public void testCompareWithSystemKeysBeforeTreeKeys() {
    vector1.setSafe(0, ObjectKeys.NUMBER_OF_KEYS_BYTES);
    vector1.setSafe(1, "key".getBytes());
    vector1.setValueCount(2);

    vector2.setSafe(0, "key".getBytes());
    vector2.setSafe(1, ObjectKeys.NUMBER_OF_KEYS_BYTES);
    vector2.setValueCount(2);

    comparator.attachVectors(vector1, vector2);
    assertThat(comparator.compare(0, 0)).isLessThan(0);
    assertThat(comparator.compare(1, 1)).isGreaterThan(0);
  }

  @Test
  public void testCompareSystemKeyComesBeforeNull() {
    vector1.setSafe(0, ObjectKeys.CATALOG_DEFINITION_BYTES);
    vector1.setValueCount(1);
    vector2.setValueCount(1);

    comparator.attachVectors(vector1, vector2);

    assertThat(comparator.compare(0, 0)).isLessThan(0);
  }
}

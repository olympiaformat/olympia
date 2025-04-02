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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.VarCharVector;

public class NodeVarCharComparator extends VectorValueComparator<VarCharVector> {
  private final ArrowBufPointer reusablePointer1 = new ArrowBufPointer();
  private final ArrowBufPointer reusablePointer2 = new ArrowBufPointer();

  public NodeVarCharComparator() {
    super(8);
  }

  @Override
  public int compare(int index1, int index2) {
    boolean isNull1 = vector1.isNull(index1);
    boolean isNull2 = vector2.isNull(index2);

    if (isNull1 && !isNull2) {
      vector2.getDataPointer(index2, reusablePointer2);
      boolean isSystemKey2 = TreeUtil.isSystemKey(getStringFromPointer(reusablePointer2));
      return isSystemKey2 ? 1 : -1;
    } else if (!isNull1 && isNull2) {
      vector1.getDataPointer(index1, reusablePointer1);
      boolean isSystemKey1 = TreeUtil.isSystemKey(getStringFromPointer(reusablePointer1));
      return isSystemKey1 ? -1 : 1;
    } else if (isNull1) {
      return 0;
    }
    return compareNotNull(index1, index2);
  }

  @Override
  public int compareNotNull(int index1, int index2) {
    vector1.getDataPointer(index1, reusablePointer1);
    vector2.getDataPointer(index2, reusablePointer2);
    boolean isSystemKey1 = TreeUtil.isSystemKey(getStringFromPointer(reusablePointer1));
    boolean isSystemKey2 = TreeUtil.isSystemKey(getStringFromPointer(reusablePointer2));
    if (isSystemKey1 && !isSystemKey2) {
      return -1;
    }
    if (!isSystemKey1 && isSystemKey2) {
      return 1;
    }
    return reusablePointer1.compareTo(reusablePointer2);
  }

  @Override
  public VectorValueComparator<VarCharVector> createNew() {
    return new NodeVarCharComparator();
  }

  private String getStringFromPointer(ArrowBufPointer pointer) {
    ArrowBuf buffer = pointer.getBuf();
    long offset = pointer.getOffset();
    long length = pointer.getLength();
    byte[] bytes = new byte[(int) length];
    buffer.getBytes(offset, bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}

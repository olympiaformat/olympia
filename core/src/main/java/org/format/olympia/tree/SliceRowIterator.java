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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.format.olympia.exception.StorageReadFailureException;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.local.LocalInputStream;

public class SliceRowIterator implements RowIterator, Comparable<RowIterator> {
  private final LocalInputStream stream;
  private final ArrowFileReader reader;
  private VectorSchemaRoot root;
  private VarCharVector keyVector;
  private VarCharVector valueVector;
  private VarCharVector childVector;

  private int batchIndex = 0;
  private final int endIndex;
  private int rowIndexInBatch = 0;
  private int globalRowIndex;
  private boolean hasNextRecordBlock = true;
  private boolean hasCurrent = false;

  private static VectorValueComparator<VarCharVector> keyComparator;

  public SliceRowIterator(CatalogStorage storage, VectorSlice slice) {
    String path = slice.path();
    int startIndex = slice.startIndex();
    this.endIndex = slice.endIndex();
    this.globalRowIndex = 0;

    try {
      BufferAllocator allocator = storage.getArrowAllocator();
      this.stream = storage.startReadLocal(path);
      this.reader = new ArrowFileReader(stream.channel(), allocator);

      if (reader.getRecordBlocks().isEmpty()) {
        hasNextRecordBlock = false;
        hasCurrent = false;
      } else {
        loadBatch(batchIndex);

        // skip to the correct block
        while (hasNextRecordBlock && (globalRowIndex + root.getRowCount()) <= startIndex) {
          globalRowIndex += root.getRowCount();
          batchIndex++;
          if (batchIndex >= reader.getRecordBlocks().size()) {
            hasNextRecordBlock = false;
            return;
          }
          loadBatch(batchIndex);
        }

        // advance to the correct index in the current block
        if (hasNextRecordBlock && globalRowIndex < startIndex) {
          int skipInCurrentBatch = startIndex - globalRowIndex;
          if (skipInCurrentBatch < root.getRowCount()) {
            rowIndexInBatch = skipInCurrentBatch;
            globalRowIndex = startIndex;
          }
        }
        keyComparator = new NodeVarCharComparator();
        hasCurrent =
            (root.getRowCount() > 0)
                && (rowIndexInBatch < root.getRowCount())
                && (globalRowIndex <= endIndex);
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  @Override
  public String key() {
    return keyVector.isNull(rowIndexInBatch)
        ? null
        : new String(keyVector.get(rowIndexInBatch), StandardCharsets.UTF_8);
  }

  @Override
  public String value() {
    return valueVector.isNull(rowIndexInBatch)
        ? null
        : new String(valueVector.get(rowIndexInBatch), StandardCharsets.UTF_8);
  }

  @Override
  public TreeNode child() {
    if (childVector.isNull(rowIndexInBatch)) {
      return null;
    }
    String path = new String(childVector.get(rowIndexInBatch), StandardCharsets.UTF_8);
    BasicTreeNode node = new BasicTreeNode();
    node.setPath(path);
    return node;
  }

  @Override
  public void next() {
    advance();
  }

  @Override
  public boolean hasNext() {
    return hasCurrent;
  }

  @Override
  public int compareTo(RowIterator o) {
    if (o instanceof SliceRowIterator) {
      return compareVectors((SliceRowIterator) o);
    } else {
      return key().compareTo(o.key());
    }
  }

  public boolean isCurrentKeyNull() {
    return keyVector.isNull(rowIndexInBatch);
  }

  public void transferToTargetVectors(
      VarCharVector targetKeyVector,
      VarCharVector targetValueVector,
      VarCharVector targetChildVector,
      int index) {
    if (keyVector.isNull(rowIndexInBatch)) {
      targetKeyVector.setNull(index);
    } else {
      targetKeyVector.setSafe(index, keyVector.get(rowIndexInBatch));
    }
    if (valueVector.isNull(rowIndexInBatch)) {
      targetValueVector.setNull(index);
    } else {
      targetValueVector.setSafe(index, valueVector.get(rowIndexInBatch));
    }

    if (childVector.isNull(rowIndexInBatch)) {
      targetChildVector.setNull(index);
    } else {
      targetChildVector.setSafe(index, childVector.get(rowIndexInBatch));
    }
  }

  private int compareVectors(SliceRowIterator other) {
    if (this.isCurrentKeyNull()) {
      return -1;
    } else if (other.isCurrentKeyNull()) {
      return 1;
    }
    keyComparator.attachVectors(this.keyVector, other.keyVector);
    return keyComparator.compare(this.rowIndexInBatch, other.rowIndexInBatch);
  }

  private void advance() {
    try {
      rowIndexInBatch++;
      globalRowIndex++;

      if (endIndex >= 0 && globalRowIndex > endIndex) {
        hasNextRecordBlock = false;
        hasCurrent = false;
        return;
      }

      if (rowIndexInBatch >= root.getRowCount()) {
        batchIndex++;
        if (batchIndex >= reader.getRecordBlocks().size()) {
          hasCurrent = false;
          hasNextRecordBlock = false;
        } else {
          loadBatch(batchIndex);
          // Only set hasCurrent true if the batch has rows
          hasCurrent = (root.getRowCount() > 0);
        }
      } else {
        // Update hasCurrent even when staying in the same batch
        hasCurrent = (rowIndexInBatch < root.getRowCount());
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  private void loadBatch(int batchIdx) throws IOException {
    reader.loadRecordBatch(reader.getRecordBlocks().get(batchIdx));
    root = reader.getVectorSchemaRoot();
    keyVector = (VarCharVector) root.getVector(0);
    valueVector = (VarCharVector) root.getVector(1);
    childVector = (VarCharVector) root.getVector(2);
    rowIndexInBatch = 0;
  }

  @Override
  public void close() throws Exception {
    try {
      reader.close();
      stream.close();
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }
}

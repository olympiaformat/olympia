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
import java.util.Iterator;
import java.util.Optional;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.format.olympia.exception.StorageReadFailureException;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.local.LocalInputStream;

public class VectorSliceStream
    implements Iterator<NodeKeyTableRow>, Comparable<VectorSliceStream>, AutoCloseable {
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

  private static VectorValueComparator<VarCharVector> keyComparator;

  public VectorSliceStream(CatalogStorage storage, VectorSlice slice) {
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

  public void advance() {
    try {
      rowIndexInBatch++;
      globalRowIndex++;

      if (endIndex >= 0 && globalRowIndex >= endIndex) {
        hasNextRecordBlock = false;
        return;
      }

      if (rowIndexInBatch >= root.getRowCount()) {
        batchIndex++;
        if (batchIndex >= reader.getRecordBlocks().size()) {
          hasNextRecordBlock = false;
        } else {
          loadBatch(batchIndex);
        }
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  public boolean isCurrentKeyNull() {
    return keyVector.isNull(rowIndexInBatch);
  }

  // TODO: return bytes for comparison
  public String getCurrentKey() {
    if (isCurrentKeyNull()) {
      return null;
    }
    return new String(keyVector.get(rowIndexInBatch), StandardCharsets.UTF_8);
  }

  public String getCurrentValue() {
    if (valueVector.isNull(rowIndexInBatch)) {
      return null;
    }
    return new String(valueVector.get(rowIndexInBatch), StandardCharsets.UTF_8);
  }

  public String getCurrentChildPath() {
    if (childVector.isNull(rowIndexInBatch)) {
      return null;
    }
    return new String(childVector.get(rowIndexInBatch), StandardCharsets.UTF_8);
  }

  @Override
  public int compareTo(VectorSliceStream other) {
    if (this.isCurrentKeyNull()) {
      return -1;
    }
    if (other.isCurrentKeyNull()) {
      return 1;
    }

    keyComparator.attachVectors(this.keyVector, other.keyVector);
    return keyComparator.compare(this.rowIndexInBatch, other.rowIndexInBatch);
  }

  @Override
  public boolean hasNext() {
    if (!hasNextRecordBlock) {
      return false;
    }
    return endIndex < 0 || globalRowIndex < endIndex;
  }

  @Override
  public NodeKeyTableRow next() {
    if (!hasNext()) {
      throw new IllegalStateException("Slice has been processed");
    }

    TreeNode child = null;
    String childPath = getCurrentChildPath();
    if (childPath != null) {
      child = new BasicTreeNode();
      child.setPath(childPath);
    }

    NodeKeyTableRow row =
        ImmutableNodeKeyTableRow.builder()
            .key(Optional.ofNullable(getCurrentKey()))
            .value(Optional.ofNullable(getCurrentValue()))
            .child(Optional.ofNullable(child))
            .build();

    advance();
    return row;
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

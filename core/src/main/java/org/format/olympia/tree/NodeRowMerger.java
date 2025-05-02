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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import org.apache.arrow.vector.VarCharVector;
import org.format.olympia.exception.OlympiaRuntimeException;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.storage.CatalogStorage;

public class NodeRowMerger implements AutoCloseable {
  private final List<RowIterator> resources = Lists.newArrayList();
  private final PriorityQueue<RowIterator> queue;
  private final boolean leftChildFound;

  public NodeRowMerger(CatalogStorage storage, TreeNode node) {
    this.queue = new PriorityQueue<>();
    node.getVectorSlices().forEach(slice -> addIterator(new SliceRowIterator(storage, slice)));

    // check for existing left child
    this.leftChildFound =
        node.getLeftmostChild().isPresent() || !queue.isEmpty() && queue.peek().key() == null;
    addIterator(
        new PendingRowIterator(node.pendingChanges(), node.getLeftmostChild().orElse(null)));
  }

  private void addIterator(RowIterator iterator) {
    resources.add(iterator);
    if (iterator.hasNext()) {
      queue.add(iterator);
    }
  }

  public NodeKeyTableRow findMiddleRow(int order) {
    int middle = order / 2;
    int count = 0;
    while (!queue.isEmpty()) {
      RowIterator current = queue.poll();
      String currentKey = current.key();
      if (currentKey != null) {
        count += 1;
      }

      if (count == middle) {
        return ImmutableNodeKeyTableRow.builder()
            .key(Optional.ofNullable(currentKey))
            .value(Optional.ofNullable(current.value()))
            .child(Optional.ofNullable(current.child()))
            .build();
      }

      skipDuplicateKeys(currentKey);
      advanceIterator(current);
    }
    return ImmutableNodeKeyTableRow.builder().build();
  }

  public int writeToVectors(
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector,
      int startIndex) {
    int index = startIndex;

    if (!leftChildFound) {
      keyVector.setNull(index);
      valueVector.setNull(index);
      childVector.setNull(index);
      index++;
    }

    while (!queue.isEmpty()) {
      RowIterator iterator = queue.poll();
      String currKey = iterator.key();

      if (iterator instanceof SliceRowIterator) {
        ((SliceRowIterator) iterator)
            .transferToTargetVectors(keyVector, valueVector, childVector, index);
      } else {
        writeRowToVectors(iterator, keyVector, valueVector, childVector, index);
      }
      index += 1;
      skipDuplicateKeys(currKey);
      advanceIterator(iterator);
    }

    return index;
  }

  private void writeRowToVectors(
      RowIterator row,
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector,
      int index) {
    if (row.key() != null) {
      keyVector.setSafe(index, row.key().getBytes(StandardCharsets.UTF_8));
    } else {
      keyVector.setNull(index);
    }

    if (row.value() != null) {
      valueVector.setSafe(index, row.value().getBytes(StandardCharsets.UTF_8));
    } else {
      valueVector.setNull(index);
    }

    if (row.child() != null && row.child().path().isPresent()) {
      childVector.setSafe(index, row.child().path().get().getBytes(StandardCharsets.UTF_8));
    } else {
      childVector.setNull(index);
    }
  }

  public List<NodeKeyTableRow> collectRows() {
    List<NodeKeyTableRow> result = Lists.newArrayList();

    while (!queue.isEmpty()) {
      RowIterator current = queue.poll();
      String currentKey = current.key();
      result.add(
          ImmutableNodeKeyTableRow.builder()
              .key(Optional.ofNullable(currentKey))
              .value(Optional.ofNullable(current.value()))
              .child(Optional.ofNullable(current.child()))
              .build());
      skipDuplicateKeys(currentKey);
      advanceIterator(current);
    }
    return result;
  }

  private void skipDuplicateKeys(String key) {
    while (!queue.isEmpty() && Objects.equals(queue.peek().key(), key)) {
      RowIterator iterator = queue.poll();
      advanceIterator(iterator);
    }
  }

  private void advanceIterator(RowIterator iterator) {
    iterator.next();
    if (iterator.hasNext()) {
      queue.add(iterator);
    }
  }

  @Override
  public void close() {
    for (AutoCloseable resource : resources) {
      try {
        resource.close();
      } catch (Exception e) {
        throw new OlympiaRuntimeException(e);
      }
    }
  }
}

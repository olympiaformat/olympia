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
package org.format.olympia.storage.local;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.format.olympia.storage.AtomicOutputStream;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.tree.BasicTreeNode;
import org.format.olympia.tree.ImmutableNodeKeyTableRow;
import org.format.olympia.tree.NodeKeyTableRow;

public class TestUtils {

  private TestUtils() {}

  public static NodeKeyTableRow createRow(String key, String value, String childPath) {
    BasicTreeNode child = null;
    if (childPath != null) {
      child = new BasicTreeNode();
      child.setPath(childPath);
    }
    return ImmutableNodeKeyTableRow.builder()
        .key(Optional.ofNullable(key))
        .value(Optional.ofNullable(value))
        .child(Optional.ofNullable(child))
        .build();
  }

  public static String createArrowFile(
      CatalogStorage storage, String filename, List<NodeKeyTableRow> rows) {
    try (BufferAllocator allocator = storage.getArrowAllocator()) {
      VarCharVector keyVector = new VarCharVector("key", allocator);
      VarCharVector valueVector = new VarCharVector("value", allocator);
      VarCharVector childVector = new VarCharVector("child", allocator);

      keyVector.allocateNew();
      valueVector.allocateNew();
      childVector.allocateNew();

      for (int i = 0; i < rows.size(); i++) {
        writeRowToVectors(rows.get(i), keyVector, valueVector, childVector, i);
      }

      keyVector.setValueCount(rows.size());
      valueVector.setValueCount(rows.size());
      childVector.setValueCount(rows.size());

      VectorSchemaRoot root = VectorSchemaRoot.of(keyVector, valueVector, childVector);
      try (AtomicOutputStream outputStream = storage.startCommit(filename);
          ArrowFileWriter writer = new ArrowFileWriter(root, null, outputStream.channel())) {
        writer.start();
        writer.writeBatch();
        writer.end();
      } finally {
        root.close();
      }
      return filename;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test Arrow file", e);
    }
  }

  private static void writeRowToVectors(
      NodeKeyTableRow row,
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector,
      int index) {
    if (row.key().isPresent()) {
      keyVector.setSafe(index, row.key().get().getBytes(StandardCharsets.UTF_8));
    } else {
      keyVector.setNull(index);
    }
    if (row.value().isPresent()) {
      valueVector.setSafe(index, row.value().get().getBytes(StandardCharsets.UTF_8));
    } else {
      valueVector.setNull(index);
    }
    if (row.child().isPresent() && row.child().get().path().isPresent()) {
      childVector.setSafe(index, row.child().get().path().get().getBytes(StandardCharsets.UTF_8));
    } else {
      childVector.setNull(index);
    }
  }
}

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
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.storage.AtomicOutputStream;
import org.format.olympia.storage.BasicCatalogStorage;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.local.LocalStorageOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestVectorSliceStream {

  @Test
  public void testBasicStreamOperations(@TempDir Path tempDir) throws Exception {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);
    List<NodeKeyTableRow> testData =
        List.of(
            createRow("a", "value-a", null),
            createRow("b", "value-b", "child-b"),
            createRow("c", "value-c", null));

    String testPath = createArrowFile(storage, "test_stream.arrow", testData);
    VectorSlice slice =
        ImmutableVectorSlice.builder().path(testPath).startIndex(0).endIndex(3).build();

    try (VectorSliceStream stream = new VectorSliceStream(storage, slice)) {
      assertStreamContents(stream, testData);
    }
  }

  @Test
  public void testPriorityQueueOrdering(@TempDir Path tempDir) throws Exception {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    // Create two files with interleaved data
    List<NodeKeyTableRow> file1Data =
        List.of(
            createRow("a", "val1", null),
            createRow("c", "val3", "child3"),
            createRow("e", "val5", null));

    List<NodeKeyTableRow> file2Data =
        List.of(
            createRow("b", "val2", "child2"),
            createRow("d", "val4", null),
            createRow("f", "val6", "child6"));

    String path1 = createArrowFile(storage, "stream1.arrow", file1Data);
    String path2 = createArrowFile(storage, "stream2.arrow", file2Data);

    // Create and populate priority queue
    PriorityQueue<VectorSliceStream> queue = new PriorityQueue<>();

    VectorSlice slice1 =
        ImmutableVectorSlice.builder().path(path1).startIndex(0).endIndex(3).build();
    VectorSlice slice2 =
        ImmutableVectorSlice.builder().path(path2).startIndex(0).endIndex(3).build();
    try (VectorSliceStream stream1 = new VectorSliceStream(storage, slice1);
        VectorSliceStream stream2 = new VectorSliceStream(storage, slice2)) {

      queue.add(stream1);
      queue.add(stream2);

      List<String> mergedKeys = Lists.newArrayList();
      while (!queue.isEmpty()) {
        VectorSliceStream current = queue.poll();
        mergedKeys.add(current.getCurrentKey());

        current.advance();
        if (current.hasNext()) {
          queue.add(current);
        }
      }

      assertThat(mergedKeys).isEqualTo(List.of("a", "b", "c", "d", "e", "f"));
    }
  }

  @Test
  public void testPriorityQueueOrderingWithNull(@TempDir Path tempDir) throws Exception {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    List<NodeKeyTableRow> file1Data =
        List.of(createRow("c", "val3", null), createRow("e", "val5", null));

    List<NodeKeyTableRow> file2Data =
        List.of(
            createRow(null, null, "leftmost-child-2"),
            createRow("b", "val2", null),
            createRow("d", "val4", null));

    List<NodeKeyTableRow> file3Data =
        List.of(createRow("a", "val1", null), createRow("f", "val6", null));

    String path1 = createArrowFile(storage, "stream1.arrow", file1Data);
    String path2 = createArrowFile(storage, "stream2.arrow", file2Data);
    String path3 = createArrowFile(storage, "stream3.arrow", file3Data);

    PriorityQueue<VectorSliceStream> queue = new PriorityQueue<>();
    VectorSlice slice1 =
        ImmutableVectorSlice.builder().path(path1).startIndex(0).endIndex(3).build();
    VectorSlice slice2 =
        ImmutableVectorSlice.builder().path(path2).startIndex(0).endIndex(3).build();
    VectorSlice slice3 =
        ImmutableVectorSlice.builder().path(path3).startIndex(0).endIndex(2).build();

    // Test merging streams
    try (VectorSliceStream stream1 = new VectorSliceStream(storage, slice1);
        VectorSliceStream stream2 = new VectorSliceStream(storage, slice2);
        VectorSliceStream stream3 = new VectorSliceStream(storage, slice3)) {

      queue.add(stream1);
      queue.add(stream2);
      queue.add(stream3);

      VectorSliceStream current = queue.poll();
      assertThat(current.isCurrentKeyNull()).isTrue();
      assertThat(current.getCurrentChildPath()).isEqualTo("leftmost-child-2");

      current.advance();
      queue.add(current);

      List<String> expectedKeys = List.of("a", "b", "c", "d", "e", "f");
      List<String> actualKeys = Lists.newArrayList();

      while (!queue.isEmpty()) {
        current = queue.poll();
        String key = current.getCurrentKey();
        if (key != null) {
          actualKeys.add(key);
        }

        current.advance();
        if (current.hasNext()) {
          queue.add(current);
        }
      }

      assertThat(actualKeys).isEqualTo(expectedKeys);
    }
  }

  private NodeKeyTableRow createRow(String key, String value, String childPath) {
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

  private String createArrowFile(
      CatalogStorage storage, String filename, List<NodeKeyTableRow> rows) {
    try (BufferAllocator allocator = storage.getArrowAllocator()) {
      VarCharVector keyVector = new VarCharVector("key", allocator);
      VarCharVector valueVector = new VarCharVector("value", allocator);
      VarCharVector childVector = new VarCharVector("child", allocator);

      keyVector.allocateNew();
      valueVector.allocateNew();
      childVector.allocateNew();

      for (int i = 0; i < rows.size(); i++) {
        NodeKeyTableRow row = rows.get(i);
        if (row.key().isPresent()) {
          keyVector.setSafe(i, row.key().get().getBytes(StandardCharsets.UTF_8));
        } else {
          keyVector.setNull(i);
        }
        if (row.value().isPresent()) {
          valueVector.setSafe(i, row.value().get().getBytes(StandardCharsets.UTF_8));
        } else {
          valueVector.setNull(i);
        }
        if (row.child().isPresent() && row.child().get().path().isPresent()) {
          childVector.setSafe(i, row.child().get().path().get().getBytes(StandardCharsets.UTF_8));
        } else {
          childVector.setNull(i);
        }
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

  private void assertStreamContents(VectorSliceStream stream, List<NodeKeyTableRow> expectedRows) {
    for (NodeKeyTableRow expected : expectedRows) {
      assertThat(stream.getCurrentKey()).isEqualTo(expected.key().orElse(null));
      assertThat(stream.getCurrentValue()).isEqualTo(expected.value().orElse(null));
      assertThat(stream.getCurrentChildPath())
          .isEqualTo(expected.child().flatMap(TreeNode::path).orElse(null));
      stream.advance();
    }
    assertThat(stream.hasNext()).isFalse();
  }
}

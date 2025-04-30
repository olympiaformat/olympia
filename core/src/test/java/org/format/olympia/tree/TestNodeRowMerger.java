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

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.format.olympia.storage.BasicCatalogStorage;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.local.LocalStorageOps;
import org.format.olympia.storage.local.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestNodeRowMerger {

  @Test
  public void testNodeRowMergerWithSlicesAndPending(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    List<NodeKeyTableRow> slice1 =
        List.of(
            TestUtils.createRow(null, null, "c0"),
            TestUtils.createRow("a", "val-a", "c1"),
            TestUtils.createRow("c", "val-c", "c2"),
            TestUtils.createRow("e", "val-e", "c3"));

    List<NodeKeyTableRow> slice2 =
        List.of(TestUtils.createRow("g", "val-g", "c4"), TestUtils.createRow("i", "val-i", "c5"));

    String file1 = TestUtils.createArrowFile(storage, "slice1", slice1);
    String file2 = TestUtils.createArrowFile(storage, "slice2", slice2);

    // Setup node
    BasicTreeNode node = new BasicTreeNode();
    node.addVectorSlice(file1, 0, 3);
    node.addVectorSlice(file2, 0, 1);

    // pending changes
    node.addInMemoryChange("b", "val-b", null);
    node.addInMemoryChange("f", "val-f", null);

    // updated
    BasicTreeNode c2 = new BasicTreeNode();
    node.setPath("c2");
    node.addInMemoryChange("c", "val-c-updated", c2);

    NodeRowMerger merger = new NodeRowMerger(storage, node);
    List<NodeKeyTableRow> rows = merger.collectRows();

    assertThat(rows).hasSize(8); // includes leftChild
    assertThat(rows.get(0).child()).isPresent();
    assertThat(rows.get(0).child().get().path()).contains("c0");

    List<String> keys = rows.stream().map(r -> r.key().orElse(null)).collect(Collectors.toList());
    assertThat(keys).containsExactly(null, "a", "b", "c", "e", "f", "g", "i");

    // validate c value is overwritten
    assertThat(rows.get(3).value()).isPresent();
    assertThat(rows.get(3).value().get()).isEqualTo("val-c-updated");
  }

  @Test
  public void testFindMiddleRow(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    List<NodeKeyTableRow> slice =
        List.of(
            TestUtils.createRow(null, null, "c0"),
            TestUtils.createRow("a", "val-a", "c1"),
            TestUtils.createRow("b", "val-b", "c2"),
            TestUtils.createRow("c", "val-c", "c3"),
            TestUtils.createRow("d", "val-d", "c4"),
            TestUtils.createRow("e", "val-e", "c5"),
            TestUtils.createRow("f", "val-f", "c6"),
            TestUtils.createRow("g", "val-g", "c7"));

    String file = TestUtils.createArrowFile(storage, "test-middle-row", slice);

    BasicTreeNode node = new BasicTreeNode();
    node.addVectorSlice(file, 0, 7);
    NodeRowMerger merger = new NodeRowMerger(storage, node);

    NodeKeyTableRow middle = merger.findMiddleRow(8);
    assertThat(middle.key()).contains("d");
    assertThat(middle.value()).contains("val-d");
    assertThat(middle.child()).isPresent();
  }
}

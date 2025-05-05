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

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.format.olympia.FileLocations;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.storage.BasicCatalogStorage;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.local.LocalStorageOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestTreeOperations {

  @Test
  public void testWriteReadRootNodeFile(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    for (int i = 0; i < 10; i++) {
      treeRoot.set("k" + i, "val" + i);
    }
    treeRoot.setPreviousRootNodeFilePath("some/path/to/previous/root");
    treeRoot.setRollbackFromRootNodeFilePath("some/path/to/rollback/from/root");
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");

    File file = tempDir.resolve("testWriteReadRootNodeFile.arrow").toFile();

    TreeOperations.writeRootNodeFile(storage, "testWriteReadRootNodeFile.arrow", treeRoot);
    assertThat(file.exists()).isTrue();

    TreeRoot root = TreeOperations.readRootNodeFile(storage, "testWriteReadRootNodeFile.arrow");
    assertThat(
            TreeOperations.getNodeKeyTable(storage, treeRoot).stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)))
        .isEqualTo(
            TreeOperations.getNodeKeyTable(storage, root).stream()
                .collect(Collectors.toMap(NodeKeyTableRow::key, NodeKeyTableRow::value)));
  }

  @Test
  public void testFindLatestVersion(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRootV0 = new BasicTreeRoot();
    treeRootV0.setCatalogDefFilePath("some/path/to/catalog/def");
    String v0Path = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, v0Path, treeRootV0);

    TreeRoot treeRootV1 = new BasicTreeRoot();
    treeRootV1.setCatalogDefFilePath("some/path/to/catalog/def");
    treeRootV1.setPreviousRootNodeFilePath(v0Path);
    String v1Path = FileLocations.rootNodeFilePath(1);
    TreeOperations.writeRootNodeFile(storage, v1Path, treeRootV1);

    TreeRoot treeRootV2 = new BasicTreeRoot();
    treeRootV2.setCatalogDefFilePath("some/path/to/catalog/def");
    treeRootV2.setPreviousRootNodeFilePath(v1Path);
    String v2Path = FileLocations.rootNodeFilePath(2);
    TreeOperations.writeRootNodeFile(storage, v2Path, treeRootV2);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertThat(root.path().get()).isEqualTo(v2Path);
  }

  @Test
  public void testTreeRootIterable(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRootV0 = new BasicTreeRoot();
    treeRootV0.setCatalogDefFilePath("some/path/to/catalog/def");
    String v0Path = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, v0Path, treeRootV0);

    TreeRoot treeRootV1 = new BasicTreeRoot();
    treeRootV1.setCatalogDefFilePath("some/path/to/catalog/def");
    treeRootV1.setPreviousRootNodeFilePath(v0Path);
    String v1Path = FileLocations.rootNodeFilePath(1);
    TreeOperations.writeRootNodeFile(storage, v1Path, treeRootV1);

    TreeRoot treeRootV2 = new BasicTreeRoot();
    treeRootV2.setCatalogDefFilePath("some/path/to/catalog/def");
    treeRootV2.setPreviousRootNodeFilePath(v1Path);
    String v2Path = FileLocations.rootNodeFilePath(2);
    TreeOperations.writeRootNodeFile(storage, v2Path, treeRootV2);

    Iterator<TreeRoot> roots = TreeOperations.listRoots(storage).iterator();

    assertThat(roots.hasNext()).isTrue();
    TreeRoot root = roots.next();
    assertThat(root.path().get()).isEqualTo(v2Path);
    assertThat(root.previousRootNodeFilePath().get()).isEqualTo(v1Path);

    assertThat(roots.hasNext()).isTrue();
    root = roots.next();
    assertThat(root.path().get()).isEqualTo(v1Path);
    assertThat(root.previousRootNodeFilePath().get()).isEqualTo(v0Path);

    assertThat(roots.hasNext()).isTrue();
    root = roots.next();
    assertThat(root.path().get()).isEqualTo(v0Path);
    assertThat(root.previousRootNodeFilePath().isPresent()).isFalse();

    assertThat(roots.hasNext()).isFalse();
  }

  @Test
  public void testMixedDirtyAndPersistedNodes(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");

    TreeOperations.setValue(storage, treeRoot, "a", "val-a");
    TreeOperations.setValue(storage, treeRoot, "c", "val-c");
    TreeOperations.setValue(storage, treeRoot, "e", "val-e");

    String path = "test-mixed.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);
    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);

    TreeOperations.setValue(storage, loadedRoot, "b", "val-b");
    TreeOperations.setValue(storage, loadedRoot, "d", "val-d");
    TreeOperations.setValue(storage, loadedRoot, "c", "modified-c");

    assertThat(loadedRoot.isDirty()).isTrue();
    assertThat(TreeOperations.searchValue(storage, loadedRoot, "c").get()).isEqualTo("modified-c");

    String newPath = "test-mixed-updated.arrow";
    TreeOperations.writeRootNodeFile(storage, newPath, loadedRoot);
    TreeRoot finalRoot = TreeOperations.readRootNodeFile(storage, newPath);

    List<NodeKeyTableRow> keyTable = TreeOperations.getNodeKeyTable(storage, finalRoot);
    List<String> actualKeys =
        keyTable.stream()
            .map(row -> row.key().orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    assertThat(actualKeys).containsExactly("a", "b", "c", "d", "e");
    assertThat(TreeOperations.searchValue(storage, finalRoot, "c").get()).isEqualTo("modified-c");
  }

  @Test
  public void testKeySortingInNodeKeyTable(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");

    TreeOperations.setValue(storage, treeRoot, "b", "val-b");
    TreeOperations.setValue(storage, treeRoot, "a", "val-a");
    TreeOperations.setValue(storage, treeRoot, "c", "val-c");

    String path = "test-sorting.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);
    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);

    // add pending changes
    TreeOperations.setValue(storage, loadedRoot, "f", "val-f");
    TreeOperations.setValue(storage, loadedRoot, "d", "val-d");
    TreeOperations.setValue(storage, loadedRoot, "e", "val-e");

    List<NodeKeyTableRow> keyTable = TreeOperations.getNodeKeyTable(storage, loadedRoot);
    List<String> actualOrder =
        keyTable.stream()
            .map(nodeKeyTableRow -> nodeKeyTableRow.key().orElse(null))
            .collect(Collectors.toList());

    List<String> expectedOrder = Lists.newArrayList("a", "b", "c", "d", "e", "f");
    assertThat(actualOrder).isEqualTo(expectedOrder);

    for (int i = 1; i < actualOrder.size(); i++) {
      String prevKey = actualOrder.get(i - 1);
      String currentKey = actualOrder.get(i);
      assertThat(currentKey).isGreaterThan(prevKey);
    }
  }

  @Test
  public void testVectorSlicesMergingWithPendingChanges(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");

    TreeOperations.setValue(storage, treeRoot, "a", "val-a");
    TreeOperations.setValue(storage, treeRoot, "c", "val-c");
    TreeOperations.setValue(storage, treeRoot, "e", "val-e");

    String path = "test-vector-slices-merging.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);

    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);

    TreeOperations.setValue(storage, loadedRoot, "b", "val-b");
    TreeOperations.setValue(storage, loadedRoot, "d", "val-d");
    TreeOperations.setValue(storage, loadedRoot, "f", "val-f");

    TreeOperations.setValue(storage, loadedRoot, "c", "val-c-updated");

    String newPath = "test-vector-slices-merging-updated.arrow";
    TreeOperations.writeRootNodeFile(storage, newPath, loadedRoot);

    TreeRoot finalRoot = TreeOperations.readRootNodeFile(storage, newPath);

    List<String> allKeys =
        TreeOperations.getNodeKeyTable(storage, finalRoot).stream()
            .map(row -> row.key().orElse(""))
            .filter(k -> !k.isEmpty())
            .collect(Collectors.toList());

    assertThat(allKeys).containsExactly("a", "b", "c", "d", "e", "f");
    assertThat(TreeOperations.searchValue(storage, finalRoot, "c").get())
        .isEqualTo("val-c-updated");
  }

  @Test
  public void testSplitNodeInMemory(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");
    treeRoot.setOrder(5);

    TreeOperations.setValue(storage, treeRoot, "a", "val-a");
    TreeOperations.setValue(storage, treeRoot, "b", "val-b");
    TreeOperations.setValue(storage, treeRoot, "c", "val-c");
    TreeOperations.setValue(storage, treeRoot, "d", "val-d");
    TreeOperations.setValue(storage, treeRoot, "e", "val-e");

    List<NodeKeyTableRow> rootRows = Lists.newArrayList(treeRoot.pendingChanges().values());
    assertThat(rootRows).size().isEqualTo(1);
    assertThat(rootRows.get(0).child().isPresent()).isTrue(); // Should have right child

    Optional<NodeKeyTableRow> leftChild = treeRoot.getLeftmostChild();
    assertThat(leftChild.isPresent()).isTrue();
    assertThat(leftChild.get().child().isPresent()).isTrue();
    List<NodeKeyTableRow> leftRows = Lists.newArrayList(treeRoot.pendingChanges().values());
    assertThat(leftRows).size().isEqualTo(1);

    List<NodeKeyTableRow> rightRows =
        Lists.newArrayList(rootRows.get(0).child().get().pendingChanges().values());
    assertThat(rightRows).size().isEqualTo(3);

    assertThat(TreeOperations.searchValue(storage, treeRoot, "a").get()).isEqualTo("val-a");
    assertThat(TreeOperations.searchValue(storage, treeRoot, "b").get()).isEqualTo("val-b");
    assertThat(TreeOperations.searchValue(storage, treeRoot, "c").get()).isEqualTo("val-c");
    assertThat(TreeOperations.searchValue(storage, treeRoot, "d").get()).isEqualTo("val-d");
    assertThat(TreeOperations.searchValue(storage, treeRoot, "e").get()).isEqualTo("val-e");
  }

  @Test
  public void testOnDiskBTreeSearch(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");
    treeRoot.setOrder(5);

    TreeOperations.setValue(storage, treeRoot, "e", "val-e");
    TreeOperations.setValue(storage, treeRoot, "j", "val-j");
    TreeOperations.setValue(storage, treeRoot, "k", "val-k");
    TreeOperations.setValue(storage, treeRoot, "m", "val-m");
    TreeOperations.setValue(storage, treeRoot, "p", "val-p");
    TreeOperations.setValue(storage, treeRoot, "r", "val-r");
    TreeOperations.setValue(storage, treeRoot, "t", "val-t");

    String path = "tree-search.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);
    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);

    assertThat(TreeOperations.searchValue(storage, loadedRoot, "e").get()).isEqualTo("val-e");
    assertThat(TreeOperations.searchValue(storage, loadedRoot, "m").get()).isEqualTo("val-m");
    assertThat(TreeOperations.searchValue(storage, loadedRoot, "t").get()).isEqualTo("val-t");
    assertThat(TreeOperations.searchValue(storage, loadedRoot, "f").isPresent()).isFalse();
    assertThat(TreeOperations.searchValue(storage, loadedRoot, "q").isPresent()).isFalse();
  }

  @Test
  public void testVectorSliceSplittingOnUpdate(@TempDir Path tempDir) {
    LocalStorageOps ops = new LocalStorageOps();
    CatalogStorage storage = new BasicCatalogStorage(new LiteralURI("file://" + tempDir), ops);

    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setCatalogDefFilePath("some/path/to/catalog/def");

    TreeOperations.setValue(storage, treeRoot, "a", "val-a");
    TreeOperations.setValue(storage, treeRoot, "c", "val-c");
    TreeOperations.setValue(storage, treeRoot, "e", "val-e");
    TreeOperations.setValue(storage, treeRoot, "g", "val-g");
    TreeOperations.setValue(storage, treeRoot, "i", "val-i");

    String path = "test-vector-splice.arrow";
    TreeOperations.writeRootNodeFile(storage, path, treeRoot);

    TreeRoot loadedRoot = TreeOperations.readRootNodeFile(storage, path);
    TreeOperations.setValue(storage, loadedRoot, "c", "val-c-updated"); // updated
    TreeOperations.setValue(storage, loadedRoot, "k", "val-k"); // new value

    assertThat(loadedRoot.pendingChanges()).containsKey("c");
    assertThat(loadedRoot.pendingChanges().get("c").value().get()).isEqualTo("val-c-updated");
    assertThat(loadedRoot.getVectorSlices()).hasSize(2);
    assertThat(loadedRoot.numKeys()).isEqualTo(treeRoot.numKeys() + 1);
  }
}

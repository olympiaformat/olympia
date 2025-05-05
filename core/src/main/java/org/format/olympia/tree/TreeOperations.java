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

import com.google.protobuf.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.format.olympia.FileLocations;
import org.format.olympia.ObjectDefinitions;
import org.format.olympia.ObjectKeys;
import org.format.olympia.Transaction;
import org.format.olympia.action.Action;
import org.format.olympia.action.AnalyzeActionConflicts;
import org.format.olympia.action.ConflictAnalysisResult;
import org.format.olympia.exception.CommitConflictUnresolvableException;
import org.format.olympia.exception.StorageAtomicSealFailureException;
import org.format.olympia.exception.StorageFileOpenFailureException;
import org.format.olympia.exception.StorageReadFailureException;
import org.format.olympia.exception.StorageWriteFailureException;
import org.format.olympia.proto.actions.ActionDef;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.relocated.com.google.common.base.Strings;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.storage.AtomicOutputStream;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.local.LocalInputStream;
import org.format.olympia.util.FileUtil;
import org.format.olympia.util.ValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TreeOperations.class);

  private static final int NODE_FILE_KEY_COLUMN_INDEX = 0;
  private static final String NODE_FILE_KEY_COLUMN_NAME = "key";

  private static final int NODE_FILE_VALUE_COLUMN_INDEX = 1;
  private static final String NODE_FILE_VALUE_COLUMN_NAME = "value";

  private static final int NODE_FILE_CHILD_POINTER_COLUMN_INDEX = 2;
  private static final String NODE_FILE_CHILD_POINTER_COLUMN_NAME = "pnode";

  private TreeOperations() {}

  public static TreeRoot readRootNodeFile(CatalogStorage storage, String path) {
    BufferAllocator allocator = storage.getArrowAllocator();
    try (LocalInputStream stream = storage.startReadLocal(path)) {
      return readRootNodeFile(stream, allocator, path);
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  public static BasicTreeNode readChildNodeFile(CatalogStorage storage, String path) {
    BufferAllocator allocator = storage.getArrowAllocator();
    try (LocalInputStream stream = storage.startReadLocal(path)) {
      return readChildNodeFile(stream, allocator, path);
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  private static BasicTreeNode readChildNodeFile(
      LocalInputStream stream, BufferAllocator allocator, String path) {
    BasicTreeNode node = new BasicTreeNode();
    node.setPath(path);
    ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator);
    try {
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        VarCharVector keyVector = (VarCharVector) root.getVector(NODE_FILE_KEY_COLUMN_INDEX);
        VarCharVector valueVector = (VarCharVector) root.getVector(NODE_FILE_VALUE_COLUMN_INDEX);
        int dataStartIndex = -1;
        int numKeys = 0;
        for (int i = 0; i < root.getRowCount(); ++i) {
          if (keyVector.isNull(i)) {
            dataStartIndex = i;
            break;
          }
          Text key = keyVector.getObject(i);
          Text value = valueVector.getObject(i);
          if (ObjectKeys.NUMBER_OF_KEYS.equals(key.toString())) {
            numKeys = Integer.parseInt(value.toString());
          }
        }
        if (numKeys != 0) {
          node.addVectorSlice(path, dataStartIndex, dataStartIndex + numKeys);
        }
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
    return node;
  }

  private static TreeRoot readRootNodeFile(
      LocalInputStream stream, BufferAllocator allocator, String path) {
    TreeRoot treeRoot = new BasicTreeRoot();
    treeRoot.setPath(path);
    ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator);
    try {
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        VarCharVector keyVector = (VarCharVector) root.getVector(NODE_FILE_KEY_COLUMN_INDEX);
        VarCharVector valueVector = (VarCharVector) root.getVector(NODE_FILE_VALUE_COLUMN_INDEX);
        int dataStartIndex = -1;
        int numKeys = 0;
        int numActions = 0;
        for (int i = 0; i < root.getRowCount(); ++i) {
          if (keyVector.isNull(i)) {
            dataStartIndex = i;
            break;
          }
          Text key = keyVector.getObject(i);
          Text value = valueVector.getObject(i);

          if (ObjectKeys.CREATED_AT_MILLIS.equals(key.toString())) {
            treeRoot.setCreatedAtMillis(Long.parseLong(value.toString()));
          } else if (ObjectKeys.CATALOG_DEFINITION.equals(key.toString())) {
            treeRoot.setCatalogDefFilePath(value.toString());
          } else if (ObjectKeys.PREVIOUS_ROOT_NODE.equals(key.toString())) {
            treeRoot.setPreviousRootNodeFilePath(value.toString());
          } else if (ObjectKeys.ROLLBACK_FROM_ROOT_NODE.equals(key.toString())) {
            treeRoot.setRollbackFromRootNodeFilePath(value.toString());
          } else if (ObjectKeys.NUMBER_OF_KEYS.equals(key.toString())) {
            numKeys = Integer.parseInt(value.toString());
          } else if (ObjectKeys.NUMBER_OF_ACTIONS.equals(key.toString())) {
            numActions = Integer.parseInt(value.toString());
          }
        }

        if (numKeys != 0) {
          treeRoot.addVectorSlice(path, dataStartIndex, dataStartIndex + numKeys);
        }
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
    return treeRoot;
  }

  public static void writeRootNodeFile(CatalogStorage storage, String path, TreeRoot root) {
    long createdAtMillis = System.currentTimeMillis();
    if (root.path().isPresent() && !path.equals(root.path().get())) {
      root.setPreviousRootNodeFilePath(root.path().get());
    }
    root.setPath(path);
    serializeTreeNode(storage, root, createdAtMillis);
  }

  public static void serializeTreeNode(
      CatalogStorage storage, TreeNode node, Long createdAtMillis) {
    node.getLeftmostChild()
        .flatMap(NodeKeyTableRow::child)
        .ifPresent(child -> serializeTreeNode(storage, child, createdAtMillis));
    for (NodeKeyTableRow row : node.pendingChanges().values()) {
      row.child()
          .ifPresent(
              child -> {
                if (child.isDirty()) {
                  serializeTreeNode(storage, child, createdAtMillis);
                }
              });
    }
    if (node.path().isPresent()) {
      try (AtomicOutputStream stream = storage.startCommit(node.path().get())) {
        writeNodeFile(storage, stream, node, createdAtMillis);
      } catch (IOException e) {
        throw new StorageAtomicSealFailureException(e);
      }
    }
  }

  private static int writeRootSystemRows(
      TreeRoot node,
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector) {
    int index = 2;

    keyVector.setSafe(index, ObjectKeys.CATALOG_DEFINITION_BYTES);
    valueVector.setSafe(index, node.catalogDefFilePath().getBytes(StandardCharsets.UTF_8));
    childVector.setNull(index++);

    if (node.previousRootNodeFilePath().isPresent()) {
      keyVector.setSafe(index, ObjectKeys.PREVIOUS_ROOT_NODE_BYTES);
      valueVector.setSafe(
          index, node.previousRootNodeFilePath().get().getBytes(StandardCharsets.UTF_8));
      childVector.setNull(index++);
    }

    if (node.rollbackFromRootNodeFilePath().isPresent()) {
      keyVector.setSafe(index, ObjectKeys.ROLLBACK_FROM_ROOT_NODE_BYTES);
      valueVector.setSafe(
          index, node.rollbackFromRootNodeFilePath().get().getBytes(StandardCharsets.UTF_8));
      childVector.setNull(index++);
    }

    return index;
  }

  private static void writeNodeTableRow(
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

  private static void writeActionRow(
      Action action,
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector,
      int index) {
    keyVector.setSafe(index, action.objectKey().getBytes(StandardCharsets.UTF_8));

    ActionDef actionDef =
        ActionDef.newBuilder()
            .setType(action.type())
            .setDef(action.def().map(Message::toByteString).orElse(null))
            .build();
    valueVector.setSafe(index, actionDef.toByteArray());
    childVector.setNull(index);
  }

  private static void writeNodeFile(
      CatalogStorage storage, AtomicOutputStream stream, TreeNode node, long createdAtMillis) {
    BufferAllocator allocator = storage.getArrowAllocator();
    VarCharVector keyVector = new VarCharVector(NODE_FILE_KEY_COLUMN_NAME, allocator);
    VarCharVector valueVector = new VarCharVector(NODE_FILE_VALUE_COLUMN_NAME, allocator);
    VarCharVector childVector = new VarCharVector(NODE_FILE_CHILD_POINTER_COLUMN_NAME, allocator);
    keyVector.allocateNew();
    valueVector.allocateNew();
    childVector.allocateNew();
    int index = 0;

    keyVector.setSafe(index, ObjectKeys.CREATED_AT_MILLIS_BYTES);
    valueVector.setSafe(index, Long.toString(createdAtMillis).getBytes(StandardCharsets.UTF_8));
    childVector.setNull(index++);

    keyVector.setSafe(index, ObjectKeys.NUMBER_OF_KEYS_BYTES);
    valueVector.setSafe(index, Integer.toString(node.numKeys()).getBytes(StandardCharsets.UTF_8));
    childVector.setNull(index++);

    if (node instanceof TreeRoot) {
      index = writeRootSystemRows((TreeRoot) node, keyVector, valueVector, childVector);
    }

    try (NodeRowMerger merger = new NodeRowMerger(storage, node)) {
      index = merger.writeToVectors(keyVector, valueVector, childVector, index);
    }

    //    if (node instanceof TreeRoot) {
    //      for (Action action : node.actions()) {
    //        writeActionRow(action, keyVector, valueVector, childVector, index++);
    //      }
    //    }

    keyVector.setValueCount(index);
    valueVector.setValueCount(index);
    childVector.setValueCount(index);
    List<Field> fields =
        Lists.newArrayList(keyVector.getField(), valueVector.getField(), childVector.getField());
    List<FieldVector> vectors = Lists.newArrayList(keyVector, valueVector, childVector);
    VectorSchemaRoot schema = new VectorSchemaRoot(fields, vectors);
    try (ArrowFileWriter writer = new ArrowFileWriter(schema, null, stream.channel())) {
      writer.start();
      writer.writeBatch();
      writer.end();
    } catch (IOException e) {
      throw new StorageWriteFailureException(e);
    }
  }

  public static void tryWriteLatestVersionFile(CatalogStorage storage, long rootVersion) {
    try (OutputStream stream = storage.startOverwrite(FileLocations.LATEST_VERSION_FILE_PATH)) {
      stream.write(Long.toString(rootVersion).getBytes(StandardCharsets.UTF_8));
    } catch (StorageWriteFailureException | IOException e) {
      LOG.error("Failed to write root node version hint file", e);
    }
  }

  public static long findVersionFromRootNode(TreeNode node) {
    ValidationUtil.checkArgument(
        node.path().isPresent(), "Cannot derive version from a node that is not persisted");
    ValidationUtil.checkArgument(
        FileLocations.isRootNodeFilePath(node.path().get()),
        "Cannot derive version from a non-root node");
    return FileLocations.versionFromNodeFilePath(node.path().get());
  }

  public static CatalogDef findCatalogDef(CatalogStorage storage, TreeRoot node) {
    return ObjectDefinitions.readCatalogDef(storage, node.catalogDefFilePath());
  }

  public static TreeRoot findLatestRoot(CatalogStorage storage) {
    // TODO: this should be improved by adding a minimum version file
    //  so that versions that are too old can be deleted in storage.
    //  Unlike the version hint file, this minimum file must exist if the minimum version is greater
    // than zero,
    //  and the creation of this file should require a global lock
    long latestVersion = 0;
    try (InputStream versionHintStream =
        storage.startRead(FileLocations.LATEST_VERSION_FILE_PATH)) {
      String versionHintText = FileUtil.readToString(versionHintStream);
      latestVersion = Long.parseLong(versionHintText);
    } catch (StorageFileOpenFailureException | StorageReadFailureException | IOException e) {
      LOG.warn("Failed to read latest version hint file, fallback to search from version 0", e);
    }

    String rootNodeFilePath = FileLocations.rootNodeFilePath(latestVersion);

    while (true) {
      long nextVersion = latestVersion + 1;
      String nextRootNodeFilePath = FileLocations.rootNodeFilePath(latestVersion);
      if (!storage.exists(nextRootNodeFilePath)) {
        break;
      }
      rootNodeFilePath = nextRootNodeFilePath;
      latestVersion = nextVersion;
    }

    TreeRoot root = TreeOperations.readRootNodeFile(storage, rootNodeFilePath);
    return root;
  }

  public static Optional<TreeRoot> findRootForVersion(CatalogStorage storage, long version) {
    TreeRoot latest = findLatestRoot(storage);
    ValidationUtil.checkArgument(
        latest.path().isPresent(), "latest tree root must be persisted with a path in storage");
    long latestVersion = FileLocations.versionFromNodeFilePath(latest.path().get());
    ValidationUtil.checkArgument(
        version <= latestVersion,
        "Version %d must not be higher than latest version %d",
        version,
        latestVersion);

    TreeRoot current = latest;
    while (current.previousRootNodeFilePath().isPresent()) {
      TreeRoot previous =
          TreeOperations.readRootNodeFile(storage, current.previousRootNodeFilePath().get());
      if (version == FileLocations.versionFromNodeFilePath(previous.path().get())) {
        return Optional.of(previous);
      }
      current = previous;
    }

    return Optional.empty();
  }

  public static TreeRoot findRootBeforeTimestamp(CatalogStorage storage, long timestampMillis) {
    TreeRoot latest = findLatestRoot(storage);
    ValidationUtil.checkArgument(
        latest.createdAtMillis().isPresent(),
        "latest tree root must be persisted with a timestamp in storage");
    long latestCreatedAtMillis = latest.createdAtMillis().get();
    ValidationUtil.checkArgument(
        timestampMillis <= latestCreatedAtMillis,
        "Timestamp %d must not be higher than latest created version %d",
        timestampMillis,
        latestCreatedAtMillis);

    TreeRoot current = latest;
    while (current.previousRootNodeFilePath().isPresent()) {
      TreeRoot previous =
          TreeOperations.readRootNodeFile(storage, current.previousRootNodeFilePath().get());
      ValidationUtil.checkArgument(
          previous.createdAtMillis().isPresent(),
          "Tree root must be persisted with a timestamp in storage");
      if (timestampMillis > previous.createdAtMillis().get()) {
        return previous;
      }
      current = previous;
    }

    return current;
  }

  public static List<NodeKeyTableRow> getNodeKeyTable(CatalogStorage storage, TreeNode node) {
    List<NodeKeyTableRow> result = Lists.newArrayList();
    inOrderTraversal(storage, node, result);
    return result;
  }

  private static void inOrderTraversal(
      CatalogStorage storage, TreeNode node, List<NodeKeyTableRow> result) {
    List<NodeKeyTableRow> nodeRows =
        node.isDirty()
            ? materializeDirtyNode(storage, node)
            : materializePersistedNode(storage, node);
    for (NodeKeyTableRow row : nodeRows) {
      if (row.key().isEmpty() && row.child().isPresent()) {
        inOrderTraversal(storage, row.child().get(), result);
      } else if (row.key().isPresent()) {
        // null is deleted
        if (row.value().isPresent()) {
          result.add(row);
        }
        if (row.child().isPresent()) {
          inOrderTraversal(storage, row.child().get(), result);
        }
      }
    }
  }

  private static List<NodeKeyTableRow> materializePersistedNode(
      CatalogStorage storage, TreeNode node) {
    if (node.path().isEmpty()) {
      return List.of();
    }

    List<NodeKeyTableRow> result = Lists.newArrayList();
    try (LocalInputStream stream = storage.startReadLocal(node.path().get());
        BufferAllocator allocator = storage.getArrowAllocator();
        ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator)) {
      for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
        reader.loadRecordBatch(arrowBlock);
        VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
        VarCharVector keysVector = (VarCharVector) schemaRoot.getVector(NODE_FILE_KEY_COLUMN_INDEX);
        VarCharVector valuesVector =
            (VarCharVector) schemaRoot.getVector(NODE_FILE_VALUE_COLUMN_INDEX);
        VarCharVector childVector =
            (VarCharVector) schemaRoot.getVector(NODE_FILE_CHILD_POINTER_COLUMN_INDEX);
        for (int i = 0; i < keysVector.getValueCount(); i++) {
          Optional<String> key = Optional.empty();
          if (!keysVector.isNull(i)) {
            String keyStr = new String(keysVector.get(i), StandardCharsets.UTF_8);
            if (!TreeUtil.isSystemKey(keyStr)) {
              key = Optional.of(keyStr);
            } else {
              continue;
            }
          }

          Optional<String> value = Optional.empty();
          if (!valuesVector.isNull(i)) {
            value = Optional.of(new String(valuesVector.get(i), StandardCharsets.UTF_8));
          }

          Optional<TreeNode> childNode = Optional.empty();
          if (!childVector.isNull(i)) {
            String childPath = new String(childVector.get(i), StandardCharsets.UTF_8);
            BasicTreeNode child = new BasicTreeNode();
            child.setPath(childPath);
            childNode = Optional.of(child);
          }

          result.add(
              ImmutableNodeKeyTableRow.builder().key(key).value(value).child(childNode).build());
        }
      }
      return result;
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
  }

  public static Iterable<TreeRoot> listRoots(CatalogStorage storage) {
    return new TreeRootIterable(storage, findLatestRoot(storage));
  }

  private static class TreeRootIterable implements Iterable<TreeRoot> {

    private final CatalogStorage storage;
    private final TreeRoot latest;

    TreeRootIterable(CatalogStorage storage, TreeRoot latest) {
      this.storage = storage;
      this.latest = latest;
    }

    @Override
    public Iterator<TreeRoot> iterator() {
      return new CatalogVersionIterator(storage, latest);
    }
  }

  private static class CatalogVersionIterator implements Iterator<TreeRoot> {

    private final CatalogStorage storage;
    private final TreeRoot latest;
    private TreeRoot current;

    CatalogVersionIterator(CatalogStorage storage, TreeRoot latest) {
      this.storage = storage;
      this.latest = latest;
      this.current = null;
    }

    @Override
    public boolean hasNext() {
      return current == null || current.previousRootNodeFilePath().isPresent();
    }

    @Override
    public TreeRoot next() {
      if (current == null) {
        this.current = latest;
      } else {
        this.current =
            TreeOperations.readRootNodeFile(storage, current.previousRootNodeFilePath().get());
      }
      return current;
    }
  }

  public static Optional<String> searchValue(
      CatalogStorage storage, TreeNode startNode, String key) {
    TreeNode currentNode = startNode;
    NodeSearchResult result;
    while (currentNode != null) {
      result = searchInNode(storage, currentNode, key);
      if (result.key().isPresent()
          && result.key().get().equals(key)
          && result.value().isPresent()) {
        return result.value();
      }
      currentNode = result.nodePointer().orElse(null);
    }
    return Optional.empty();
  }

  public static void setValue(CatalogStorage storage, TreeRoot root, String key, String value) {
    List<NodeSearchResult> path = findPathToChild(storage, root, key);
    NodeSearchResult leafResult = path.get(path.size() - 1);
    TreeNode leafNode = leafResult.sourceNode().orElse(null);
    ValidationUtil.checkState(leafNode != null, "Path search didn't return a valid node");
    boolean wasClean = !leafNode.isDirty();

    if (leafResult.key().isPresent()
        && leafResult.key().get().equals(key)
        && leafResult.location().isPresent()) {
      splitVectorSliceAtIndex(leafNode, leafResult.location().get());
    }

    leafNode.set(key, value);
    if (wasClean && !(leafNode instanceof TreeRoot)) {
      propagateChildChangesUpward(path);
    }

    if (needsSplit(leafNode, root.order())) {
      splitNode(storage, path, root.order());
    }
  }

  private static void splitVectorSliceAtIndex(TreeNode leafNode, VectorSliceLocation location) {
    VectorSlice slice = location.slice();
    int index = location.index();
    leafNode.getVectorSlices().remove(slice);
    if (slice.startIndex() <= index - 1) {
      leafNode
          .getVectorSlices()
          .add(
              ImmutableVectorSlice.copyOf(slice)
                  .withEndIndex(index - 1)
                  .withStartsWithLeftChild(slice.startsWithLeftChild()));
    }

    if (index + 1 <= slice.endIndex()) {
      leafNode
          .getVectorSlices()
          .add(
              ImmutableVectorSlice.copyOf(slice)
                  .withStartIndex(index + 1)
                  .withStartsWithLeftChild(false));
    }
  }

  private static void propagateChildChangesUpward(List<NodeSearchResult> path) {
    for (int i = path.size() - 1; i > 0; i--) {
      NodeSearchResult childResult = path.get(i);
      NodeSearchResult parentResult = path.get(i - 1);
      TreeNode parent = parentResult.sourceNode().get();
      TreeNode child = childResult.sourceNode().get();

      if (child.isDirty()) {
        String key = parentResult.key().orElse(null);
        String value = parentResult.value().orElse(null);
        if (key != null && childResult.location().isPresent()) {
          splitVectorSliceAtIndex(parent, childResult.location().get());
        }
        parent.addInMemoryChange(key, value, child);
      }
    }
  }

  private static boolean needsSplit(TreeNode node, int order) {
    return node.numKeys() >= order - 1;
  }

  public static void removeKey(CatalogStorage storage, TreeRoot root, String key) {
    // TODO: implement actual algorithm
    setValue(storage, root, key, null);
  }

  private static List<NodeSearchResult> findPathToChild(
      CatalogStorage storage, TreeNode startNode, String key) {
    List<NodeSearchResult> path = Lists.newArrayList();
    TreeNode currentNode = startNode;

    while (currentNode != null) {
      NodeSearchResult result = searchInNode(storage, currentNode, key);
      path.add(ImmutableNodeSearchResult.copyOf(result).withSourceNode(currentNode));
      if (result.key().isPresent() && result.key().get().equals(key)
          || result.nodePointer().isEmpty()) {
        break;
      }
      currentNode = result.nodePointer().orElse(null);
    }
    return path;
  }

  private static NodeSearchResult searchInNode(CatalogStorage storage, TreeNode node, String key) {
    NodeSearchResult memoryResult = node.search(key);

    // If we found an exact match in pending changes, return it
    if (memoryResult.key().isPresent() && memoryResult.key().get().equals(key)) {
      return memoryResult;
    }

    NodeSearchResult bestSliceResult = null;
    for (VectorSlice slice : node.getVectorSlices()) {
      NodeSearchResult result = searchInPersistedData(storage, slice, key);

      // Handle exact match in slice
      if (result.key().isPresent() && result.key().get().equals(key)) {
        if (!node.pendingChanges().containsKey(key)) {
          return result; // Return exact match if not overwritten
        }
        continue; // Skip if overwritten by pending changes
      }

      // Skip if no valid pointer
      if (result.nodePointer().isEmpty()) {
        continue;
      }

      bestSliceResult = chooseBetterFloorKey(bestSliceResult, result);
    }

    // if no slice result
    if (bestSliceResult == null) {
      return memoryResult;
    }

    // pending changes returned no pointer use slice result
    if (memoryResult.nodePointer().isEmpty() && bestSliceResult.nodePointer().isPresent()) {
      return bestSliceResult;
    }

    return chooseBetterFloorKey(memoryResult, bestSliceResult);
  }

  private static NodeSearchResult chooseBetterFloorKey(
      NodeSearchResult first, NodeSearchResult second) {
    if (first == null) {
      return second;
    } else if (second == null) {
      return first;
    } else if (first.key().isPresent() && second.key().isPresent()) {
      return (first.key().get().compareTo(second.key().get()) >= 0) ? first : second;
    }
    return first.key().isPresent() ? first : second.key().isPresent() ? second : first;
  }

  public static NodeSearchResult searchInPersistedData(
      CatalogStorage storage, VectorSlice slice, String key) {
    try (LocalInputStream stream = storage.startReadLocal(slice.path());
        BufferAllocator allocator = storage.getArrowAllocator()) {

      VarCharVector searchKeyVector = TreeUtil.createSearchKey(allocator, key);
      try (ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator)) {
        for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
          reader.loadRecordBatch(arrowBlock);
          VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
          VarCharVector keyVector =
              (VarCharVector) schemaRoot.getVector(NODE_FILE_KEY_COLUMN_INDEX);
          VarCharVector valueVector =
              (VarCharVector) schemaRoot.getVector(NODE_FILE_VALUE_COLUMN_INDEX);
          VarCharVector childVector =
              (VarCharVector) schemaRoot.getVector(NODE_FILE_CHILD_POINTER_COLUMN_INDEX);

          int searchEnd = Math.min(slice.endIndex(), keyVector.getValueCount() - 1);
          if (slice.startIndex() > searchEnd) {
            continue;
          }
          VectorValueComparator<VarCharVector> comparator = new NodeVarCharComparator();
          comparator.attachVector(keyVector);
          int result =
              TreeUtil.vectorBinarySearch(
                  keyVector, comparator, searchKeyVector, NODE_FILE_KEY_COLUMN_INDEX, 0, searchEnd);

          if (result >= 0) {
            return createNodeSearchResult(
                storage, keyVector, valueVector, childVector, slice, result);
          } else {
            int floorIndex = -result - 2;
            if (floorIndex >= slice.startIndex() && floorIndex <= searchEnd) {
              return createNodeSearchResult(
                  storage, keyVector, valueVector, childVector, slice, floorIndex);
            }
          }
        }
      } finally {
        searchKeyVector.close();
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }

    return ImmutableNodeSearchResult.builder()
        .value(Optional.empty())
        .nodePointer(Optional.empty())
        .build();
  }

  public static void splitNode(CatalogStorage storage, List<NodeSearchResult> path, int order) {
    TreeNode nodeToSplit = path.get(path.size() - 1).sourceNode().orElse(null);

    NodeKeyTableRow middleRow = findPivotRow(storage, nodeToSplit, order);
    String middleKey = middleRow.key().orElse(null);
    String middleValue = middleRow.value().orElse(null);
    ValidationUtil.checkState(
        !Strings.isNullOrEmpty(middleKey),
        "Middle key is null during split, the node state might be corrupted");

    // Create left and right nodes
    BasicTreeNode leftNode = new BasicTreeNode();
    BasicTreeNode rightNode = new BasicTreeNode();
    leftNode.setPath(FileLocations.newNodeFilePath());
    rightNode.setPath(FileLocations.newNodeFilePath());

    // binary search the vector slices to split slices between before and after midkey
    for (VectorSlice slice : nodeToSplit.getVectorSlices()) {
      SplitVectorResult splitResult = splitVectorSlices(storage, slice, middleKey);
      if (splitResult.leftSlice().isPresent()) {
        leftNode.getVectorSlices().add(splitResult.leftSlice().get());
      }
      if (splitResult.rightSlice().isPresent()) {
        rightNode.getVectorSlices().add(splitResult.rightSlice().get());
      }
    }

    // if nodeToSplit has a left child that goes to the leftNode
    if (nodeToSplit.getLeftmostChild().isPresent()) {
      leftNode.setLeftmostChild(nodeToSplit.getLeftmostChild().get());
    }

    // if nodeToSplit has a child that becomes the left child for rightNode
    if (middleRow.child().isPresent()) {
      rightNode.setLeftmostChild(
          ImmutableNodeKeyTableRow.builder().child(middleRow.child().get()).build());
    }

    // move pending changes with the nodes
    for (NodeKeyTableRow pendingChange : nodeToSplit.pendingChanges().values()) {
      String currentKey = pendingChange.key().get();
      int comparison = currentKey.compareTo(middleKey);
      if (comparison > 0) {
        rightNode.pendingChanges().put(currentKey, pendingChange);
      } else if (comparison < 0) {
        leftNode.pendingChanges().put(currentKey, pendingChange);
      }
    }

    if (nodeToSplit instanceof TreeRoot) {
      nodeToSplit.clear();
      nodeToSplit.addInMemoryChange(null, null, leftNode);
      nodeToSplit.addInMemoryChange(middleKey, middleValue, rightNode);
    } else {
      NodeSearchResult parentSearch = path.get(path.size() - 2);
      TreeNode parent = parentSearch.sourceNode().get();
      String floorKey = parentSearch.key().orElse(null);
      String floorValue = parentSearch.value().orElse(null);

      parent.addInMemoryChange(floorKey, floorValue, leftNode);
      parent.addInMemoryChange(middleKey, middleValue, rightNode);

      if (needsSplit(parent, order)) {
        splitNode(storage, path.subList(0, path.size() - 1), order);
      }
    }
  }

  private static NodeKeyTableRow findPivotRow(CatalogStorage storage, TreeNode node, int order) {
    try (NodeRowMerger merger = new NodeRowMerger(storage, node)) {
      return merger.findMiddleRow(order);
    }
  }

  private static SplitVectorResult splitVectorSlices(
      CatalogStorage storage, VectorSlice slice, String key) {
    try (LocalInputStream stream = storage.startReadLocal(slice.path());
        BufferAllocator allocator = storage.getArrowAllocator()) {

      VarCharVector searchKeyVector = TreeUtil.createSearchKey(allocator, key);
      try (ArrowFileReader reader = new ArrowFileReader(stream.channel(), allocator)) {
        for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
          reader.loadRecordBatch(arrowBlock);
          VectorSchemaRoot schemaRoot = reader.getVectorSchemaRoot();
          VarCharVector keyVector =
              (VarCharVector) schemaRoot.getVector(NODE_FILE_KEY_COLUMN_INDEX);

          int searchEnd = Math.min(slice.endIndex(), keyVector.getValueCount() - 1);
          if (slice.startIndex() > searchEnd) {
            continue;
          }
          VectorValueComparator<VarCharVector> comparator = new NodeVarCharComparator();
          comparator.attachVector(keyVector);
          int result =
              TreeUtil.vectorBinarySearch(
                  keyVector,
                  comparator,
                  searchKeyVector,
                  NODE_FILE_KEY_COLUMN_INDEX,
                  slice.startIndex(),
                  searchEnd);
          ImmutableSplitVectorResult.Builder builder = ImmutableSplitVectorResult.builder();

          if (result >= 0) {
            if (slice.startIndex() <= result - 1) {
              builder.leftSlice(
                  ImmutableVectorSlice.copyOf(slice)
                      .withStartsWithLeftChild(slice.startsWithLeftChild())
                      .withEndIndex(result - 1));
            }
            if (result + 1 <= searchEnd) {
              builder.rightSlice(
                  ImmutableVectorSlice.copyOf(slice)
                      .withStartsWithLeftChild(false)
                      .withStartIndex(result + 1));
            }
          } else {
            int floorIndex = -result - 2;

            if (slice.startIndex() <= floorIndex) {
              builder.leftSlice(
                  ImmutableVectorSlice.copyOf(slice)
                      .withStartsWithLeftChild(slice.startsWithLeftChild())
                      .withEndIndex(floorIndex));
            }
            if (floorIndex + 1 <= searchEnd) {
              builder.rightSlice(
                  ImmutableVectorSlice.copyOf(slice)
                      .withStartsWithLeftChild(false)
                      .withStartIndex(floorIndex + 1));
            }
          }
          return builder.build();
        }
      } finally {
        searchKeyVector.close();
      }
    } catch (IOException e) {
      throw new StorageReadFailureException(e);
    }
    return ImmutableSplitVectorResult.builder().build();
  }

  private static List<NodeKeyTableRow> materializeDirtyNode(CatalogStorage storage, TreeNode node) {
    try (NodeRowMerger merger = new NodeRowMerger(storage, node)) {
      return merger.collectRows();
    }
  }

  private static NodeSearchResult createNodeSearchResult(
      CatalogStorage storage,
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector,
      VectorSlice slice,
      int index) {
    String key =
        keyVector.isNull(index) ? null : new String(keyVector.get(index), StandardCharsets.UTF_8);
    String value =
        valueVector.isNull(index)
            ? null
            : new String(valueVector.get(index), StandardCharsets.UTF_8);
    String childPath =
        childVector.isNull(index)
            ? null
            : new String(childVector.get(index), StandardCharsets.UTF_8);

    TreeNode child = null;
    if (!Strings.isNullOrEmpty(childPath)) {
      child = readChildNodeFile(storage, childPath);
    }
    VectorSliceLocation location =
        ImmutableVectorSliceLocation.builder().slice(slice).index(index).build();
    return ImmutableNodeSearchResult.builder()
        .key(Optional.ofNullable(key))
        .value(Optional.ofNullable(value))
        .nodePointer(Optional.ofNullable(child))
        .location(location)
        .build();
  }

  public static void resolveConflictingRootsInTransaction(
      CatalogStorage storage, Transaction transaction) {
    for (Action baseAction : transaction.beginningRoot().actions()) {
      for (Action newAction : transaction.runningRoot().actions()) {
        ConflictAnalysisResult analysisResult =
            AnalyzeActionConflicts.analyze(newAction, baseAction, transaction.isolationLevel());
        if (analysisResult.hasConflict()) {
          if (!analysisResult.canResolveConflict()) {
            throw new CommitConflictUnresolvableException(
                "Cannot resolve conflict between %s and %s", baseAction, newAction);
          } else {
            Optional<String> newBaseValue =
                searchValue(storage, transaction.beginningRoot(), newAction.objectKey());
            ValidationUtil.checkArgument(
                newBaseValue.isPresent(),
                "Cannot find object %s in beginning root %s",
                baseAction.objectKey(),
                transaction.beginningRoot().path());
            // TODO: apply change to get new key values and set the new value using the new key
          }
        }
      }
    }
  }
}

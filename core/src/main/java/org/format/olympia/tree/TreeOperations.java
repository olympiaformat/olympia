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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
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
import org.format.olympia.exception.OlympiaRuntimeException;
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
          node.addVectorSlice(path, dataStartIndex, dataStartIndex + numKeys - 1);
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
    List<NodeKeyTableRow> nodeRows = materializeDirtyNode(storage, node);
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
    valueVector.setSafe(index, Integer.toString(nodeRows.size()).getBytes(StandardCharsets.UTF_8));
    childVector.setNull(index++);

    if (node instanceof TreeRoot) {
      index = writeRootSystemRows((TreeRoot) node, keyVector, valueVector, childVector);
    }

    for (NodeKeyTableRow row : nodeRows) {
      writeNodeTableRow(row, keyVector, valueVector, childVector, index++);
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
    TreeNode leafNode = leafResult.nodePointer().orElse(null);
    ValidationUtil.checkState(leafNode != null, "Path search didn't return a valid node");
    boolean wasClean = !leafNode.isDirty();

    leafNode.set(key, value);

    if (wasClean && !(leafNode instanceof TreeRoot)) {
      String newLeafPath = FileLocations.newNodeFilePath();
      leafNode.setPath(newLeafPath);
      updatePathPointers(path);
    }

    if (needsSplit(leafNode, root.order())) {
      splitAndPropagateChanges(storage, path, root.order() - 1);
    }
  }

  private static void updatePathPointers(List<NodeSearchResult> path) {
    // TODO: ensure paths aren't changed already
    for (int i = path.size() - 1; i >= 0; i--) {
      TreeNode childNode = path.get(i).nodePointer().get();
      TreeNode parentNode = path.get(i - 1).nodePointer().get();
      NodeSearchResult childResult = path.get(i);

      parentNode.addInMemoryChange(
          childResult.key().orElse(null), childResult.value().orElse(null), childNode);
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
    List<NodeSearchResult> path =
        Lists.newArrayList(ImmutableNodeSearchResult.builder().nodePointer(startNode).build());
    TreeNode currentNode = startNode;

    while (true) {
      NodeSearchResult result = searchInNode(storage, currentNode, key);
      if (result.key().isPresent() && result.key().get().equals(key)) {
        return path;
      }
      if (result.nodePointer().isPresent()) {
        path.add(result);
        currentNode = result.nodePointer().get();
      } else {
        return path;
      }
    }
  }

  private static NodeSearchResult searchInNode(CatalogStorage storage, TreeNode node, String key) {
    NodeSearchResult memoryResult = node.search(key);

    // If we found an exact match in pending changes, return it
    if (memoryResult.key().isPresent() && memoryResult.key().get().equals(key)) {
      return memoryResult;
    }

    NodeSearchResult bestSliceResult = null;
    //    NodeSearchResult bestSliceResult = findBestSliceResult(storage, node, key);

    for (VectorSlice slice : node.getVectorSlices()) {
      NodeSearchResult result =
          searchInPersistedData(storage, slice.path(), key, slice.startIndex(), slice.endIndex());

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
      CatalogStorage storage, String nodePath, String key, int startIndex, int endIndex) {
    try (LocalInputStream stream = storage.startReadLocal(nodePath);
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

          int searchEnd = Math.min(endIndex, keyVector.getValueCount() - 1);
          if (startIndex > searchEnd) {
            continue;
          }
          VectorValueComparator<VarCharVector> comparator = new NodeVarCharComparator();
          comparator.attachVector(keyVector);
          int result =
              TreeUtil.vectorBinarySearch(
                  keyVector, comparator, searchKeyVector, NODE_FILE_KEY_COLUMN_INDEX, 0, searchEnd);

          if (result >= 0) {
            return createNodeSearchResult(storage, keyVector, valueVector, childVector, result);
          } else {
            int floorIndex = -result - 2;
            if (floorIndex >= startIndex && floorIndex < endIndex) {
              return createNodeSearchResult(
                  storage, keyVector, valueVector, childVector, floorIndex);
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

  private static void splitAndPropagateChanges(
      CatalogStorage storage, List<NodeSearchResult> path, int order) {
    TreeNode nodeToSplit = path.get(path.size() - 1).nodePointer().get();
    List<NodeKeyTableRow> rows = materializeDirtyNode(storage, nodeToSplit);

    int middleIndex = (rows.size() - 1) / 2;
    NodeKeyTableRow middleRow = rows.get(middleIndex);
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

    for (int i = 0; i < rows.size(); i++) {
      NodeKeyTableRow row = rows.get(i);
      if (i < middleIndex) {
        leftNode.addInMemoryChange(
            row.key().orElse(null), row.value().orElse(null), row.child().orElse(null));
      } else if (i > middleIndex) {
        rightNode.addInMemoryChange(
            row.key().orElse(null), row.value().orElse(null), row.child().orElse(null));
      }
    }

    if (nodeToSplit instanceof TreeRoot) {
      nodeToSplit.clear();
      nodeToSplit.addInMemoryChange(null, null, leftNode);
      nodeToSplit.addInMemoryChange(middleKey, middleValue, rightNode);
    } else {
      NodeSearchResult parentSearch = path.get(path.size() - 2);
      NodeSearchResult floorResult = path.get(path.size() - 1);

      TreeNode parent = parentSearch.nodePointer().get();
      String floorKey = floorResult.key().orElse(null);
      String floorValue = floorResult.value().orElse(null);

      parent.addInMemoryChange(floorKey, floorValue, leftNode);
      parent.addInMemoryChange(middleKey, middleValue, rightNode);

      if (needsSplit(parent, order)) {
        splitAndPropagateChanges(storage, path.subList(0, path.size() - 1), order);
      }
    }
  }

  private static List<NodeKeyTableRow> materializeDirtyNode(CatalogStorage storage, TreeNode node) {
    Map<String, NodeKeyTableRow> pendingChanges = node.pendingChanges();
    List<NodeKeyTableRow> result = Lists.newArrayList();
    List<AutoCloseable> resources = Lists.newArrayList();
    PriorityQueue<VectorSliceStream> sliceQueue =
        new PriorityQueue<>(Comparator.comparing(VectorSliceStream::getCurrentKey));

    try {
      for (VectorSlice slice : node.getVectorSlices()) {
        VectorSliceStream stream = new VectorSliceStream(storage, slice);
        resources.add(stream);
        if (stream.hasNext()) {
          sliceQueue.add(stream);
        }
      }
      mergeView(node, sliceQueue, result, pendingChanges);
      return result;
    } finally {
      for (AutoCloseable resource : resources) {
        try {
          resource.close();
        } catch (Exception e) {
          throw new OlympiaRuntimeException(e);
        }
      }
    }
  }

  private static void mergeView(
      TreeNode node,
      PriorityQueue<VectorSliceStream> sliceQueue,
      List<NodeKeyTableRow> result,
      Map<String, NodeKeyTableRow> pendingChanges) {
    boolean hasSliceWithNullKey = !sliceQueue.isEmpty() && sliceQueue.peek().isCurrentKeyNull();
    Optional<NodeKeyTableRow> pendingLeftChild = node.getLeftmostChild();
    if (pendingLeftChild.isPresent()) {
      result.add(pendingLeftChild.get());
      if (hasSliceWithNullKey) {
        advanceQueue(sliceQueue);
      }
    } else if (hasSliceWithNullKey) {
      // Use slice with null key if no in-memory leftmost child
      result.add(createRowFromSlice(sliceQueue.peek()));
      advanceQueue(sliceQueue);
    } else {
      // No leftmost child in either source, add empty entry
      result.add(ImmutableNodeKeyTableRow.builder().build());
    }

    Iterator<NodeKeyTableRow> pendingIter = pendingChanges.values().iterator();
    NodeKeyTableRow currentPending = pendingIter.hasNext() ? pendingIter.next() : null;

    while (!sliceQueue.isEmpty() || currentPending != null) {
      if (currentPending == null) {
        addRemainingSlices(sliceQueue, result);
        return;
      }

      if (sliceQueue.isEmpty()) {
        addRemainingPending(currentPending, pendingIter, result);
        return;
      }

      currentPending =
          compareKeysAndAdvanceStreams(sliceQueue, result, currentPending, pendingIter);
    }
  }

  private static NodeKeyTableRow compareKeysAndAdvanceStreams(
      PriorityQueue<VectorSliceStream> sliceQueue,
      List<NodeKeyTableRow> result,
      NodeKeyTableRow currentPending,
      Iterator<NodeKeyTableRow> pendingIter) {
    String pendingKey = currentPending.key().orElse(null);
    String sliceKey = sliceQueue.peek().getCurrentKey();
    NodeKeyTableRow current = currentPending;
    int compare = TreeUtil.compareKeys(sliceKey, pendingKey);
    if (compare < 0) {
      // Slice key comes first
      result.add(createRowFromSlice(sliceQueue.peek()));
      advanceQueue(sliceQueue);
    } else if (compare > 0) {
      // Pending key comes first
      result.add(currentPending);
      current = pendingIter.hasNext() ? pendingIter.next() : null;
    } else {
      // Same key pending overrides slice value
      result.add(currentPending);
      current = pendingIter.hasNext() ? pendingIter.next() : null;
      advanceQueue(sliceQueue);
    }
    return current;
  }

  private static void advanceQueue(PriorityQueue<VectorSliceStream> queue) {
    VectorSliceStream stream = queue.poll();
    if (stream.hasNext()) {
      stream.advance();
      if (stream.hasNext()) {
        queue.add(stream);
      }
    }
  }

  private static void addRemainingSlices(
      PriorityQueue<VectorSliceStream> sliceQueue, List<NodeKeyTableRow> result) {

    while (!sliceQueue.isEmpty()) {
      result.add(createRowFromSlice(sliceQueue.peek()));
      advanceQueue(sliceQueue);
    }
  }

  // Add remaining pending changes
  private static void addRemainingPending(
      NodeKeyTableRow current, Iterator<NodeKeyTableRow> iterator, List<NodeKeyTableRow> result) {

    result.add(current);
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }
  }

  private static NodeKeyTableRow createRowFromSlice(VectorSliceStream stream) {
    String sliceKey = stream.getCurrentKey();
    TreeNode child = null;
    String childPath = stream.getCurrentChildPath();
    if (childPath != null) {
      child = new BasicTreeNode();
      child.setPath(childPath);
    }
    return ImmutableNodeKeyTableRow.builder()
        .key(Optional.ofNullable(sliceKey))
        .value(Optional.ofNullable(stream.getCurrentValue()))
        .child(Optional.ofNullable(child))
        .build();
  }

  private static NodeSearchResult createNodeSearchResult(
      CatalogStorage storage,
      VarCharVector keyVector,
      VarCharVector valueVector,
      VarCharVector childVector,
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

    return ImmutableNodeSearchResult.builder()
        .key(Optional.ofNullable(key))
        .value(Optional.ofNullable(value))
        .nodePointer(Optional.ofNullable(child))
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

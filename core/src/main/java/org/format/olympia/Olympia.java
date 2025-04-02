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
package org.format.olympia;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.format.olympia.exception.CommitFailureException;
import org.format.olympia.exception.NonEmptyNamespaceException;
import org.format.olympia.exception.ObjectAlreadyExistsException;
import org.format.olympia.exception.ObjectNotFoundException;
import org.format.olympia.proto.actions.ActionType;
import org.format.olympia.proto.actions.NamespaceSetPropertiesDef;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.proto.objects.Column;
import org.format.olympia.proto.objects.DistributedTransactionDef;
import org.format.olympia.proto.objects.NamespaceDef;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.tree.BasicTreeRoot;
import org.format.olympia.tree.NodeKeyTableRow;
import org.format.olympia.tree.TreeOperations;
import org.format.olympia.tree.TreeRoot;
import org.format.olympia.util.ValidationUtil;

public class Olympia {

  private Olympia() {}

  public static boolean catalogExists(CatalogStorage storage) {
    String rootNodeFilePath = FileLocations.rootNodeFilePath(0);
    return storage.exists(rootNodeFilePath);
  }

  public static void createCatalog(CatalogStorage storage, CatalogDef catalogDef) {
    String catalogDefFilePath = FileLocations.newCatalogDefFilePath();
    ObjectDefinitions.writeCatalogDef(storage, catalogDefFilePath, catalogDef);

    BasicTreeRoot root = new BasicTreeRoot();
    root.setCatalogDefFilePath(catalogDefFilePath);
    String rootNodeFilePath = FileLocations.rootNodeFilePath(0);
    TreeOperations.writeRootNodeFile(storage, rootNodeFilePath, root);
    TreeOperations.tryWriteRootNodeVersionHintFile(storage, 0);
  }

  public static Transaction beginTransaction(CatalogStorage storage) {
    return beginTransaction(storage, ImmutableMap.of());
  }

  public static Transaction beginTransaction(
      CatalogStorage storage, Map<String, String> txnProperties) {
    TreeRoot current = TreeOperations.findLatestRoot(storage);
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, current);
    TransactionProperties transactionProperties =
        new TransactionProperties(catalogDef, txnProperties);
    long currentTimeMillis = System.currentTimeMillis();
    return Transaction.builder()
        .setBeganAtMillis(currentTimeMillis)
        .setExpireAtMillis(currentTimeMillis + transactionProperties.txnValidMillis())
        .setTransactionId(transactionProperties.txnId())
        .setBeginningRoot(current)
        .setRunningRoot(current)
        .setIsolationLevel(transactionProperties.isolationLevel())
        .build();
  }

  public static void commitTransaction(CatalogStorage storage, Transaction transaction)
      throws CommitFailureException {
    ValidationUtil.checkArgument(
        !transaction.runningRoot().pendingChanges().isEmpty(),
        "There is no change to be committed");
    ValidationUtil.checkState(
        transaction.beginningRoot().path().isPresent(),
        "Cannot find persisted storage path for beginning root");

    String beginningRootNodeFilePath = transaction.beginningRoot().path().get();
    long beginningRootVersion = FileLocations.versionFromNodeFilePath(beginningRootNodeFilePath);
    long nextRootVersion = beginningRootVersion + 1;
    String nextVersionFilePath = FileLocations.rootNodeFilePath(nextRootVersion);
    transaction.runningRoot().setPreviousRootNodeFilePath(beginningRootNodeFilePath);

    TreeOperations.writeRootNodeFile(storage, nextVersionFilePath, transaction.runningRoot());
    TreeOperations.tryWriteRootNodeVersionHintFile(storage, nextRootVersion);
    transaction.runningRoot().setPath(nextVersionFilePath);
    transaction.setCommitted();
  }

  public static String saveDistTransaction(CatalogStorage storage, Transaction transaction) {
    String runningRootNodeFilePath = FileLocations.newNodeFilePath();
    TreeOperations.writeRootNodeFile(storage, runningRootNodeFilePath, transaction.runningRoot());
    DistributedTransactionDef transactionDef =
        ObjectDefinitions.newDistTransactionDefBuilder()
            .setId(transaction.transactionId())
            .setIsolationLevel(transaction.isolationLevel())
            .setBeginningRootNodeFilePath(transaction.beginningRoot().path().get())
            .setRunningRootNodeFilePath(runningRootNodeFilePath)
            .setBeganAtMillis(transaction.beganAtMillis())
            .setExpireAtMillis(transaction.expireAtMillis())
            .build();
    String transactionDefFilePath =
        FileLocations.distTransactionDefFilePath(transaction.transactionId());
    ObjectDefinitions.writeTransactionDef(storage, transactionDefFilePath, transactionDef);
    return transactionDefFilePath;
  }

  public static Transaction loadDistTransaction(CatalogStorage storage, String distTransactionId) {
    String distTransactionDefFilePath = FileLocations.distTransactionDefFilePath(distTransactionId);
    DistributedTransactionDef transactionDef =
        ObjectDefinitions.readDistTransactionDef(storage, distTransactionDefFilePath);
    TreeRoot beginningRoot =
        TreeOperations.readRootNodeFile(storage, transactionDef.getBeginningRootNodeFilePath());
    TreeRoot runningRoot =
        TreeOperations.readRootNodeFile(storage, transactionDef.getRunningRootNodeFilePath());
    return Transaction.builder()
        .setTransactionId(transactionDef.getId())
        .setBeginningRoot(beginningRoot)
        .setRunningRoot(runningRoot)
        .setBeganAtMillis(transactionDef.getBeganAtMillis())
        .setExpireAtMillis(transactionDef.getExpireAtMillis())
        .setIsolationLevel(transactionDef.getIsolationLevel())
        .build();
  }

  public static boolean distTransactionExists(CatalogStorage storage, String transactionId) {
    String distTransactionDefFilePath = FileLocations.distTransactionDefFilePath(transactionId);
    return storage.exists(distTransactionDefFilePath);
  }

  public static List<String> showNamespaces(CatalogStorage storage, Transaction transaction) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    TreeRoot root = transaction.runningRoot();
    List<String> namespaces =
        TreeOperations.getNodeKeyTable(storage, root).stream()
            .map(NodeKeyTableRow::key)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(key -> ObjectKeys.isNamespaceKey(key, catalogDef))
            .map(key -> ObjectKeys.namespaceNameFromKey(key, catalogDef))
            .collect(Collectors.toList());
    transaction.addAction(
        ImmutableAction.builder().type(ActionType.CATALOG_SNOW_NAMESPACES).build());
    return namespaces;
  }

  public static boolean namespaceExists(
      CatalogStorage storage, Transaction transaction, String namespaceName) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    boolean exists =
        TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent();
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_EXISTS).build());
    return exists;
  }

  public static NamespaceDef describeNamespace(
      CatalogStorage storage, Transaction transaction, String namespaceName)
      throws ObjectNotFoundException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    Optional<String> namespaceDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey);
    if (!namespaceDefFilePath.isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    NamespaceDef def = ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath.get());
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_DESCRIBE).build());
    return def;
  }

  public static void createNamespace(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      NamespaceDef namespaceDef)
      throws ObjectAlreadyExistsException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectAlreadyExistsException("Namespace %s already exists", namespaceName);
    }

    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
    TreeOperations.setValue(storage, transaction.runningRoot(), namespaceKey, namespaceDefFilePath);
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_CREATE).build());
  }

  public static void alterNamespace(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      NamespaceDef namespaceDef)
      throws ObjectNotFoundException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(storage, namespaceDefFilePath, namespaceName, namespaceDef);
    TreeOperations.setValue(storage, transaction.runningRoot(), namespaceKey, namespaceDefFilePath);
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_ALTER).build());
  }

  public static void alterNamespaceSetProperties(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      Collection<Pair<String, String>> keyValuePairs) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    Optional<String> namespaceDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey);
    if (!namespaceDefFilePath.isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    NamespaceDef namespaceDef =
        ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath.get());
    NamespaceDef.Builder newNamespaceDefBuilder = namespaceDef.toBuilder();
    List<String> keys = Lists.newArrayList();
    for (Pair<String, String> keyValuePair : keyValuePairs) {
      newNamespaceDefBuilder.putProperties(keyValuePair.first(), keyValuePair.second());
      keys.add(keyValuePair.first());
    }

    String newNamespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(
        storage, newNamespaceDefFilePath, namespaceName, newNamespaceDefBuilder.build());
    TreeOperations.setValue(
        storage, transaction.runningRoot(), namespaceKey, newNamespaceDefFilePath);
    transaction.addAction(
        ImmutableAction.builder()
            .type(ActionType.NAMESPACE_ALTER_SET_PROPERTIES)
            .def(NamespaceSetPropertiesDef.newBuilder().addAllKeys(keys).build())
            .build());
  }

  public static void alterNamespaceUnsetProperties(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      Collection<String> keys) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    Optional<String> namespaceDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey);
    if (!namespaceDefFilePath.isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    NamespaceDef namespaceDef =
        ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath.get());
    NamespaceDef.Builder newNamespaceDefBuilder = namespaceDef.toBuilder();
    for (String key : keys) {
      // TODO: should it fail if key not found?
      newNamespaceDefBuilder.removeProperties(key);
    }

    String newNamespaceDefFilePath = FileLocations.newNamespaceDefFilePath(namespaceName);
    ObjectDefinitions.writeNamespaceDef(
        storage, newNamespaceDefFilePath, namespaceName, newNamespaceDefBuilder.build());
    TreeOperations.setValue(
        storage, transaction.runningRoot(), namespaceKey, newNamespaceDefFilePath);
    transaction.addAction(
        ImmutableAction.builder()
            .type(ActionType.NAMESPACE_ALTER_UNSET_PROPERTIES)
            .def(NamespaceSetPropertiesDef.newBuilder().addAllKeys(keys).build())
            .build());
  }

  public static void dropNamespace(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      DropNamespaceBehavior dropNsBehavior)
      throws ObjectNotFoundException, NonEmptyNamespaceException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }

    List<String> tableNames = Olympia.showTables(storage, transaction, namespaceName);

    switch (dropNsBehavior) {
      case CASCADE:
        for (String tableName : tableNames) {
          Olympia.dropTable(storage, transaction, namespaceName, tableName);
        }
        break;
      case RESTRICT:
        if (!tableNames.isEmpty()) {
          throw new NonEmptyNamespaceException("Namespace %s is not empty", namespaceName);
        }
        break;
      default:
        throw new IllegalArgumentException(
            String.format("DropNamespaceBehavior %s is not supported", dropNsBehavior));
    }

    TreeOperations.removeKey(storage, transaction.runningRoot(), namespaceKey);
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_DROP).build());
  }

  public static List<String> showTables(
      CatalogStorage storage, Transaction transaction, String namespaceName)
      throws ObjectNotFoundException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String tableKeyNamespacePrefix = ObjectKeys.tableKeyNamespacePrefix(namespaceName, catalogDef);
    TreeRoot root = transaction.runningRoot();
    List<String> tables =
        TreeOperations.getNodeKeyTable(storage, root).stream()
            .map(NodeKeyTableRow::key)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(key -> key.startsWith(tableKeyNamespacePrefix))
            .map(key -> ObjectKeys.tableNameFromKey(key, catalogDef))
            .collect(Collectors.toList());
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_SHOW_TABLES).build());
    return tables;
  }

  public static boolean tableExists(
      CatalogStorage storage, Transaction transaction, String namespaceName, String tableName) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, catalogDef);
    boolean exists =
        TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent();
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_SHOW_TABLES).build());
    return exists;
  }

  public static TableDef describeTable(
      CatalogStorage storage, Transaction transaction, String namespaceName, String tableName)
      throws ObjectNotFoundException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, catalogDef);
    Optional<String> tableDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey);
    if (!tableDefFilePath.isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exist", namespaceName, tableName);
    }
    TableDef def = ObjectDefinitions.readTableDef(storage, tableDefFilePath.get());
    transaction.addAction(ImmutableAction.builder().type(ActionType.TABLE_EXISTS).build());
    return def;
  }

  public static void createTable(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String tableName,
      TableDef tableDef)
      throws ObjectAlreadyExistsException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, catalogDef);
    if (TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent()) {
      throw new ObjectAlreadyExistsException(
          "Namespace %s table %s already exists", namespaceName, tableName);
    }

    String tableDefFilePath = FileLocations.newTableDefFilePath(namespaceName, tableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
    TreeOperations.setValue(storage, transaction.runningRoot(), tableKey, tableDefFilePath);
    transaction.addAction(ImmutableAction.builder().type(ActionType.TABLE_CREATE).build());
  }

  public static void alterTable(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String tableName,
      TableDef tableDef)
      throws ObjectNotFoundException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exists", namespaceName, tableName);
    }

    String tableDefFilePath = FileLocations.newTableDefFilePath(namespaceName, tableName);
    ObjectDefinitions.writeTableDef(storage, tableDefFilePath, namespaceName, tableName, tableDef);
    TreeOperations.setValue(storage, transaction.runningRoot(), tableKey, tableDefFilePath);
    transaction.addAction(ImmutableAction.builder().type(ActionType.TABLE_ALTER).build());
  }

  public static void alterTableAddColumns(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String tableName,
      Collection<Column> columns) {
    // TODO
  }

  public static void alterTableRemoveColumns(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String tableName,
      Collection<Column> columns) {
    // TODO
  }

  public static void modifyTableInsert(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String tableName,
      Collection<String> dataFileLocations) {
    // TODO
  }

  public static void dropTable(
      CatalogStorage storage, Transaction transaction, String namespaceName, String tableName)
      throws ObjectNotFoundException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String tableKey = ObjectKeys.tableKey(namespaceName, tableName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), tableKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s table %s does not exists", namespaceName, tableName);
    }

    TreeOperations.removeKey(storage, transaction.runningRoot(), tableKey);
    transaction.addAction(ImmutableAction.builder().type(ActionType.TABLE_DROP).build());
  }

  public static List<String> showViews(
      CatalogStorage storage, Transaction transaction, String namespaceName) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String viewKeyNamespacePrefix = ObjectKeys.viewKeyNamespacePrefix(namespaceName, catalogDef);
    TreeRoot root = transaction.runningRoot();
    List<String> views =
        TreeOperations.getNodeKeyTable(storage, root).stream()
            .map(NodeKeyTableRow::key)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(key -> key.startsWith(viewKeyNamespacePrefix))
            .map(key -> ObjectKeys.viewNameFromKey(key, catalogDef))
            .collect(Collectors.toList());
    transaction.addAction(ImmutableAction.builder().type(ActionType.NAMESPACE_SHOW_VIEWS).build());
    return views;
  }

  public static boolean viewExists(
      CatalogStorage storage, Transaction transaction, String namespaceName, String viewName) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, catalogDef);
    boolean exists =
        TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent();
    transaction.addAction(ImmutableAction.builder().type(ActionType.VIEW_EXISTS).build());
    return exists;
  }

  public static ViewDef describeView(
      CatalogStorage storage, Transaction transaction, String namespaceName, String viewName)
      throws ObjectNotFoundException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, catalogDef);
    Optional<String> viewDefFilePath =
        TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey);
    if (!viewDefFilePath.isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s view %s does not exist", namespaceName, viewName);
    }
    ViewDef def = ObjectDefinitions.readViewDef(storage, viewDefFilePath.get());
    transaction.addAction(ImmutableAction.builder().type(ActionType.VIEW_DESCRIBE).build());
    return def;
  }

  public static void createView(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String viewName,
      ViewDef viewDef)
      throws ObjectAlreadyExistsException, CommitFailureException {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, catalogDef);
    if (TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent()) {
      throw new ObjectAlreadyExistsException(
          "Namespace %s view %s already exists", namespaceName, viewName);
    }

    String viewDefFilePath = FileLocations.newViewDefFilePath(namespaceName, viewName);
    ObjectDefinitions.writeViewDef(storage, viewDefFilePath, namespaceName, viewName, viewDef);
    TreeOperations.setValue(storage, transaction.runningRoot(), viewKey, viewDefFilePath);
    transaction.addAction(ImmutableAction.builder().type(ActionType.VIEW_CREATE).build());
  }

  public static void replaceView(
      CatalogStorage storage,
      Transaction transaction,
      String namespaceName,
      String viewName,
      ViewDef viewDef) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String namespaceKey = ObjectKeys.namespaceKey(namespaceName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), namespaceKey).isPresent()) {
      throw new ObjectNotFoundException("Namespace %s does not exist", namespaceName);
    }
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s view %s does not exists", namespaceName, viewName);
    }

    String viewDefFilePath = FileLocations.newViewDefFilePath(namespaceName, viewName);
    ObjectDefinitions.writeViewDef(storage, viewDefFilePath, namespaceName, viewName, viewDef);
    TreeOperations.setValue(storage, transaction.runningRoot(), viewKey, viewDefFilePath);
    transaction.addAction(ImmutableAction.builder().type(ActionType.VIEW_REPLACE).build());
  }

  public static void dropView(
      CatalogStorage storage, Transaction transaction, String namespaceName, String viewName) {
    CatalogDef catalogDef = TreeOperations.findCatalogDef(storage, transaction.runningRoot());
    String viewKey = ObjectKeys.viewKey(namespaceName, viewName, catalogDef);
    if (!TreeOperations.searchValue(storage, transaction.runningRoot(), viewKey).isPresent()) {
      throw new ObjectNotFoundException(
          "Namespace %s view %s does not exists", namespaceName, viewName);
    }

    TreeOperations.removeKey(storage, transaction.runningRoot(), viewKey);
    transaction.addAction(ImmutableAction.builder().type(ActionType.VIEW_DROP).build());
  }
}

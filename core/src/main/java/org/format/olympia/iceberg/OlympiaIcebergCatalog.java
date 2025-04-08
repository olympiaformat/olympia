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
package org.format.olympia.iceberg;

import com.google.protobuf.Descriptors;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.OlympiaIcebergTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewUtil;
import org.apache.iceberg.view.ViewVersion;
import org.format.olympia.DropNamespaceBehavior;
import org.format.olympia.ObjectDefinitions;
import org.format.olympia.Olympia;
import org.format.olympia.Transaction;
import org.format.olympia.exception.ObjectAlreadyExistsException;
import org.format.olympia.exception.ObjectNotFoundException;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.proto.objects.NamespaceDef;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.relocated.com.google.common.collect.ImmutableList;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.relocated.com.google.common.collect.Maps;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.CatalogStorages;
import org.format.olympia.util.PropertyUtil;
import org.format.olympia.util.ValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OlympiaIcebergCatalog implements Catalog, ViewCatalog, SupportsNamespaces {

  private static final Logger LOG = LoggerFactory.getLogger(OlympiaIcebergCatalog.class);

  private CatalogStorage storage;
  private OlympiaIcebergCatalogProperties catalogProperties;
  private String catalogName;
  private Map<String, String> allProperties;
  private MetricsReporter metricsReporter;
  private Optional<Transaction> catalogTransaction = Optional.empty();

  /**
   * Constructor for dynamic initialization. It is expected to call {@code initialize} after using
   * this constructor
   */
  public OlympiaIcebergCatalog() {}

  /**
   * Constructor for directly initializing a catalog
   *
   * @param name catalog name
   * @param properties catalog properties
   */
  public OlympiaIcebergCatalog(String name, Map<String, String> properties) {
    initialize(name, properties);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String warehouse =
        PropertyUtil.propertyAsNullableString(properties, CatalogProperties.WAREHOUSE_LOCATION);

    if (!warehouse.contains("://")) {
      warehouse = "file://" + warehouse;
    }

    String storageType =
        PropertyUtil.propertyAsNullableString(
            properties, OlympiaIcebergCatalogProperties.STORAGE_TYPE);
    Map<String, String> storageOpsProperties =
        PropertyUtil.propertiesWithPrefix(
            properties, OlympiaIcebergCatalogProperties.STORAGE_OPS_PROPERTIES_PREFIX);

    Map<String, String> storageProperties = Maps.newHashMap();
    storageProperties.putAll(storageOpsProperties);
    if (storageType != null) {
      storageProperties.put(CatalogStorages.STORAGE_TYPE, storageType);
    }
    storageProperties.put(CatalogStorages.STORAGE_ROOT, warehouse);

    this.storage = CatalogStorages.initialize(storageProperties);
    this.catalogProperties = new OlympiaIcebergCatalogProperties(properties);
    this.allProperties = ImmutableMap.copyOf(properties);
    this.catalogName = name;
    this.metricsReporter = CatalogUtil.loadMetricsReporter(properties);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    if (namespace.isEmpty()) {
      return false;
    }

    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    if (nsInfo.isSystem()) {
      return Olympia.catalogExists(storage);
    }

    if (nsInfo.distTransactionId().isPresent()) {
      return Olympia.distTransactionExists(storage, nsInfo.distTransactionId().get());
    }

    Transaction transaction = Olympia.beginTransaction(storage);
    return Olympia.namespaceExists(storage, transaction, nsInfo.namespaceName());
  }

  @Override
  public List<Namespace> listNamespaces() {
    Transaction transaction = Olympia.beginTransaction(storage);
    List<String> namespaces = Olympia.showNamespaces(storage, transaction);
    List<Namespace> result =
        Lists.newArrayList(namespaces.stream().map(Namespace::of).collect(Collectors.toList()));
    result.add(Namespace.of(catalogProperties.systemNamespaceName()));
    return result;
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (namespace.isEmpty()) {
      return listNamespaces();
    }

    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    if (nsInfo.isSystem()) {
      return ImmutableList.of(
          Namespace.of(
              catalogProperties.systemNamespaceName(),
              catalogProperties.dtxnParentNamespaceName()));
    }

    if (nsInfo.distTransactionId().isPresent()) {
      Transaction transaction =
          Olympia.loadDistTransaction(storage, nsInfo.distTransactionId().get());
      return Olympia.showNamespaces(storage, transaction).stream()
          .map(Namespace::of)
          .collect(Collectors.toList());
    }

    return ImmutableList.of();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    if (nsInfo.isSystem()) {
      return ImmutableMap.of();
    }

    Transaction transaction = beginOrLoadTransaction(nsInfo);
    try {
      NamespaceDef namespaceDef =
          Olympia.describeNamespace(storage, transaction, nsInfo.namespaceName());
      return namespaceDef.getPropertiesMap();
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, ImmutableMap.of());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> properties) {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    if (nsInfo.isSystem()) {
      CatalogDef.Builder builder = ObjectDefinitions.newCatalogDefBuilder();
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        Descriptors.FieldDescriptor field =
            builder.getDescriptorForType().findFieldByName(entry.getKey());
        if (field != null) {
          builder.setField(field, entry.getValue());
        }
      }
      Olympia.createCatalog(storage, builder.build());
      return;
    }

    Transaction transaction = beginOrLoadTransaction(nsInfo);

    try {
      Olympia.createNamespace(
          storage,
          transaction,
          nsInfo.namespaceName(),
          ObjectDefinitions.newNamespaceDefBuilder()
              .putAllProperties(properties)
              .setId(UUID.randomUUID().toString())
              .build());
    } catch (ObjectAlreadyExistsException e) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    if (!nsInfo.distTransactionId().isPresent()) {
      Olympia.commitTransaction(storage, transaction);
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> propertyKeys)
      throws NoSuchNamespaceException {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    Transaction transaction = beginOrLoadTransaction(nsInfo);

    NamespaceDef currentDef;
    try {
      currentDef = Olympia.describeNamespace(storage, transaction, nsInfo.namespaceName());
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    NamespaceDef.Builder newDefBuilder =
        ObjectDefinitions.newNamespaceDefBuilder().mergeFrom(currentDef);
    propertyKeys.forEach(newDefBuilder::removeProperties);

    Olympia.alterNamespace(storage, transaction, nsInfo.namespaceName(), newDefBuilder.build());

    if (!nsInfo.distTransactionId().isPresent()) {
      Olympia.commitTransaction(storage, transaction);
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    Transaction transaction = beginOrLoadTransaction(nsInfo);

    NamespaceDef currentDef;
    try {
      currentDef = Olympia.describeNamespace(storage, transaction, nsInfo.namespaceName());
    } catch (ObjectNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    NamespaceDef.Builder newDefBuilder =
        ObjectDefinitions.newNamespaceDefBuilder().mergeFrom(currentDef);
    properties.forEach(newDefBuilder::putProperties);

    Olympia.alterNamespace(storage, transaction, nsInfo.namespaceName(), newDefBuilder.build());

    if (!nsInfo.distTransactionId().isPresent()) {
      Olympia.commitTransaction(storage, transaction);
    }
    return true;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);
    Transaction transaction = beginOrLoadTransaction(nsInfo);

    try {
      Olympia.dropNamespace(
          storage, transaction, nsInfo.namespaceName(), DropNamespaceBehavior.RESTRICT);
    } catch (ObjectNotFoundException e) {
      LOG.warn("Detected dropping non-existent namespace {}", namespace);
      return false;
    }

    if (!nsInfo.distTransactionId().isPresent()) {
      Olympia.commitTransaction(storage, transaction);
    }
    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);

    if (nsInfo.isSystem()) {
      return ImmutableList.of();
    }

    Transaction transaction = beginOrLoadTransaction(nsInfo);
    List<String> tableNames = Olympia.showTables(storage, transaction, nsInfo.namespaceName());
    return tableNames.stream()
        .map(t -> TableIdentifier.of(namespace, t))
        .collect(Collectors.toList());
  }

  @Override
  public boolean tableExists(TableIdentifier tableIdentifier) {
    IcebergTableInfo tableInfo =
        IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);

    Transaction transaction = beginOrLoadTransaction(tableInfo);

    return Olympia.tableExists(
        storage, transaction, tableInfo.namespaceName(), tableInfo.tableName());
  }

  @Override
  public Table loadTable(TableIdentifier tableIdentifier) {
    IcebergTableInfo tableInfo =
        IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);
    Transaction transaction = beginOrLoadTransaction(tableInfo);

    TableOperations ops =
        new OlympiaIcebergTableOperations(storage, allProperties, transaction, tableInfo);

    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    }

    if (tableInfo.metadataTableType().isPresent()) {
      return MetadataTableUtils.createMetadataTableInstance(
          ops,
          name(),
          TableIdentifier.of(tableInfo.namespaceName(), tableInfo.tableName()),
          tableIdentifier,
          tableInfo.metadataTableType().get());
    }

    Table table =
        new OlympiaIcebergTable(
            ops, OlympiaToIceberg.fullTableName(catalogName, tableInfo), metricsReporter);
    LOG.info("Table loaded by catalog: {}", table);
    return table;
  }

  @Override
  public Table registerTable(TableIdentifier tableIdentifier, String metadataFileLocation) {
    IcebergTableInfo tableInfo =
        IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);
    ValidationUtil.checkArgument(
        !tableInfo.metadataTableType().isPresent(),
        "Cannot register metadata table: %s",
        tableIdentifier);

    Transaction transaction = beginOrLoadTransaction(tableInfo);
    try {
      Olympia.createTable(
          storage,
          transaction,
          tableInfo.namespaceName(),
          tableInfo.tableName(),
          ObjectDefinitions.newTableDefBuilder()
              .setTableFormat(TableDef.TableFormat.ICEBERG)
              .setIcebergMetadataLocation(metadataFileLocation)
              .build());
    } catch (ObjectAlreadyExistsException e) {
      throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
    }

    commitOrSaveTransaction(tableInfo, transaction);
    TableOperations ops =
        new OlympiaIcebergTableOperations(storage, allProperties, transaction, tableInfo);

    return new OlympiaIcebergTable(
        ops, OlympiaToIceberg.fullTableName(catalogName, tableInfo), metricsReporter);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return buildTable(identifier, schema).create();
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return buildTable(identifier, schema).withPartitionSpec(spec).create();
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withPartitionSpec(spec)
        .withProperties(properties)
        .create();
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return buildTable(identifier, schema)
        .withLocation(location)
        .withPartitionSpec(spec)
        .withProperties(properties)
        .create();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new OlympiaIcebergTableBuilder(identifier, schema);
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier) {
    return dropTable(tableIdentifier, true /* drop data and metadata files */);
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    // TODO: provide a clear definition for purge vs no purge behavior.
    //  before that we will ignore the purge flag.

    IcebergTableInfo tableInfo =
        IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);

    Transaction transaction = beginOrLoadTransaction(tableInfo);

    try {
      Olympia.dropTable(storage, transaction, tableInfo.namespaceName(), tableInfo.tableName());
      commitOrSaveTransaction(tableInfo, transaction);
      return true;
    } catch (ObjectNotFoundException e) {
      return false;
    }
  }

  @Override
  public org.apache.iceberg.Transaction newReplaceTableTransaction(
      TableIdentifier identifier, Schema schema, boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(identifier, schema, orCreate);
  }

  @Override
  public org.apache.iceberg.Transaction newReplaceTableTransaction(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, orCreate);
  }

  @Override
  public org.apache.iceberg.Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties,
      boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(identifier, schema, spec, properties, orCreate);
  }

  @Override
  public org.apache.iceberg.Transaction newReplaceTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties,
      boolean orCreate) {
    return Catalog.super.newReplaceTableTransaction(
        identifier, schema, spec, location, properties, orCreate);
  }

  @Override
  public org.apache.iceberg.Transaction newCreateTableTransaction(
      TableIdentifier identifier, Schema schema) {
    return Catalog.super.newCreateTableTransaction(identifier, schema);
  }

  @Override
  public org.apache.iceberg.Transaction newCreateTableTransaction(
      TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return Catalog.super.newCreateTableTransaction(identifier, schema, spec);
  }

  @Override
  public org.apache.iceberg.Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return Catalog.super.newCreateTableTransaction(identifier, schema, spec, properties);
  }

  @Override
  public org.apache.iceberg.Transaction newCreateTableTransaction(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {
    return Catalog.super.newCreateTableTransaction(identifier, schema, spec, location, properties);
  }

  @Override
  public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
    // TODO: add support
  }

  @Override
  public void invalidateTable(TableIdentifier identifier) {}

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    IcebergNamespaceInfo nsInfo = IcebergToOlympia.parseNamespace(namespace, catalogProperties);

    if (nsInfo.isSystem()) {
      return ImmutableList.of();
    }

    Transaction transaction = beginOrLoadTransaction(nsInfo);
    List<String> viewNames = Olympia.showViews(storage, transaction, nsInfo.namespaceName());
    return viewNames.stream()
        .map(t -> TableIdentifier.of(namespace, t))
        .collect(Collectors.toList());
  }

  @Override
  public View loadView(TableIdentifier viewIdentifier) {
    IcebergTableInfo viewInfo =
        IcebergToOlympia.parseTableIdentifier(viewIdentifier, catalogProperties);
    Transaction transaction = beginOrLoadTransaction(viewInfo);

    ViewOperations ops =
        new OlympiaIcebergViewOperations(storage, catalogProperties, transaction, viewInfo);

    if (ops.current() == null) {
      throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
    }

    View view = new BaseView(ops, OlympiaToIceberg.fullTableName(catalogName, viewInfo));
    LOG.info("View loaded by catalog: {}", view);
    return view;
  }

  @Override
  public boolean viewExists(TableIdentifier viewIdentifier) {
    IcebergTableInfo viewInfo =
        IcebergToOlympia.parseTableIdentifier(viewIdentifier, catalogProperties);

    Transaction transaction = beginOrLoadTransaction(viewInfo);

    return Olympia.viewExists(storage, transaction, viewInfo.namespaceName(), viewInfo.tableName());
  }

  @Override
  public ViewBuilder buildView(TableIdentifier viewIdentifier) {
    return new OlympiaIcebergViewBuilder(viewIdentifier);
  }

  @Override
  public boolean dropView(TableIdentifier viewIdentifier) {
    IcebergTableInfo viewInfo =
        IcebergToOlympia.parseTableIdentifier(viewIdentifier, catalogProperties);

    Transaction transaction = beginOrLoadTransaction(viewInfo);

    try {
      Olympia.dropView(storage, transaction, viewInfo.namespaceName(), viewInfo.tableName());
      commitOrSaveTransaction(viewInfo, transaction);
      return true;
    } catch (ObjectNotFoundException e) {
      return false;
    }
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    // Todo: Add support
  }

  @Override
  public void invalidateView(TableIdentifier identifier) {
    // Todo: Add support
  }

  public synchronized void beginCatalogTransaction() {
    ValidationUtil.checkArgument(
        !catalogTransaction.isPresent(), "Catalog transaction already exists");
    this.catalogTransaction = Optional.of(Olympia.beginTransaction(storage));
  }

  public synchronized void commitCatalogTransaction() {
    ValidationUtil.checkArgument(
        catalogTransaction.isEmpty(), "There is no catalog transaction to be committed");
    Olympia.commitTransaction(storage, catalogTransaction.get());
    this.catalogTransaction = Optional.empty();
  }

  public synchronized void rollbackCatalogTransaction() {
    ValidationUtil.checkArgument(
        !catalogTransaction.isPresent(), "There is no catalog transaction to be rolled back");
    this.catalogTransaction = Optional.empty();
  }

  private Transaction beginOrLoadTransaction(IcebergNamespaceInfo nsInfo) {
    if (catalogTransaction.isPresent()) {
      ValidationUtil.checkArgument(
          !nsInfo.distTransactionId().isPresent(),
          "Cannot load distributed transaction within an ongoing transaction");
      return catalogTransaction.get();
    }

    ValidationUtil.checkArgument(
        !nsInfo.isSystem(), "Cannot start transaction against system namespace");
    return nsInfo.distTransactionId().isPresent()
        ? Olympia.loadDistTransaction(storage, nsInfo.distTransactionId().get())
        : Olympia.beginTransaction(storage);
  }

  private Transaction beginOrLoadTransaction(IcebergTableInfo tableInfo) {
    if (catalogTransaction.isPresent()) {
      ValidationUtil.checkArgument(
          !tableInfo.distTransactionId().isPresent(),
          "Cannot load distributed transaction within an ongoing transaction");
      return catalogTransaction.get();
    }

    return tableInfo.distTransactionId().isPresent()
        ? Olympia.loadDistTransaction(storage, tableInfo.distTransactionId().get())
        : Olympia.beginTransaction(storage);
  }

  private void commitOrSaveTransaction(IcebergTableInfo tableInfo, Transaction transaction) {
    if (tableInfo.distTransactionId().isPresent()) {
      Olympia.saveDistTransaction(storage, transaction);
    } else {
      Olympia.commitTransaction(storage, transaction);
    }
  }

  private String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // TODO: think about what is the best route for table root
    return storage.root().extendPath(tableIdentifier.toString()).toString();
  }

  public class OlympiaIcebergTableBuilder implements TableBuilder {

    private final TableIdentifier tableIdentifier;
    private final Schema schema;
    private final Map<String, String> tableProperties = Maps.newHashMap();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    public OlympiaIcebergTableBuilder(TableIdentifier tableIdentifier, Schema schema) {
      this.tableIdentifier = tableIdentifier;
      this.schema = schema;
      this.tableProperties.putAll(tableDefaultProperties());
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        tableProperties.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      tableProperties.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      IcebergTableInfo tableInfo =
          IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);
      Transaction transaction = beginOrLoadTransaction(tableInfo);
      TableOperations ops =
          new OlympiaIcebergTableOperations(storage, allProperties, transaction, tableInfo);

      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
      }

      TableMetadata metadata = prepareNewTableMetadata();
      ops.commit(null, metadata);
      return new OlympiaIcebergTable(
          ops, OlympiaToIceberg.fullTableName(catalogName, tableInfo), metricsReporter);
    }

    @Override
    public org.apache.iceberg.Transaction createTransaction() {
      IcebergTableInfo tableInfo =
          IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);

      Transaction transaction = beginOrLoadTransaction(tableInfo);
      OlympiaIcebergTableOperations ops =
          new OlympiaIcebergTableOperations(storage, allProperties, transaction, tableInfo);

      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
      }

      TableMetadata metadata = prepareNewTableMetadata();
      return Transactions.createTableTransaction(
          tableIdentifier.toString(), ops, metadata, metricsReporter);
    }

    @Override
    public org.apache.iceberg.Transaction replaceTransaction() {
      IcebergTableInfo tableInfo =
          IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);

      Transaction transaction = beginOrLoadTransaction(tableInfo);
      OlympiaIcebergTableOperations ops =
          new OlympiaIcebergTableOperations(storage, allProperties, transaction, tableInfo);

      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
      }

      TableMetadata metadata = prepareReplaceTableMetadata(ops);
      return Transactions.replaceTableTransaction(
          tableIdentifier.toString(), ops, metadata, metricsReporter);
    }

    @Override
    public org.apache.iceberg.Transaction createOrReplaceTransaction() {
      IcebergTableInfo tableInfo =
          IcebergToOlympia.parseTableIdentifier(tableIdentifier, catalogProperties);

      Transaction transaction = beginOrLoadTransaction(tableInfo);
      OlympiaIcebergTableOperations ops =
          new OlympiaIcebergTableOperations(storage, allProperties, transaction, tableInfo);

      TableMetadata metadata =
          ops.current() == null ? prepareNewTableMetadata() : prepareReplaceTableMetadata(ops);
      return Transactions.createOrReplaceTableTransaction(
          tableIdentifier.toString(), ops, metadata, metricsReporter);
    }

    private TableMetadata prepareNewTableMetadata() {
      String baseLocation = location != null ? location : defaultWarehouseLocation(tableIdentifier);
      tableProperties.putAll(tableOverrideProperties());
      return TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
    }

    private TableMetadata prepareReplaceTableMetadata(TableOperations ops) {
      String baseLocation = location != null ? location : defaultWarehouseLocation(tableIdentifier);
      tableProperties.putAll(tableOverrideProperties());
      return ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, tableProperties);
    }

    /**
     * Get default table properties set at Catalog level through catalog properties.
     *
     * @return default table properties specified in catalog properties
     */
    private Map<String, String> tableDefaultProperties() {
      Map<String, String> tableDefaultProperties =
          org.apache.iceberg.util.PropertyUtil.propertiesWithPrefix(
              allProperties, CatalogProperties.TABLE_DEFAULT_PREFIX);
      LOG.info(
          "Table properties set at catalog level through catalog properties: {}",
          tableDefaultProperties);
      return tableDefaultProperties;
    }

    /**
     * Get table properties that are enforced at Catalog level through catalog properties.
     *
     * @return default table properties enforced through catalog properties
     */
    private Map<String, String> tableOverrideProperties() {
      Map<String, String> tableOverrideProperties =
          org.apache.iceberg.util.PropertyUtil.propertiesWithPrefix(
              allProperties, CatalogProperties.TABLE_OVERRIDE_PREFIX);
      LOG.info(
          "Table properties enforced at catalog level through catalog properties: {}",
          tableOverrideProperties);
      return tableOverrideProperties;
    }
  }

  public class OlympiaIcebergViewBuilder implements ViewBuilder {

    private final TableIdentifier viewIdentifier;
    private final Map<String, String> viewProperties = Maps.newHashMap();
    private final List<ViewRepresentation> representations = Lists.newArrayList();
    private Namespace defaultNamespace = null;
    private String defaultCatalog = null;
    private Schema schema = null;
    private String location = null;

    public OlympiaIcebergViewBuilder(TableIdentifier viewIdentifier) {
      this.viewIdentifier = viewIdentifier;
      this.viewProperties.putAll(viewDefaultProperties());
    }

    private Map<String, String> viewDefaultProperties() {
      Map<String, String> viewDefaultProperties =
          org.apache.iceberg.util.PropertyUtil.propertiesWithPrefix(
              allProperties, CatalogProperties.VIEW_DEFAULT_PREFIX);
      LOG.info(
          "View properties set at catalog level through catalog properties: {}",
          viewDefaultProperties);
      return viewDefaultProperties;
    }

    @Override
    public ViewBuilder withProperties(Map<String, String> properties) {
      this.viewProperties.putAll(properties);
      return this;
    }

    @Override
    public ViewBuilder withProperty(String key, String value) {
      this.viewProperties.put(key, value);
      return this;
    }

    @Override
    public ViewBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public ViewBuilder withSchema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ViewBuilder withQuery(String dialect, String sql) {
      representations.add(
          ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
      return this;
    }

    @Override
    public ViewBuilder withDefaultCatalog(String catalog) {
      this.defaultCatalog = catalog;
      return this;
    }

    @Override
    public ViewBuilder withDefaultNamespace(Namespace namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    @Override
    public View create() {
      return create(newViewOps());
    }

    @Override
    public View replace() {
      return replace(newViewOps());
    }

    @Override
    public View createOrReplace() {
      ViewOperations ops = newViewOps();
      if (null == ops.current()) {
        return create(ops);
      } else {
        return replace(ops);
      }
    }

    private View create(ViewOperations ops) {
      if (ops.current() != null) {
        throw new AlreadyExistsException("View already exists: %s", viewIdentifier);
      }

      ValidationUtil.checkState(
          !representations.isEmpty(), "Cannot create view without specifying a query");
      ValidationUtil.checkState(null != schema, "Cannot create view without specifying schema");
      ValidationUtil.checkState(
          null != defaultNamespace, "Cannot create view without specifying a default namespace");
      ViewMetadata viewMetadata = prepareNewViewMetadata();
      ops.commit(null, viewMetadata);
      return new BaseView(ops, OlympiaToIceberg.fullTableName(catalogName, getViewInfo()));
    }

    private View replace(ViewOperations ops) {
      if (tableExists(viewIdentifier)) {
        throw new AlreadyExistsException("Table with same name already exists: %s", viewIdentifier);
      }

      if (null == ops.current()) {
        throw new NoSuchViewException("View does not exist: %s", viewIdentifier);
      }

      ValidationUtil.checkState(
          !representations.isEmpty(), "Cannot replace view without specifying a query");
      ValidationUtil.checkState(null != schema, "Cannot replace view without specifying schema");
      ValidationUtil.checkState(
          null != defaultNamespace, "Cannot replace view without specifying a default namespace");

      ViewMetadata metadata = ops.current();
      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(0)
              .schemaId(schema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(defaultNamespace)
              .defaultCatalog(defaultCatalog)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      ViewMetadata.Builder builder =
          ViewMetadata.buildFrom(metadata)
              .setProperties(viewProperties)
              .setCurrentVersion(viewVersion, schema);

      if (null != location) {
        builder.setLocation(location);
      }

      ViewMetadata replacement = builder.build();

      try {
        ops.commit(metadata, replacement);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("View was updated concurrently: %s", viewIdentifier);
      }

      return new BaseView(ops, ViewUtil.fullViewName(name(), viewIdentifier));
    }

    private ViewOperations newViewOps() {
      IcebergTableInfo viewInfo = getViewInfo();
      Transaction transaction = beginOrLoadTransaction(viewInfo);

      return new OlympiaIcebergViewOperations(storage, catalogProperties, transaction, viewInfo);
    }

    private IcebergTableInfo getViewInfo() {
      return IcebergToOlympia.parseTableIdentifier(viewIdentifier, catalogProperties);
    }

    private ViewMetadata prepareNewViewMetadata() {
      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(0)
              .schemaId(schema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(defaultNamespace)
              .defaultCatalog(defaultCatalog)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      return ViewMetadata.builder()
          .setProperties(viewProperties)
          .setLocation(null != location ? location : defaultWarehouseLocation(viewIdentifier))
          .setCurrentVersion(viewVersion, schema)
          .build();
    }
  }
}

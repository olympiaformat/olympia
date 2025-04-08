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

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.SnapshotIdGeneratorUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.LocationUtil;
import org.format.olympia.ObjectDefinitions;
import org.format.olympia.Olympia;
import org.format.olympia.Transaction;
import org.format.olympia.exception.ObjectNotFoundException;
import org.format.olympia.exception.StorageAtomicSealFailureException;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.relocated.com.google.common.base.Objects;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.util.ValidationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OlympiaIcebergTableOperations implements TableOperations, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(OlympiaIcebergTableOperations.class);

  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";

  private final CatalogStorage storage;
  private final Map<String, String> allProperties;
  private final IcebergTableInfo tableInfo;
  private final String namespaceTableName;
  private final FileIO fileIO;

  private Transaction transaction;
  private TableMetadata currentMetadata = null;
  private TableDef currentTableDef = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  public OlympiaIcebergTableOperations(
      CatalogStorage storage,
      Map<String, String> allProperties,
      Transaction transaction,
      IcebergTableInfo tableInfo) {
    this.storage = storage;
    this.allProperties = allProperties;
    this.transaction = transaction;
    this.tableInfo = tableInfo;
    this.namespaceTableName =
        String.format("%s.%s", tableInfo.namespaceName(), tableInfo.tableName());
    this.fileIO = initializeFileIO(allProperties);
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String ioImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL);
    return CatalogUtil.loadFileIO(ioImpl, properties, null);
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @Override
  public TableMetadata refresh() {
    boolean currentMetadataWasAvailable = currentMetadata != null;
    try {
      this.currentTableDef =
          Olympia.describeTable(
              storage, transaction, tableInfo.namespaceName(), tableInfo.tableName());
      loadMetadata(OlympiaToIceberg.tableMetadataLocation(currentTableDef));
    } catch (ObjectNotFoundException e) {
      currentMetadata = null;
      currentTableDef = null;
      currentMetadataLocation = null;
      version = -1;

      if (currentMetadataWasAvailable) {
        LOG.warn("Could not find the table during refresh, setting current metadata to null", e);
        shouldRefresh = true;
        throw e;
      } else {
        shouldRefresh = false;
      }
    }
    return current();
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      if (base != null) {
        throw new CommitFailedException("Cannot commit: stale table metadata");
      } else {
        // when current is non-null, the table exists. but when base is null, the commit is trying
        // to create the table
        throw new AlreadyExistsException("Table already exists: %s", namespaceTableName);
      }
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    long start = System.currentTimeMillis();

    String newMetadataLocation = writeNewMetadataIfRequired(base == null, metadata);
    TableDef.Builder newTableDef = ObjectDefinitions.newTableDefBuilder();

    if (currentTableDef != null) {
      newTableDef.mergeFrom(currentTableDef);
    }

    newTableDef.setIcebergMetadataLocation(newMetadataLocation);

    if (currentMetadata != null) {
      Olympia.alterTable(
          storage,
          transaction,
          tableInfo.namespaceName(),
          tableInfo.tableName(),
          newTableDef.build());
    } else {
      try {
        Olympia.createTable(
            storage,
            transaction,
            tableInfo.namespaceName(),
            tableInfo.tableName(),
            newTableDef.build());
      } catch (ObjectNotFoundException e) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: %s.%s", tableInfo.namespaceName(), tableInfo.tableName());
      }
    }

    if (tableInfo.distTransactionId().isPresent()) {
      // every commit needs to be saved to be used in a separated execution
      Olympia.saveDistTransaction(storage, transaction);
    } else {
      try {
        Olympia.commitTransaction(storage, transaction);
      } catch (StorageAtomicSealFailureException e) {
        throw new CommitFailedException(
            e, "Cannot commit, detected conflicting transaction");
      }
      // begin a new transaction since the previous one is already committed
      transaction = Olympia.beginTransaction(storage);
    }

    shouldRefresh = true;
    LOG.info(
        "Successfully committed to table {} in {} ms",
        namespaceTableName,
        System.currentTimeMillis() - start);
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public EncryptionManager encryption() {
    return PlaintextEncryptionManager.instance();
  }

  protected String writeNewMetadataIfRequired(boolean newTable, TableMetadata metadata) {
    return newTable && metadata.metadataFileLocation() != null
        ? metadata.metadataFileLocation()
        : writeNewMetadata(metadata, version + 1);
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    TableMetadataParser.overwrite(metadata, newMetadataLocation);

    return newMetadataLocation.location();
  }

  protected void loadMetadata(String newLocation) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      LOG.info("Loading table metadata from location: {}", newLocation);

      TableMetadata newMetadata = TableMetadataParser.read(io(), newLocation);
      String newUUID = newMetadata.uuid();
      if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
        ValidationUtil.checkState(
            newUUID.equals(currentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s",
            currentMetadata.uuid(),
            newUUID);
      }

      this.currentMetadata = newMetadata;
      this.currentMetadataLocation = newLocation;
      this.version = parseVersion(newLocation);
    }
    this.shouldRefresh = false;
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s", metadata.location(), filename);
    }
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return OlympiaIcebergTableOperations.this.metadataFileLocation(
            uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return OlympiaIcebergTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return OlympiaIcebergTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return OlympiaIcebergTableOperations.this.newSnapshotId();
      }
    };
  }

  @Override
  public long newSnapshotId() {
    return SnapshotIdGeneratorUtil.generateSnapshotID();
  }

  @Override
  public boolean requireStrictCleanup() {
    return true;
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName =
        meta.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(
        meta,
        String.format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  /**
   * Parse the version from table metadata file name.
   *
   * @param metadataLocation table metadata file location
   * @return version of the table metadata file in success case and -1 if the version is not
   *     parsable (as a sign that the metadata is not part of this catalog)
   */
  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      // found filesystem table's metadata
      return -1;
    }

    try {
      return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }

  public Transaction transaction() {
    return transaction;
  }
}

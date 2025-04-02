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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.format.olympia.exception.StorageReadFailureException;
import org.format.olympia.exception.StorageWriteFailureException;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.proto.objects.DistributedTransactionDef;
import org.format.olympia.proto.objects.IsolationLevel;
import org.format.olympia.proto.objects.NamespaceDef;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.storage.CatalogStorage;

public class ObjectDefinitions {

  public static final int CATALOG_MAJOR_VERSION_DEFAULT = 0;

  public static final int CATALOG_ORDER_DEFAULT = 128;

  public static final int CATALOG_NAMESPACE_NAME_MAX_SIZE_BYTES_DEFAULT = 100;

  public static final int CATALOG_TABLE_NAME_MAX_SIZE_BYTES_DEFAULT = 100;

  public static final int CATALOG_VIEW_NAME_MAX_SIZE_BYTES_DEFAULT = 100;

  public static final long CATALOG_NODE_FILE_MAX_SIZE_BYTES_DEFAULT = 1048576;

  public static final IsolationLevel CATALOG_TRANSACTION_ISOLATION_LEVEL_DEFAULT =
      IsolationLevel.SNAPSHOT;

  public static final long CATALOG_TRANSACTION_TTL_MILLIS_DEFAULT = TimeUnit.DAYS.toMillis(3);

  private ObjectDefinitions() {}

  public static void writeCatalogDef(CatalogStorage storage, String path, CatalogDef catalogDef) {

    try (OutputStream stream = storage.startCommit(path)) {
      catalogDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e, "Failed to write catalog definition to storage path %s at %s", path, storage.root());
    }
  }

  public static CatalogDef readCatalogDef(CatalogStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return CatalogDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e, "Failed to read catalog definition from storage path %s at %s", path, storage.root());
    }
  }

  public static CatalogDef.Builder newCatalogDefBuilder() {
    return CatalogDef.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setMajorVersion(CATALOG_MAJOR_VERSION_DEFAULT)
        .setOrder(CATALOG_ORDER_DEFAULT)
        .setNamespaceNameMaxSizeBytes(CATALOG_NAMESPACE_NAME_MAX_SIZE_BYTES_DEFAULT)
        .setTableNameMaxSizeBytes(CATALOG_TABLE_NAME_MAX_SIZE_BYTES_DEFAULT)
        .setViewNameMaxSizeBytes(CATALOG_VIEW_NAME_MAX_SIZE_BYTES_DEFAULT)
        .setNodeFileMaxSizeBytes(CATALOG_NODE_FILE_MAX_SIZE_BYTES_DEFAULT)
        .setTxnIsolationLevel(CATALOG_TRANSACTION_ISOLATION_LEVEL_DEFAULT)
        .setTxnTtlMillis(CATALOG_TRANSACTION_TTL_MILLIS_DEFAULT);
  }

  public static void writeNamespaceDef(
      CatalogStorage storage, String path, String namespaceName, NamespaceDef namespaceDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      namespaceDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s definition to storage path %s at %s",
          namespaceName,
          path,
          storage.root());
    }
  }

  public static NamespaceDef readNamespaceDef(CatalogStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return NamespaceDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read namespace definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static NamespaceDef.Builder newNamespaceDefBuilder() {
    return NamespaceDef.newBuilder().setId(UUID.randomUUID().toString());
  }

  public static void writeTableDef(
      CatalogStorage storage,
      String path,
      String namespaceName,
      String tableName,
      TableDef tableDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      tableDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s table %s definition to storage path %s at %s",
          namespaceName,
          tableName,
          path,
          storage.root());
    }
  }

  public static TableDef.Builder newTableDefBuilder() {
    return TableDef.newBuilder().setId(UUID.randomUUID().toString());
  }

  public static TableDef readTableDef(CatalogStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return TableDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e, "Failed to read table definition from storage path %s at %s", path, storage.root());
    }
  }

  public static void writeViewDef(
      CatalogStorage storage, String path, String namespaceName, String viewName, ViewDef viewDef) {
    try (OutputStream stream = storage.startCommit(path)) {
      viewDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write namespace %s view %s definition to storage path %s at %s",
          namespaceName,
          viewName,
          path,
          storage.root());
    }
  }

  public static ViewDef readViewDef(CatalogStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return ViewDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e, "Failed to read view definition from storage path %s at %s", path, storage.root());
    }
  }

  public static ViewDef.Builder newViewDefBuilder() {
    return ViewDef.newBuilder().setId(UUID.randomUUID().toString());
  }

  public static void writeTransactionDef(
      CatalogStorage storage, String path, DistributedTransactionDef transactionDef) {
    // TODO: The transaction definition has a fixed path name.
    //  New changes to the transaction state can overwrite the old one.
    //  Ideally we should also use the commit mechanism with a version number for each change.
    //  we will do that in later iterations.
    try (OutputStream stream = storage.startOverwrite(path)) {
      transactionDef.writeTo(stream);
    } catch (IOException e) {
      throw new StorageWriteFailureException(
          e,
          "Failed to write transaction %s definition to storage path %s at %s",
          transactionDef.getId(),
          path,
          storage.root());
    }
  }

  public static DistributedTransactionDef readDistTransactionDef(
      CatalogStorage storage, String path) {
    try (InputStream stream = storage.startRead(path)) {
      return DistributedTransactionDef.parseFrom(stream);
    } catch (IOException e) {
      throw new StorageReadFailureException(
          e,
          "Failed to read transaction definition from storage path %s at %s",
          path,
          storage.root());
    }
  }

  public static DistributedTransactionDef.Builder newDistTransactionDefBuilder() {
    return DistributedTransactionDef.newBuilder();
  }
}

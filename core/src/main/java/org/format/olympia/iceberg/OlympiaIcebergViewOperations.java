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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.format.olympia.Olympia;
import org.format.olympia.Transaction;
import org.format.olympia.exception.ObjectNotFoundException;
import org.format.olympia.exception.StorageAtomicSealFailureException;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.storage.CatalogStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OlympiaIcebergViewOperations implements ViewOperations, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(OlympiaIcebergViewOperations.class);

  private final CatalogStorage storage;
  private final OlympiaIcebergCatalogProperties catalogProps;
  private final IcebergTableInfo viewInfo;
  private final String namespaceViewName;

  private Transaction transaction;
  private ViewMetadata currentMetadata = null;
  private ViewDef currentViewDef = null;
  private boolean shouldRefresh = true;

  public OlympiaIcebergViewOperations(
      CatalogStorage storage,
      OlympiaIcebergCatalogProperties catalogProps,
      Transaction transaction,
      IcebergTableInfo viewInfo) {
    this.storage = storage;
    this.catalogProps = catalogProps;
    this.transaction = transaction;
    this.viewInfo = viewInfo;
    this.namespaceViewName = String.format("%s.%s", viewInfo.namespaceName(), viewInfo.tableName());
  }

  @Override
  public ViewMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @Override
  public ViewMetadata refresh() {
    boolean currentMetadataWasAvailable = currentMetadata != null;
    try {
      this.currentViewDef =
          Olympia.describeView(
              storage, transaction, viewInfo.namespaceName(), viewInfo.tableName());
      currentMetadata = OlympiaToIceberg.loadViewMetadata(currentViewDef);
      this.shouldRefresh = false;
    } catch (ObjectNotFoundException e) {
      currentMetadata = null;
      currentViewDef = null;

      if (currentMetadataWasAvailable) {
        LOG.warn("Could not find the view during refresh, setting current metadata to null", e);
        shouldRefresh = true;
        throw e;
      } else {
        shouldRefresh = false;
      }
    }
    return current();
  }

  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      if (base != null) {
        throw new CommitFailedException("Cannot commit: stale view metadata");
      } else {
        // when current is non-null, the view exists. but when base is null, the commit is trying
        // to create the view
        throw new AlreadyExistsException("View already exists: %s", namespaceViewName);
      }
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    long start = System.currentTimeMillis();

    ViewDef newViewDef = IcebergToOlympia.parseViewDef(metadata, catalogProps);
    if (currentMetadata != null) {
      Olympia.replaceView(
          storage, transaction, viewInfo.namespaceName(), viewInfo.tableName(), newViewDef);
    } else {
      try {
        Olympia.createView(
            storage, transaction, viewInfo.namespaceName(), viewInfo.tableName(), newViewDef);
      } catch (ObjectNotFoundException e) {
        throw new NoSuchNamespaceException(
            "Namespace does not exist: %s.%s", viewInfo.namespaceName(), viewInfo.tableName());
      }
    }

    if (viewInfo.distTransactionId().isPresent()) {
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
        "Successfully committed to view {} in {} ms",
        namespaceViewName,
        System.currentTimeMillis() - start);
  }
}

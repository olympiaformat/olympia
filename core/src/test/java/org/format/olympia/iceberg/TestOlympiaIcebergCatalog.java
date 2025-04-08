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

import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Disabled("Skip until all fixes are done")
class TestOlympiaIcebergCatalog extends CatalogTests<OlympiaIcebergCatalog> {

  @TempDir private Path warehouse;
  private OlympiaIcebergCatalog catalog;

  @BeforeEach
  public void before() {
    catalog =
        initCatalog(
            "olympia", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString()));
    catalog.createNamespace(
        Namespace.of(OlympiaIcebergCatalogProperties.SYSTEM_NAMESPACE_NAME_DEFAULT));
  }

  @Override
  protected OlympiaIcebergCatalog catalog() {
    return catalog;
  }

  @Override
  protected OlympiaIcebergCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString());
    properties.putAll(additionalProperties);
    return new OlympiaIcebergCatalog("olympia", properties);
  }

  @Override
  protected boolean supportsEmptyNamespace() {
    return false;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return false;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return false;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Test
  @Override
  public void testRenameTable() {
    // TODO: support rename
  }

  @Test
  @Override
  public void testRenameTableDestinationTableAlreadyExists() {
    // TODO: support rename
  }

  @Test
  @Override
  public void testRenameTableMissingSourceTable() {
    // TODO: support rename
  }

  @Test
  @Override
  public void renameTableNamespaceMissing() {
    // TODO: support rename
  }
}

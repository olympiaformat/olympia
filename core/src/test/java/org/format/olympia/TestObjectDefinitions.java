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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.UUID;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.proto.objects.NamespaceDef;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.storage.BasicCatalogStorage;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.local.LocalStorageOps;
import org.format.olympia.storage.local.LocalStorageOpsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestObjectDefinitions {

  @TempDir private File tempDir;
  private CatalogStorage storage;

  private String testNamespaceName;
  private String testViewName;
  private String testTableName;

  private CatalogDef catalogDef;
  private NamespaceDef namespaceDef;
  private TableDef tableDef;

  private final ViewDef testViewDef =
      ObjectDefinitions.newViewDefBuilder()
          .setSchemaBinding(false)
          .putProperties("k1", "v1")
          .build();

  @BeforeEach
  public void beforeEach() {
    CommonStorageOpsProperties props =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir + "/tmp-write",
                CommonStorageOpsProperties.PREPARE_READ_STAGING_DIRECTORY, tempDir + "/tmp-read"));
    this.storage =
        new BasicCatalogStorage(
            new LiteralURI("file://" + tempDir),
            new LocalStorageOps(props, LocalStorageOpsProperties.instance()));

    String randomSuffix = String.valueOf(UUID.randomUUID()).substring(0, 6);
    this.testNamespaceName = "test-namespace-" + randomSuffix;
    this.testViewName = "test-view-" + randomSuffix;
    this.testTableName = "test-table-" + randomSuffix;

    this.catalogDef = ObjectDefinitions.newCatalogDefBuilder().build();
    this.namespaceDef = ObjectDefinitions.newNamespaceDefBuilder().build();
    this.tableDef = ObjectDefinitions.newTableDefBuilder().build();
  }

  @Test
  public void testWriteViewDef() {
    String testViewDefFilePath = FileLocations.newViewDefFilePath(testNamespaceName, testViewName);
    File testViewDefFile = new File(tempDir, testViewDefFilePath);

    assertThat(testViewDefFile.exists()).isFalse();
    ObjectDefinitions.writeViewDef(
        storage, testViewDefFilePath, testNamespaceName, testViewName, testViewDef);
    assertThat(testViewDefFile.exists()).isTrue();
  }

  @Test
  public void testReadViewDef() {
    String testViewDefFilePath = FileLocations.newViewDefFilePath(testNamespaceName, testViewName);
    File testViewDefFile = new File(tempDir, testViewDefFilePath);

    assertThat(testViewDefFile.exists()).isFalse();
    ObjectDefinitions.writeViewDef(
        storage, testViewDefFilePath, testNamespaceName, testViewName, testViewDef);
    assertThat(testViewDefFile.exists()).isTrue();

    ViewDef viewDef = ObjectDefinitions.readViewDef(storage, testViewDefFilePath);
    assertThat(viewDef).isEqualTo(testViewDef);
  }

  @Test
  public void testWriteNamespaceDef() {
    String catalogDefFilePath = FileLocations.newCatalogDefFilePath();
    File catalogDefFile = new File(tempDir, catalogDefFilePath);
    assertThat(catalogDefFile.exists()).isFalse();
    ObjectDefinitions.writeCatalogDef(storage, catalogDefFilePath, catalogDef);
    assertThat(catalogDefFile.exists()).isTrue();
    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(testNamespaceName);

    File namespaceDefFile = new File(tempDir, namespaceDefFilePath);
    assertThat(namespaceDefFile.exists()).isFalse();
    ObjectDefinitions.writeNamespaceDef(
        storage, namespaceDefFilePath, testNamespaceName, namespaceDef);
    assertThat(namespaceDefFile.exists()).isTrue();
  }

  @Test
  public void testReadNamespaceDef() {
    String catalogDefFilePath = FileLocations.newCatalogDefFilePath();
    ObjectDefinitions.writeCatalogDef(storage, catalogDefFilePath, catalogDef);
    String namespaceDefFilePath = FileLocations.newNamespaceDefFilePath(testNamespaceName);
    ObjectDefinitions.writeNamespaceDef(
        storage, namespaceDefFilePath, testNamespaceName, namespaceDef);

    NamespaceDef testNamespaceDef =
        ObjectDefinitions.readNamespaceDef(storage, namespaceDefFilePath);
    assertThat(testNamespaceDef).isEqualTo(namespaceDef);
  }

  @Test
  public void testWriteTableDef() {
    ObjectDefinitions.writeCatalogDef(storage, FileLocations.newCatalogDefFilePath(), catalogDef);
    ObjectDefinitions.writeNamespaceDef(
        storage,
        FileLocations.newNamespaceDefFilePath(testNamespaceName),
        testNamespaceName,
        namespaceDef);
    String tableDefFilePath = FileLocations.newTableDefFilePath(testNamespaceName, testTableName);
    File tableDefFile = new File(tempDir, tableDefFilePath);

    assertThat(tableDefFile.exists()).isFalse();
    ObjectDefinitions.writeTableDef(
        storage, tableDefFilePath, testNamespaceName, testTableName, tableDef);
    assertThat(tableDefFile.exists()).isTrue();
  }

  @Test
  public void testReadTableDef() {
    ObjectDefinitions.writeCatalogDef(storage, FileLocations.newCatalogDefFilePath(), catalogDef);
    ObjectDefinitions.writeNamespaceDef(
        storage,
        FileLocations.newNamespaceDefFilePath(testNamespaceName),
        testNamespaceName,
        namespaceDef);
    String tableDefFilePath = FileLocations.newTableDefFilePath(testNamespaceName, testTableName);
    ObjectDefinitions.writeTableDef(
        storage, tableDefFilePath, testNamespaceName, testTableName, tableDef);

    TableDef testTableDef = ObjectDefinitions.readTableDef(storage, tableDefFilePath);
    assertThat(testTableDef).isEqualTo(tableDef);
  }
}

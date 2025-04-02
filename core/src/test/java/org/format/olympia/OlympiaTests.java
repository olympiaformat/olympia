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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import org.format.olympia.exception.NonEmptyNamespaceException;
import org.format.olympia.exception.ObjectAlreadyExistsException;
import org.format.olympia.exception.ObjectNotFoundException;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.proto.objects.NamespaceDef;
import org.format.olympia.proto.objects.NamespaceObjectFullName;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.tree.TreeOperations;
import org.format.olympia.tree.TreeRoot;
import org.junit.jupiter.api.Test;

public abstract class OlympiaTests {

  protected static final CatalogDef CATALOG_DEF = ObjectDefinitions.newCatalogDefBuilder().build();

  protected static final String NAMESPACE = "ns1";

  protected static final NamespaceDef NAMESPACE_DEF =
      ObjectDefinitions.newNamespaceDefBuilder().putProperties("k1", "v1").build();

  protected static final TableDef TABLE1_DEF =
      ObjectDefinitions.newTableDefBuilder().putProperties("k1", "v1").build();

  protected static final String TABLE1 = "tbl1";

  protected static final TableDef TABLE2_DEF =
      ObjectDefinitions.newTableDefBuilder().putProperties("k2", "v2").build();

  protected static final String TABLE2 = "tbl2";

  protected static final ViewDef VIEW_DEF =
      ObjectDefinitions.newViewDefBuilder()
          .setSchemaBinding(false)
          .addReferencedObjectFullNames(
              NamespaceObjectFullName.newBuilder()
                  .setNamespaceName(NAMESPACE)
                  .setName(TABLE1)
                  .build())
          .putProperties("k1", "v1")
          .build();

  protected static final String VIEW = "view1";

  protected abstract CatalogStorage storage();

  @Test
  public void testCreateCatalog() {
    CatalogStorage storage = storage();

    assertThat(storage.exists(FileLocations.LATEST_VERSION_FILE_PATH)).isTrue();
    assertThat(Olympia.catalogExists(storage)).isTrue();
    assertTreeRoot(0);
  }

  @Test
  public void testCreateNamespace() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    assertTreeRoot(1);
    TreeRoot root = TreeOperations.findLatestRoot(storage);
    String ns1Key = ObjectKeys.namespaceKey(NAMESPACE, CATALOG_DEF);
    Optional<String> ns1Path = TreeOperations.searchValue(storage, root, ns1Key);
    assertThat(ns1Path.isPresent()).isTrue();
    NamespaceDef readDef = ObjectDefinitions.readNamespaceDef(storage, ns1Path.get());
    assertThat(readDef).isEqualTo(NAMESPACE_DEF);
  }

  @Test
  public void testCreateNamespaceAlreadyExists() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThatThrownBy(
            () -> Olympia.createNamespace(storage, transaction, NAMESPACE, NAMESPACE_DEF))
        .isInstanceOf(ObjectAlreadyExistsException.class);
  }

  @Test
  public void testDescribeNamespace() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isTrue();

    NamespaceDef ns1DefDescribe = Olympia.describeNamespace(storage, transaction, NAMESPACE);
    assertThat(ns1DefDescribe).isEqualTo(NAMESPACE_DEF);
  }

  @Test
  public void testDropNamespace() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    Olympia.dropNamespace(storage, transaction, NAMESPACE, DropNamespaceBehavior.RESTRICT);
    Olympia.commitTransaction(storage, transaction);
    transaction = Olympia.beginTransaction(storage);

    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isFalse();
  }

  @Test
  public void testDropNamespaceCascade() {
    CatalogStorage storage = storage();
    createNamespaceAndTables();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE2)).isTrue();

    Olympia.dropNamespace(storage, transaction, NAMESPACE, DropNamespaceBehavior.CASCADE);
    Olympia.commitTransaction(storage, transaction);
    transaction = Olympia.beginTransaction(storage);

    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isFalse();
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isFalse();
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE2)).isFalse();
  }

  @Test
  public void testDropNonEmptyNamespace() {
    CatalogStorage storage = storage();
    createNamespaceAndTables();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE2)).isTrue();
    assertThatThrownBy(
            () ->
                Olympia.dropNamespace(
                    storage, transaction, NAMESPACE, DropNamespaceBehavior.RESTRICT))
        .isInstanceOf(NonEmptyNamespaceException.class)
        .hasMessageContaining(String.format("Namespace %s is not empty", NAMESPACE));
  }

  @Test
  public void testDescribeMissingNamespace() {
    CatalogStorage storage = storage();
    String ns = "nonexistent";

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, ns)).isFalse();
    assertThatThrownBy(() -> Olympia.describeNamespace(storage, transaction, ns))
        .isInstanceOf(ObjectNotFoundException.class);
  }

  @Test
  public void testAlterDescribeNamespace() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    NamespaceDef ns1DefDescribe = Olympia.describeNamespace(storage, transaction, NAMESPACE);
    assertThat(ns1DefDescribe).isEqualTo(NAMESPACE_DEF);

    NamespaceDef ns1DefAlter =
        ObjectDefinitions.newNamespaceDefBuilder().putProperties("k1", "v2").build();
    Olympia.alterNamespace(storage, transaction, NAMESPACE, ns1DefAlter);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.namespaceExists(storage, transaction, NAMESPACE)).isTrue();
    NamespaceDef ns1DefAlterDescribe = Olympia.describeNamespace(storage, transaction, NAMESPACE);
    assertThat(ns1DefAlterDescribe).isEqualTo(ns1DefAlter);
  }

  @Test
  public void testCreateTable() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    Olympia.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertTreeRoot(2);
    String t1Key = ObjectKeys.tableKey(NAMESPACE, TABLE1, CATALOG_DEF);
    Optional<String> t1Path = TreeOperations.searchValue(storage, root, t1Key);
    assertThat(t1Path.isPresent()).isTrue();
    TableDef readDef = ObjectDefinitions.readTableDef(storage, t1Path.get());
    assertThat(readDef).isEqualTo(TABLE1_DEF);
  }

  @Test
  public void testDescribeTable() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    TableDef t1DefDescribe = Olympia.describeTable(storage, transaction, NAMESPACE, TABLE1);
    assertThat(t1DefDescribe).isEqualTo(TABLE1_DEF);
  }

  @Test
  public void testAlterDescribeTable() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    TableDef t1DefDescribe = Olympia.describeTable(storage, transaction, NAMESPACE, TABLE1);
    assertThat(t1DefDescribe).isEqualTo(TABLE1_DEF);

    TableDef t1DefAlter = ObjectDefinitions.newTableDefBuilder().putProperties("k1", "v2").build();
    Olympia.alterTable(storage, transaction, NAMESPACE, TABLE1, t1DefAlter);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    TableDef t1DefAlterDescribe = Olympia.describeTable(storage, transaction, NAMESPACE, TABLE1);
    assertThat(t1DefAlterDescribe).isEqualTo(t1DefAlter);
  }

  @Test
  public void testDropTable() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isTrue();
    Olympia.dropTable(storage, transaction, NAMESPACE, TABLE1);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.tableExists(storage, transaction, NAMESPACE, TABLE1)).isFalse();
  }

  @Test
  public void testCreateTableNoNamespace() {
    CatalogStorage storage = storage();

    Transaction transaction = Olympia.beginTransaction(storage);
    assertThatThrownBy(
            () -> Olympia.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF))
        .isInstanceOf(ObjectNotFoundException.class);
  }

  @Test
  public void testCreateView() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    Olympia.commitTransaction(storage, transaction);

    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertTreeRoot(2);
    String v1Key = ObjectKeys.viewKey(NAMESPACE, VIEW, CATALOG_DEF);
    Optional<String> v1Path = TreeOperations.searchValue(storage, root, v1Key);
    assertThat(v1Path.isPresent()).isTrue();
    ViewDef readDef = ObjectDefinitions.readViewDef(storage, v1Path.get());
    assertThat(readDef).isEqualTo(VIEW_DEF);
  }

  @Test
  public void testShowViews() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createView(storage, transaction, NAMESPACE, "view1", VIEW_DEF);
    Olympia.createView(storage, transaction, NAMESPACE, "view2", VIEW_DEF);
    List<String> viewIdentifiers = Olympia.showViews(storage, transaction, NAMESPACE);
    Olympia.commitTransaction(storage, transaction);

    assertTreeRoot(2);

    assertThat(viewIdentifiers).hasSize(2);
    assertThat(viewIdentifiers).containsExactly("view1", "view2");
  }

  @Test
  public void testCreateDescribeView() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.viewExists(storage, transaction, NAMESPACE, VIEW)).isTrue();
    ViewDef v1DefDescribe = Olympia.describeView(storage, transaction, NAMESPACE, VIEW);
    assertThat(v1DefDescribe).isEqualTo(VIEW_DEF);
  }

  @Test
  public void testCreateDescribeDropDescribeNamespace() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.viewExists(storage, transaction, NAMESPACE, VIEW)).isTrue();
    ViewDef v1DefDescribe = Olympia.describeView(storage, transaction, NAMESPACE, VIEW);
    assertThat(v1DefDescribe).isEqualTo(VIEW_DEF);

    Olympia.dropView(storage, transaction, NAMESPACE, VIEW);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.viewExists(storage, transaction, NAMESPACE, VIEW)).isFalse();
  }

  @Test
  public void testDropView() {
    CatalogStorage storage = storage();
    createNamespaceAndCommit();

    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createView(storage, transaction, NAMESPACE, VIEW, VIEW_DEF);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.viewExists(storage, transaction, NAMESPACE, VIEW)).isTrue();
    Olympia.dropView(storage, transaction, NAMESPACE, VIEW);
    Olympia.commitTransaction(storage, transaction);

    transaction = Olympia.beginTransaction(storage);
    assertThat(Olympia.viewExists(storage, transaction, NAMESPACE, VIEW)).isFalse();
  }

  protected void assertTreeRoot(int expectedVersion) {
    CatalogStorage storage = storage();
    TreeRoot root = TreeOperations.findLatestRoot(storage);
    assertThat(root.path().isPresent()).isTrue();
    assertThat(root.path().get()).isEqualTo(FileLocations.rootNodeFilePath(expectedVersion));
    if (root.previousRootNodeFilePath().isPresent()) {
      assertThat(root.previousRootNodeFilePath().get())
          .isEqualTo(FileLocations.rootNodeFilePath(expectedVersion - 1));
    }
  }

  protected void createNamespaceAndCommit() {
    CatalogStorage storage = storage();
    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createNamespace(storage, transaction, NAMESPACE, NAMESPACE_DEF);
    Olympia.commitTransaction(storage, transaction);
  }

  protected void createNamespaceAndTables() {
    CatalogStorage storage = storage();
    Transaction transaction = Olympia.beginTransaction(storage);
    Olympia.createNamespace(storage, transaction, NAMESPACE, NAMESPACE_DEF);
    Olympia.createTable(storage, transaction, NAMESPACE, TABLE1, TABLE1_DEF);
    Olympia.createTable(storage, transaction, NAMESPACE, TABLE2, TABLE2_DEF);
    Olympia.commitTransaction(storage, transaction);
  }
}

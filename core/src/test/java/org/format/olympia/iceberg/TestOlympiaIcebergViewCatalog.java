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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewProperties;
import org.apache.iceberg.view.ViewUtil;
import org.apache.iceberg.view.ViewVersion;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestOlympiaIcebergViewCatalog {

  private static final Schema SCHEMA =
      new Schema(
          5,
          required(3, "id", Types.IntegerType.get()),
          required(4, "data", Types.StringType.get()));

  private static final Schema OTHER_SCHEMA =
      new Schema(7, required(1, "some_id", Types.IntegerType.get()));

  private static final ReadRel READ_REL =
      ReadRel.newBuilder()
          .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("prod.person").build())
          .setBaseSchema(NamedStruct.newBuilder().addNames("id").addNames("data").build())
          .build();

  @TempDir private Path warehouse;
  private OlympiaIcebergCatalog catalog;

  @BeforeEach
  public void before() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString())
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key1", "catalog-default-key1")
            .put(CatalogProperties.VIEW_DEFAULT_PREFIX + "key2", "catalog-default-key2")
            .build();
    catalog = new OlympiaIcebergCatalog("olympia", properties);
    catalog.createNamespace(
        Namespace.of(OlympiaIcebergCatalogProperties.SYSTEM_NAMESPACE_NAME_DEFAULT));
  }

  protected OlympiaIcebergCatalog catalog() {
    return catalog;
  }

  @Test
  public void basicCreateView() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");
    catalog().createNamespace(identifier.namespace());

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    // validate view settings
    assertThat(view.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(view.schema().schemaId()).isEqualTo(0);
    assertThat(view.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(view.currentVersion().operation()).isEqualTo("replace");
    assertThat(view.schemas()).hasSize(1).containsKey(0);
    assertThat(view.versions()).hasSize(1).containsExactly(view.currentVersion());

    assertThat(view.currentVersion())
        .isEqualTo(
            ImmutableViewVersion.builder()
                .timestampMillis(view.currentVersion().timestampMillis())
                .versionId(0)
                .schemaId(0)
                .summary(view.currentVersion().summary())
                .defaultNamespace(Namespace.of("ns"))
                .addRepresentations(
                    ImmutableSQLViewRepresentation.builder()
                        .sql(Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
                        .dialect("substrait")
                        .build())
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void createViewThatAlreadyExists() {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");
    catalog().createNamespace(identifier.namespace());

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    View view =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
            .create();

    assertThat(view).isNotNull();
    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildView(identifier)
                    .withSchema(OTHER_SCHEMA)
                    .withQuery(
                        "substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
                    .withDefaultNamespace(identifier.namespace())
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("View already exists: ns.view");
  }

  @ParameterizedTest(name = ".createOrReplace() = {arguments}")
  @ValueSource(booleans = {false, true})
  public void createOrReplaceView(boolean useCreateOrReplace) {
    TableIdentifier identifier = TableIdentifier.of("ns", "view");
    catalog().createNamespace(identifier.namespace());

    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();

    ViewBuilder viewBuilder =
        catalog()
            .buildView(identifier)
            .withSchema(SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2");
    View view = useCreateOrReplace ? viewBuilder.createOrReplace() : viewBuilder.create();

    assertThat(catalog().viewExists(identifier)).as("View should exist").isTrue();

    ViewVersion viewVersion = view.currentVersion();
    assertThat(viewVersion.representations())
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql(Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
                .dialect("substrait")
                .build());

    viewBuilder =
        catalog()
            .buildView(identifier)
            .withSchema(OTHER_SCHEMA)
            .withDefaultNamespace(identifier.namespace())
            .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
            .withProperty("replacedProp1", "val1")
            .withProperty("replacedProp2", "val2")
            .withProperty(ViewProperties.REPLACE_DROP_DIALECT_ALLOWED, "true");
    View replacedView = useCreateOrReplace ? viewBuilder.createOrReplace() : viewBuilder.replace();

    // validate replaced view settings
    assertThat(replacedView.name()).isEqualTo(ViewUtil.fullViewName(catalog().name(), identifier));
    assertThat(replacedView.properties())
        .containsEntry("prop1", "val1")
        .containsEntry("prop2", "val2")
        .containsEntry("replacedProp1", "val1")
        .containsEntry("replacedProp2", "val2");

    assertThat(replacedView.schema().asStruct()).isEqualTo(OTHER_SCHEMA.asStruct());

    ViewVersion replacedViewVersion = replacedView.currentVersion();

    assertThat(replacedViewVersion).isNotNull();
    assertThat(replacedViewVersion.operation()).isEqualTo("replace");
    assertThat(replacedViewVersion.representations())
        .containsExactly(
            ImmutableSQLViewRepresentation.builder()
                .sql(Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
                .dialect("substrait")
                .build());

    assertThat(catalog().dropView(identifier)).isTrue();
    assertThat(catalog().viewExists(identifier)).as("View should not exist").isFalse();
  }

  @Test
  public void listViews() {
    Namespace ns1 = Namespace.of("ns1");
    Namespace ns2 = Namespace.of("ns2");

    TableIdentifier view1 = TableIdentifier.of(ns1, "view1");
    TableIdentifier view2 = TableIdentifier.of(ns2, "view2");
    TableIdentifier view3 = TableIdentifier.of(ns2, "view3");

    catalog().createNamespace(ns1);
    catalog().createNamespace(ns2);

    assertThat(catalog().listViews(ns1)).isEmpty();
    assertThat(catalog().listViews(ns2)).isEmpty();

    catalog()
        .buildView(view1)
        .withSchema(SCHEMA)
        .withDefaultNamespace(view1.namespace())
        .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
        .create();

    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).isEmpty();

    catalog()
        .buildView(view2)
        .withSchema(SCHEMA)
        .withDefaultNamespace(view2.namespace())
        .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
        .create();

    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).containsExactly(view2);

    catalog()
        .buildView(view3)
        .withSchema(SCHEMA)
        .withDefaultNamespace(view3.namespace())
        .withQuery("substrait", Base64.getEncoder().encodeToString(READ_REL.toByteArray()))
        .create();

    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).containsExactlyInAnyOrder(view2, view3);

    assertThat(catalog().dropView(view2)).isTrue();
    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).containsExactly(view3);

    assertThat(catalog().dropView(view3)).isTrue();
    assertThat(catalog().listViews(ns1)).containsExactly(view1);
    assertThat(catalog().listViews(ns2)).isEmpty();

    assertThat(catalog().dropView(view1)).isTrue();
    assertThat(catalog().listViews(ns1)).isEmpty();
    assertThat(catalog().listViews(ns2)).isEmpty();
  }
}

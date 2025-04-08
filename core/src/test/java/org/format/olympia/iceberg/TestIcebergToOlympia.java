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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import java.util.Base64;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.format.olympia.proto.objects.Column;
import org.format.olympia.proto.objects.NamespaceObjectFullName;
import org.format.olympia.proto.objects.Schema;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestIcebergToOlympia {

  @Test
  public void testParseViewDef() {
    ReadRel readRel =
        ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("prod.person"))
            .setBaseSchema(NamedStruct.newBuilder().addNames("name").build())
            .build();
    ByteString readRelByteStr = ByteString.copyFrom(readRel.toByteArray());
    String base64EncodedReadRel = Base64.getEncoder().encodeToString(readRel.toByteArray());

    ImmutableViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(0)
            .timestampMillis(System.currentTimeMillis())
            .defaultCatalog("prod")
            .defaultNamespace(Namespace.of("default"))
            .putSummary("user", "some-user")
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .dialect("substrait")
                    .sql(base64EncodedReadRel)
                    .build())
            .schemaId(0)
            .build();

    ViewMetadata icebergViewMetadata =
        ViewMetadata.builder()
            .setLocation("location")
            .addSchema(
                new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "x", Types.LongType.get()),
                    Types.NestedField.optional(2, "y", Types.DateType.get())))
            .addVersion(viewVersion)
            .setProperties(ImmutableMap.of("key1", "val1"))
            .build();

    ViewDef viewDef =
        IcebergToOlympia.parseViewDef(
            icebergViewMetadata, new OlympiaIcebergCatalogProperties(ImmutableMap.of()));

    assertThat(viewDef.getId()).isNotNull();
    assertThat(viewDef.getSchemaBinding()).isTrue();
    assertThat(viewDef.getSchema())
        .isEqualTo(
            Schema.newBuilder()
                .addColumns(
                    Column.newBuilder()
                        .setId(1)
                        .setName("x")
                        .setType(Column.DataType.INT8)
                        .setNullable(false)
                        .build())
                .addColumns(
                    Column.newBuilder()
                        .setId(2)
                        .setName("y")
                        .setType(Column.DataType.DATE)
                        .setNullable(true)
                        .build())
                .build());
    assertThat(viewDef.getSubstraitReadRel()).isEqualTo(readRelByteStr);
    assertThat(viewDef.getDefaultNamespaceName()).isEqualTo("default");
    assertThat(viewDef.getReferencedObjectFullNamesList())
        .containsExactly(
            NamespaceObjectFullName.newBuilder()
                .setNamespaceName("prod")
                .setName("person")
                .build());
    assertThat(viewDef.getPropertiesMap()).isEqualTo(ImmutableMap.of("key1", "val1"));
  }
}

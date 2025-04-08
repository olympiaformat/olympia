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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.google.protobuf.ByteString;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import java.util.Base64;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.format.olympia.proto.objects.Column;
import org.format.olympia.proto.objects.NamespaceObjectFullName;
import org.format.olympia.proto.objects.Schema;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestOlympiaToIceberg {

  @Test
  public void testLoadViewMetadata() {
    ReadRel readRel =
        ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("prod.person"))
            .setBaseSchema(NamedStruct.newBuilder().addNames("name").build())
            .build();

    ByteString readRelByteStr = ByteString.copyFrom(readRel.toByteArray());
    String base64EncodedReadRel = Base64.getEncoder().encodeToString(readRel.toByteArray());

    ViewDef viewDef =
        ViewDef.newBuilder()
            .setId("0123")
            .setSchemaBinding(true)
            .setSchema(
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
                    .build())
            .setSubstraitReadRel(readRelByteStr)
            .setDefaultNamespaceName("prod")
            .addReferencedObjectFullNames(
                NamespaceObjectFullName.newBuilder()
                    .setNamespaceName("prod")
                    .setName("person")
                    .build())
            .putAllProperties(ImmutableMap.of("key1", "val1"))
            .build();

    ViewMetadata viewMetadata = OlympiaToIceberg.loadViewMetadata(viewDef);

    assertThat(viewMetadata.uuid()).isNotNull();
    assertThat(viewMetadata.properties()).isEqualTo(ImmutableMap.of("key1", "val1"));
    assertThat(viewMetadata.location()).isEqualTo("-");

    assertThat(viewMetadata.schemas().size()).isEqualTo(1);
    assertThat(viewMetadata.schemas().get(0).toString())
        .isEqualTo(
            new org.apache.iceberg.Schema(
                    Types.NestedField.required(1, "x", Types.LongType.get()),
                    Types.NestedField.optional(2, "y", Types.DateType.get()))
                .toString());

    assertThat(viewMetadata.currentVersion()).isNotNull();
    assertThat(viewMetadata.currentVersion().defaultNamespace()).isEqualTo(Namespace.of("prod"));
    assertThat(viewMetadata.currentVersion().representations().size()).isEqualTo(1);

    ViewRepresentation representation = viewMetadata.currentVersion().representations().get(0);
    assertThat(representation).isInstanceOf(SQLViewRepresentation.class);

    SQLViewRepresentation sqlViewRep = (SQLViewRepresentation) representation;
    assertThat(sqlViewRep.dialect()).isEqualTo(IcebergFormatProperties.SUBSTRAIT_DIALECT);
    assertThat(sqlViewRep.sql()).isEqualTo(base64EncodedReadRel);
  }
}

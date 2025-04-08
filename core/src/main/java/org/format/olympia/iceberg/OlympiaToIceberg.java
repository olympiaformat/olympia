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

import com.google.protobuf.ByteString;
import io.substrait.proto.ReadRel;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.format.olympia.proto.objects.Column;
import org.format.olympia.proto.objects.Schema;
import org.format.olympia.proto.objects.TableDef;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.util.SubstraitUtil;
import org.format.olympia.util.ValidationUtil;

public class OlympiaToIceberg {

  private OlympiaToIceberg() {}

  public static String tableMetadataLocation(TableDef tableDef) {
    ValidationUtil.checkNotNull(
        tableDef.getIcebergMetadataLocation(),
        "Metadata location is required in Iceberg format properties");
    return tableDef.getIcebergMetadataLocation();
  }

  public static String fullTableName(String catalogName, IcebergTableInfo tableInfo) {
    return String.format("%s.%s.%s", catalogName, tableInfo.namespaceName(), tableInfo.tableName());
  }

  public static ViewMetadata loadViewMetadata(ViewDef viewDef) {
    ValidationUtil.checkArgument(
        viewDef.getSchemaBinding(), "Schema binding must be set for Iceberg view");
    ValidationUtil.checkArgument(
        viewDef.getReferencedObjectFullNamesCount() > 0,
        "Must have at least one referenced object");

    ViewMetadata.Builder viewMetadataBuilder = ViewMetadata.builder();

    viewMetadataBuilder.addSchema(loadSchema(viewDef.getSchema()));
    ViewRepresentation viewRepresentation = loadViewRepresentation(viewDef.getSubstraitReadRel());

    viewMetadataBuilder.addVersion(
        ImmutableViewVersion.builder()
            .versionId(0)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(0)
            .addRepresentations(viewRepresentation)
            .defaultNamespace(Namespace.of(viewDef.getDefaultNamespaceName()))
            .build());

    // view metadata is generated on the fly, there is no actual view metadata location
    viewMetadataBuilder.setLocation("-").setProperties(viewDef.getPropertiesMap());
    return viewMetadataBuilder.build();
  }

  private static org.apache.iceberg.Schema loadSchema(Schema schema) {
    List<Types.NestedField> fields =
        schema.getColumnsList().stream()
            .map(OlympiaToIceberg::loadColumnField)
            .collect(Collectors.toList());
    return new org.apache.iceberg.Schema(fields);
  }

  private static Types.NestedField loadColumnField(Column column) {
    if (column.getNullable()) {
      return Types.NestedField.optional(column.getId(), column.getName(), loadColumnType(column));
    }
    return Types.NestedField.required(column.getId(), column.getName(), loadColumnType(column));
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static Type loadColumnType(Column column) {
    Column.DataType type = column.getType();
    if (type == Column.DataType.BOOLEAN) {
      return Types.BooleanType.get();
    } else if (type == Column.DataType.INT4) {
      return Types.IntegerType.get();
    } else if (type == Column.DataType.INT8) {
      return Types.LongType.get();
    } else if (type == Column.DataType.FLOAT4) {
      return Types.FloatType.get();
    } else if (type == Column.DataType.FLOAT8) {
      return Types.DoubleType.get();
    } else if (type == Column.DataType.DATE) {
      return Types.DateType.get();
    } else if (type == Column.DataType.TIME6) {
      return Types.TimeType.get();
    } else if (type == Column.DataType.TIMESTAMP6) {
      return Types.TimestampType.withoutZone();
    } else if (type == Column.DataType.TIMESTAMPTZ6) {
      return Types.TimestampType.withZone();
    } else if (type == Column.DataType.FIXED) {
      return Types.FixedType.ofLength(16);
    } else if (type == Column.DataType.BINARY) {
      return Types.BinaryType.get();
    } else if (type == Column.DataType.VARCHAR) {
      return Types.StringType.get();
    } else if (type == Column.DataType.DECIMAL) {
      return Types.DecimalType.of(38, 10);
    } else {
      // TODO: Add recursive conversion for STRUCT, MAP, LIST
      throw new IllegalArgumentException("Unsupported data type: " + type);
    }
  }

  private static ViewRepresentation loadViewRepresentation(ByteString substraitReadRel) {
    ReadRel substraitReadReal = SubstraitUtil.loadSubstraitReadReal(substraitReadRel);
    String encodedSubstraitReadReal =
        Base64.getEncoder().encodeToString(substraitReadReal.toByteArray());
    return ImmutableSQLViewRepresentation.builder()
        .dialect(IcebergFormatProperties.SUBSTRAIT_DIALECT)
        .sql(encodedSubstraitReadReal)
        .build();
  }
}

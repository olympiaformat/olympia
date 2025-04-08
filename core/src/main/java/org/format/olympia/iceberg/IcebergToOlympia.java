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

import io.substrait.proto.ReadRel;
import java.util.List;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewVersion;
import org.format.olympia.ObjectDefinitions;
import org.format.olympia.exception.InvalidArgumentException;
import org.format.olympia.proto.objects.Column;
import org.format.olympia.proto.objects.NamespaceObjectFullName;
import org.format.olympia.proto.objects.Schema;
import org.format.olympia.proto.objects.ViewDef;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.util.SubstraitUtil;
import org.format.olympia.util.ValidationUtil;

public class IcebergToOlympia {

  private IcebergToOlympia() {}

  public static IcebergNamespaceInfo parseNamespace(
      Namespace namespace, OlympiaIcebergCatalogProperties catalogProperties) {
    ValidationUtil.checkArgument(
        !namespace.isEmpty(), "Empty namespace is not allowed");

    if (!catalogProperties.systemNamespaceName().equals(namespace.level(0))) {
      ValidationUtil.checkArgument(
          namespace.length() == 1, "Namespace name must only have 1 level");
      return ImmutableIcebergNamespaceInfo.builder()
          .isSystem(false)
          .namespaceName(namespace.level(0))
          .build();
    }

    if (namespace.length() == 1) {
      return ImmutableIcebergNamespaceInfo.builder()
          .isSystem(true)
          .namespaceName(catalogProperties.systemNamespaceName())
          .build();
    }

    String parentNamespaceName = namespace.level(1);
    if (catalogProperties.dtxnParentNamespaceName().equals(parentNamespaceName)) {
      String distTransactionId = namespace.level(2);
      ValidationUtil.checkArgument(
          distTransactionId.startsWith(catalogProperties.dtxnNamespacePrefix()),
          "Distributed transaction namespace name must start with %s",
          catalogProperties.dtxnNamespacePrefix());
      return ImmutableIcebergNamespaceInfo.builder()
          .isSystem(false)
          .distTransactionId(
              distTransactionId.substring(catalogProperties.dtxnNamespacePrefix().length()))
          .namespaceName(namespace.level(3))
          .build();
    }

    throw new InvalidArgumentException(
        "Unknown parent namespace name in system namespace: %s", parentNamespaceName);
  }

  public static IcebergTableInfo parseTableIdentifier(
      TableIdentifier tableIdentifier, OlympiaIcebergCatalogProperties catalogProperties) {
    Namespace namespace = tableIdentifier.namespace();

    ValidationUtil.checkArgument(
        !namespace.isEmpty(), "Empty namespace is not allowed");

    if (!catalogProperties.systemNamespaceName().equals(namespace.level(0))) {
      ValidationUtil.checkArgument(
          namespace.length() <= 2, "Namespace name must only have 1 level");

      if (namespace.length() == 1) {
        return ImmutableIcebergTableInfo.builder()
            .namespaceName(namespace.level(0))
            .tableName(tableIdentifier.name())
            .build();
      }

      MetadataTableType metadataTableType = MetadataTableType.from(tableIdentifier.name());
      ValidationUtil.checkArgument(
          metadataTableType != null,
          "Unknown metadata table type %s in table identifier: %s",
          tableIdentifier.name(),
          tableIdentifier);
      return ImmutableIcebergTableInfo.builder()
          .namespaceName(namespace.level(0))
          .tableName(namespace.level(1))
          .metadataTableType(metadataTableType)
          .build();
    }

    ValidationUtil.checkArgument(
        namespace.length() == 3 || namespace.length() == 4,
        "system namespace does not directly contain any table");

    String subSystemNamespace = namespace.level(1);
    ValidationUtil.checkArgument(
        catalogProperties.dtxnParentNamespaceName().equals(subSystemNamespace),
        "system namespace does not contain any table in sub-namespace: %s",
        subSystemNamespace);

    String distTransactionNamespace = namespace.level(2);
    ValidationUtil.checkArgument(
        distTransactionNamespace.startsWith(catalogProperties.dtxnNamespacePrefix()),
        "Distributed transaction namespace name must start with %s",
        catalogProperties.dtxnNamespacePrefix());

    String distTransactionId =
        distTransactionNamespace.substring(catalogProperties.dtxnNamespacePrefix().length());

    if (namespace.length() == 3) {
      return ImmutableIcebergTableInfo.builder()
          .distTransactionId(distTransactionId)
          .namespaceName(namespace.level(2))
          .tableName(tableIdentifier.name())
          .build();
    }

    MetadataTableType metadataTableType = MetadataTableType.from(tableIdentifier.name());
    ValidationUtil.checkArgument(
        metadataTableType != null,
        "Unknown metadata table type %s in table identifier: %s",
        tableIdentifier.name(),
        tableIdentifier);
    return ImmutableIcebergTableInfo.builder()
        .distTransactionId(distTransactionId)
        .namespaceName(namespace.level(2))
        .tableName(namespace.level(3))
        .metadataTableType(metadataTableType)
        .build();
  }

  public static ViewDef parseViewDef(
      ViewMetadata metadata, OlympiaIcebergCatalogProperties catalogProps) {
    ViewDef.Builder newViewDefBuilder = ObjectDefinitions.newViewDefBuilder();
    newViewDefBuilder.setSchemaBinding(metadata.schema() != null);

    if (metadata.schema() != null) {
      newViewDefBuilder.setSchema(parseSchema(metadata.schema()));
    }

    ViewVersion currentViewVersion = metadata.currentVersion();
    if (currentViewVersion != null && currentViewVersion.representations() != null) {
      IcebergNamespaceInfo namespaceInfo =
          parseNamespace(currentViewVersion.defaultNamespace(), catalogProps);

      newViewDefBuilder.setDefaultNamespaceName(namespaceInfo.namespaceName());

      List<ViewRepresentation> viewReprs = currentViewVersion.representations();
      ValidationUtil.checkArgument(
          viewReprs.size() == 1, "Expect exactly one view representation to exist");

      ReadRel readRel = parseSubstraitReadRel(viewReprs.get(0));
      newViewDefBuilder.setSubstraitReadRel(readRel.toByteString());
      newViewDefBuilder.addAllReferencedObjectFullNames(parseFullNames(readRel, catalogProps));
    }

    newViewDefBuilder.putAllProperties(metadata.properties());
    return newViewDefBuilder.build();
  }

  private static Schema parseSchema(org.apache.iceberg.Schema icebergSchema) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    icebergSchema.columns().forEach(field -> schemaBuilder.addColumns(parseColumn(field)));
    return schemaBuilder.build();
  }

  private static Column parseColumn(Types.NestedField field) {
    Column.Builder columnBuilder = Column.newBuilder();

    columnBuilder.setId(field.fieldId());
    columnBuilder.setName(field.name());
    columnBuilder.setType(parseDataType(field.type()));
    columnBuilder.setNullable(field.isOptional());

    Type icebergType = field.type();

    if (icebergType instanceof Types.StructType
        || icebergType instanceof Types.MapType
        || icebergType instanceof Types.ListType) {
      // TODO: add recursive conversion
      throw new IllegalArgumentException("Unsupported iceberg column type");
    }

    return columnBuilder.build();
  }

  @SuppressWarnings("CyclomaticComplexity")
  private static Column.DataType parseDataType(Type icebergType) {
    if (icebergType instanceof Types.BooleanType) {
      return Column.DataType.BOOLEAN;
    } else if (icebergType instanceof Types.IntegerType) {
      return Column.DataType.INT4;
    } else if (icebergType instanceof Types.LongType) {
      return Column.DataType.INT8;
    } else if (icebergType instanceof Types.FloatType) {
      return Column.DataType.FLOAT4;
    } else if (icebergType instanceof Types.DoubleType) {
      return Column.DataType.FLOAT8;
    } else if (icebergType instanceof Types.DateType) {
      return Column.DataType.DATE;
    } else if (icebergType instanceof Types.TimeType) {
      return Column.DataType.TIME6;
    } else if (icebergType instanceof Types.TimestampType) {
      return ((Types.TimestampType) icebergType).shouldAdjustToUTC()
          ? Column.DataType.TIMESTAMPTZ6
          : Column.DataType.TIMESTAMP6;
    } else if (icebergType instanceof Types.FixedType) {
      return Column.DataType.FIXED;
    } else if (icebergType instanceof Types.BinaryType) {
      return Column.DataType.BINARY;
    } else if (icebergType instanceof Types.StringType) {
      return Column.DataType.VARCHAR;
    } else if (icebergType instanceof Types.UUIDType) {
      return Column.DataType.VARCHAR;
    } else if (icebergType instanceof Types.DecimalType) {
      return Column.DataType.DECIMAL;
    } else if (icebergType instanceof Types.StructType) {
      return Column.DataType.STRUCT;
    } else if (icebergType instanceof Types.ListType) {
      return Column.DataType.LIST;
    } else if (icebergType instanceof Types.MapType) {
      return Column.DataType.MAP;
    } else {
      throw new IllegalArgumentException("Unsupported Iceberg data type: " + icebergType);
    }
  }

  private static ReadRel parseSubstraitReadRel(ViewRepresentation viewRepr) {
    ValidationUtil.checkArgument(
        viewRepr instanceof SQLViewRepresentation,
        "view presentation cannot be null and must be SQLViewRepresentation");

    SQLViewRepresentation sqlViewRepr = (SQLViewRepresentation) viewRepr;
    ValidationUtil.checkArgument(
        IcebergFormatProperties.SUBSTRAIT_DIALECT.equalsIgnoreCase(sqlViewRepr.dialect()),
        "sql view representation must be substrait dialect");

    return SubstraitUtil.loadSubstraitReadReal(sqlViewRepr.sql());
  }

  private static List<NamespaceObjectFullName> parseFullNames(
      ReadRel substraitReadRel, OlympiaIcebergCatalogProperties catalogProps) {
    List<NamespaceObjectFullName> fullNames = Lists.newArrayList();

    if (substraitReadRel == null) {
      return fullNames;
    }

    for (String name : substraitReadRel.getNamedTable().getNamesList()) {
      TableIdentifier tblIdentifier = TableIdentifier.parse(name);
      IcebergTableInfo tableInfo = parseTableIdentifier(tblIdentifier, catalogProps);
      fullNames.add(
          NamespaceObjectFullName.newBuilder()
              .setNamespaceName(tableInfo.namespaceName())
              .setName(tableInfo.tableName())
              .build());
    }

    return fullNames;
  }
}

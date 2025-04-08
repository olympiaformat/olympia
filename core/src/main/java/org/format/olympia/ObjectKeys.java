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

import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.format.olympia.proto.objects.CatalogDef;
import org.format.olympia.relocated.com.google.common.collect.ImmutableSet;
import org.format.olympia.util.ValidationUtil;

public class ObjectKeys {

  public static final String CATALOG_DEFINITION = "catalog_def";
  public static final byte[] CATALOG_DEFINITION_BYTES =
      CATALOG_DEFINITION.getBytes(StandardCharsets.UTF_8);

  public static final String PREVIOUS_ROOT_NODE = "previous_root";
  public static final byte[] PREVIOUS_ROOT_NODE_BYTES =
      PREVIOUS_ROOT_NODE.getBytes(StandardCharsets.UTF_8);

  public static final String ROLLBACK_FROM_ROOT_NODE = "rollback_from_root";
  public static final byte[] ROLLBACK_FROM_ROOT_NODE_BYTES =
      ROLLBACK_FROM_ROOT_NODE.getBytes(StandardCharsets.UTF_8);

  public static final String NUMBER_OF_KEYS = "n_keys";
  public static final byte[] NUMBER_OF_KEYS_BYTES = NUMBER_OF_KEYS.getBytes(StandardCharsets.UTF_8);

  public static final String NUMBER_OF_ACTIONS = "n_actions";
  public static final byte[] NUMBER_OF_ACTIONS_BYTES =
      NUMBER_OF_ACTIONS.getBytes(StandardCharsets.UTF_8);

  public static final String CREATED_AT_MILLIS = "created_at_millis";
  public static final byte[] CREATED_AT_MILLIS_BYTES =
      CREATED_AT_MILLIS.getBytes(StandardCharsets.UTF_8);

  public static final Set<String> SYSTEM_INTERNAL_KEYS =
      ImmutableSet.<String>builder()
          .add(CATALOG_DEFINITION)
          .add(PREVIOUS_ROOT_NODE)
          .add(ROLLBACK_FROM_ROOT_NODE)
          .add(CREATED_AT_MILLIS)
          .add(NUMBER_OF_KEYS)
          .add(NUMBER_OF_ACTIONS)
          .build();

  private static final int ENCODED_OBJECT_TYPE_ID_PART_SIZE = 4;
  private static final String ENCODED_TYPE_ID_NAMESPACE = "B===";
  private static final String ENCODED_TYPE_ID_TABLE = "C===";
  private static final String ENCODED_TYPE_ID_VIEW = "D===";

  private ObjectKeys() {}

  public static String namespaceKey(String namespaceName, CatalogDef catalogDef) {
    ValidationUtil.checkNotNull(catalogDef, "Catalog definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(namespaceName, "namespace name must be provided");
    ValidationUtil.checkArgument(
        namespaceName.length() <= catalogDef.getNamespaceNameMaxSizeBytes(),
        "namespace name %s must be less than or equal to %s in catalog definition",
        namespaceName,
        catalogDef.getNamespaceNameMaxSizeBytes());

    StringBuilder sb = new StringBuilder();
    sb.append(ENCODED_TYPE_ID_NAMESPACE);
    sb.append(namespaceName);
    for (int i = 0; i < catalogDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }

    return sb.toString();
  }

  public static String namespaceNameFromKey(String namespaceKey, CatalogDef catalogDef) {
    ValidationUtil.checkArgument(
        isNamespaceKey(namespaceKey, catalogDef), "Invalid namespace key: %s", namespaceKey);
    return namespaceKey.substring(ENCODED_OBJECT_TYPE_ID_PART_SIZE).trim();
  }

  public static boolean isNamespaceKey(String key, CatalogDef catalogDef) {
    ValidationUtil.checkNotNull(catalogDef, "Catalog definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(key, "key must be provided");
    return key.startsWith(ENCODED_TYPE_ID_NAMESPACE)
        && key.length()
            == ENCODED_OBJECT_TYPE_ID_PART_SIZE + catalogDef.getNamespaceNameMaxSizeBytes();
  }

  public static String tableKey(String namespaceName, String tableName, CatalogDef catalogDef) {
    ValidationUtil.checkNotNull(catalogDef, "Catalog definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(namespaceName, "namespace name must be provided");
    ValidationUtil.checkNotNullOrEmptyString(tableName, "table name must be provided");
    ValidationUtil.checkArgument(
        namespaceName.length() <= catalogDef.getNamespaceNameMaxSizeBytes(),
        "namespace name %s must be less than or equal to %s in catalog definition",
        namespaceName,
        catalogDef.getNamespaceNameMaxSizeBytes());

    ValidationUtil.checkArgument(
        tableName.length() <= catalogDef.getTableNameMaxSizeBytes(),
        "table name %s must be less than or equal to %s in catalog definition",
        tableName,
        catalogDef.getTableNameMaxSizeBytes());

    StringBuilder sb = new StringBuilder();
    sb.append(ENCODED_TYPE_ID_TABLE);
    sb.append(namespaceName);
    for (int i = 0; i < catalogDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }

    sb.append(tableName);
    for (int i = 0; i < catalogDef.getTableNameMaxSizeBytes() - tableName.length(); i++) {
      sb.append(' ');
    }

    return sb.toString();
  }

  public static String tableNameFromKey(String tableKey, CatalogDef catalogDef) {
    ValidationUtil.checkArgument(
        isTableKey(tableKey, catalogDef), "Invalid table key: %s", tableKey);
    return tableKey
        .substring(ENCODED_OBJECT_TYPE_ID_PART_SIZE + catalogDef.getNamespaceNameMaxSizeBytes())
        .trim();
  }

  public static boolean isTableKey(String key, CatalogDef catalogDef) {
    ValidationUtil.checkNotNull(catalogDef, "Catalog definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(key, "key must be provided");
    return key.startsWith(ENCODED_TYPE_ID_TABLE)
        && key.length()
            == (ENCODED_OBJECT_TYPE_ID_PART_SIZE
                + catalogDef.getNamespaceNameMaxSizeBytes()
                + catalogDef.getTableNameMaxSizeBytes());
  }

  public static String tableKeyNamespacePrefix(String namespaceName, CatalogDef catalogDef) {
    StringBuilder sb =
        new StringBuilder(
            ENCODED_TYPE_ID_TABLE.length() + catalogDef.getNamespaceNameMaxSizeBytes());
    sb.append(ENCODED_TYPE_ID_TABLE);
    sb.append(namespaceName);
    for (int i = 0; i < catalogDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }
    return sb.toString();
  }

  public static String viewKey(String namespaceName, String viewName, CatalogDef catalogDef) {
    ValidationUtil.checkNotNull(catalogDef, "Catalog definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(namespaceName, "namespace name must be provided");
    ValidationUtil.checkNotNullOrEmptyString(viewName, "view name must be provided");
    ValidationUtil.checkArgument(
        namespaceName.length() <= catalogDef.getNamespaceNameMaxSizeBytes(),
        "namespace name %s must be less than or equal to %s in catalog definition",
        namespaceName,
        catalogDef.getNamespaceNameMaxSizeBytes());

    ValidationUtil.checkArgument(
        viewName.length() <= catalogDef.getViewNameMaxSizeBytes(),
        "view name %s must be less than or equal to %s in catalog definition",
        viewName,
        catalogDef.getViewNameMaxSizeBytes());

    StringBuilder sb = new StringBuilder();
    sb.append(ENCODED_TYPE_ID_VIEW);
    sb.append(namespaceName);
    for (int i = 0; i < catalogDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }

    sb.append(viewName);
    for (int i = 0; i < catalogDef.getViewNameMaxSizeBytes() - viewName.length(); i++) {
      sb.append(' ');
    }

    return sb.toString();
  }

  public static String viewNameFromKey(String viewKey, CatalogDef catalogDef) {
    ValidationUtil.checkArgument(isViewKey(viewKey, catalogDef), "Invalid view key: %s", viewKey);
    return viewKey
        .substring(ENCODED_OBJECT_TYPE_ID_PART_SIZE + catalogDef.getNamespaceNameMaxSizeBytes())
        .trim();
  }

  public static boolean isViewKey(String key, CatalogDef catalogDef) {
    ValidationUtil.checkNotNull(catalogDef, "Catalog definition must be provided");
    ValidationUtil.checkNotNullOrEmptyString(key, "key must be provided");
    return key.startsWith(ENCODED_TYPE_ID_VIEW)
        && key.length()
            == (ENCODED_OBJECT_TYPE_ID_PART_SIZE
                + catalogDef.getNamespaceNameMaxSizeBytes()
                + catalogDef.getViewNameMaxSizeBytes());
  }

  public static String viewKeyNamespacePrefix(String namespaceName, CatalogDef catalogDef) {
    StringBuilder sb =
        new StringBuilder(
            ENCODED_TYPE_ID_VIEW.length() + catalogDef.getNamespaceNameMaxSizeBytes());
    sb.append(ENCODED_TYPE_ID_VIEW);
    sb.append(namespaceName);
    for (int i = 0; i < catalogDef.getNamespaceNameMaxSizeBytes() - namespaceName.length(); i++) {
      sb.append(' ');
    }
    return sb.toString();
  }
}

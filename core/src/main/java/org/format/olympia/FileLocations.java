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

import java.util.UUID;
import java.util.regex.Pattern;
import org.format.olympia.relocated.com.google.common.base.Joiner;
import org.format.olympia.util.ValidationUtil;

public class FileLocations {

  private static final Joiner SLASH = Joiner.on("/");

  public static final String VERSION_DIR_NAME = "vn";

  public static final String DEFINITION_DIR_NAME = "def";

  public static final String NODE_DIR_NAME = "node";

  public static final String LATEST_VERSION_FILE_PATH = SLASH.join(VERSION_DIR_NAME, "latest");

  public static final String CATALOG_DEF_FILE_DIR = SLASH.join(DEFINITION_DIR_NAME, "catalog");

  public static final String NAMESPACE_DEF_FILE_DIR = SLASH.join(DEFINITION_DIR_NAME, "ns");

  public static final String TABLE_DEF_FILE_DIR = SLASH.join(DEFINITION_DIR_NAME, "table");

  public static final String VIEW_DEF_FILE_DIR = SLASH.join(DEFINITION_DIR_NAME, "view");

  public static final String DIST_TXN_DEF_FILE_DIR = SLASH.join(DEFINITION_DIR_NAME, "dtxn");

  public static final String PROTOBUF_BINARY_FILE_SUFFIX = ".binpb";

  public static final String ARROW_FILE_SUFFIX = ".arrow";

  private static final int ROOT_NODE_FILE_VERSION_BINARY_LENGTH = 64;

  private static final int ROOT_NODE_FILE_LENGTH = 67;

  private static final String ROOT_NODE_FILE_PREFIX_REVERSED = "/nv";

  private static final Pattern ROOT_NODE_FILE_PATH_PATTERN = Pattern.compile("^vn/[01]{64}$");

  private FileLocations() {}

  public static boolean isRootNodeFilePath(String path) {
    return ROOT_NODE_FILE_PATH_PATTERN.matcher(path).matches();
  }

  public static long versionFromNodeFilePath(String path) {
    ValidationUtil.checkArgument(
        isRootNodeFilePath(path),
        "Root node file path must match pattern: %s",
        ROOT_NODE_FILE_PATH_PATTERN);
    String reversedBinary = path.substring(3); // after vn/
    String binary = new StringBuilder().append(reversedBinary).reverse().toString();
    return Long.parseLong(binary, 2);
  }

  public static String rootNodeFilePath(long version) {
    ValidationUtil.checkArgument(version >= 0, "version must be non-negative");
    StringBuilder sb = new StringBuilder(ROOT_NODE_FILE_LENGTH);
    String binaryLong = Long.toBinaryString(version);
    for (int i = 0; i < ROOT_NODE_FILE_VERSION_BINARY_LENGTH - binaryLong.length(); i++) {
      sb.append("0");
    }
    sb.append(binaryLong);
    sb.append(ROOT_NODE_FILE_PREFIX_REVERSED);
    return sb.reverse().toString();
  }

  public static String newCatalogDefFilePath() {
    return CATALOG_DEF_FILE_DIR + '/' + UUID.randomUUID() + PROTOBUF_BINARY_FILE_SUFFIX;
  }

  public static String newNamespaceDefFilePath(String namespaceName) {
    return NAMESPACE_DEF_FILE_DIR
        + '/'
        + UUID.randomUUID()
        + '-'
        + namespaceName
        + PROTOBUF_BINARY_FILE_SUFFIX;
  }

  public static String newTableDefFilePath(String namespaceName, String tableName) {
    return TABLE_DEF_FILE_DIR
        + '/'
        + UUID.randomUUID()
        + '-'
        + namespaceName
        + '-'
        + tableName
        + PROTOBUF_BINARY_FILE_SUFFIX;
  }

  public static String newViewDefFilePath(String namespaceName, String viewName) {
    return VIEW_DEF_FILE_DIR
        + '/'
        + UUID.randomUUID()
        + '-'
        + namespaceName
        + '-'
        + viewName
        + PROTOBUF_BINARY_FILE_SUFFIX;
  }

  public static String distTransactionDefFilePath(String transactionId) {
    return DIST_TXN_DEF_FILE_DIR + '/' + transactionId + PROTOBUF_BINARY_FILE_SUFFIX;
  }

  public static String newNodeFilePath() {
    return NODE_DIR_NAME + '/' + UUID.randomUUID() + ARROW_FILE_SUFFIX;
  }
}

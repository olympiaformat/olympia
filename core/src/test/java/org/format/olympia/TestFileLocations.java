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

import org.format.olympia.exception.InvalidArgumentException;
import org.junit.jupiter.api.Test;

public class TestFileLocations {

  @Test
  public void testVersionToRootNodeFilePath() {
    assertThat(FileLocations.rootNodeFilePath(1))
        .isEqualTo("vn/1000000000000000000000000000000000000000000000000000000000000000");

    assertThat(FileLocations.rootNodeFilePath(12345678))
        .isEqualTo("vn/0111001010000110001111010000000000000000000000000000000000000000");

    assertThat(FileLocations.rootNodeFilePath(9223372036854775802L))
        .isEqualTo("vn/0101111111111111111111111111111111111111111111111111111111111110");

    assertThatThrownBy(() -> FileLocations.rootNodeFilePath(-1))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("version must be non-negative");
  }

  @Test
  public void testRootNodeFilePathToVersion() {
    assertThat(
            FileLocations.versionFromNodeFilePath(
                "vn/1000000000000000000000000000000000000000000000000000000000000000"))
        .isEqualTo(1);

    assertThat(
            FileLocations.versionFromNodeFilePath(
                "vn/0111001010000110001111010000000000000000000000000000000000000000"))
        .isEqualTo(12345678);

    assertThat(
            FileLocations.versionFromNodeFilePath(
                "vn/0101111111111111111111111111111111111111111111111111111111111110"))
        .isEqualTo(9223372036854775802L);

    assertThat(FileLocations.isRootNodeFilePath("invalid.txt")).isFalse();
    assertThatThrownBy(() -> FileLocations.versionFromNodeFilePath("invalid.txt"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Root node file path must match pattern");

    assertThat(
            FileLocations.isRootNodeFilePath(
                "vn/2000000000000000000000000000000000000000000000000000000000000000"))
        .isFalse();
    assertThatThrownBy(
            () ->
                FileLocations.versionFromNodeFilePath(
                    "vn/2000000000000000000000000000000000000000000000000000000000000000"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Root node file path must match pattern");
  }

  @Test
  public void testCatalogDefFilePath() {
    assertThat(FileLocations.newCatalogDefFilePath())
        .startsWith(FileLocations.CATALOG_DEF_FILE_DIR)
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(
            FileLocations.CATALOG_DEF_FILE_DIR.length()
                + 36
                + 1
                + FileLocations.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }

  @Test
  public void testNamespaceDefFilePath() {
    assertThat(FileLocations.newNamespaceDefFilePath("ns1"))
        .contains("ns1")
        .startsWith(FileLocations.NAMESPACE_DEF_FILE_DIR)
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(
            FileLocations.NAMESPACE_DEF_FILE_DIR.length()
                + "ns1".length()
                + 36
                + 2
                + FileLocations.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }

  @Test
  public void testTableDefFilePath() {
    assertThat(FileLocations.newTableDefFilePath("ns1", "t1"))
        .contains("ns1")
        .contains("t1")
        .startsWith(FileLocations.TABLE_DEF_FILE_DIR)
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(
            FileLocations.TABLE_DEF_FILE_DIR.length()
                + "ns1".length()
                + "t1".length()
                + 36
                + 3
                + FileLocations.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }

  @Test
  public void testViewDefFilePath() {
    assertThat(FileLocations.newViewDefFilePath("ns1", "v1"))
        .contains("view")
        .contains("ns1")
        .contains("v1")
        .startsWith(FileLocations.VIEW_DEF_FILE_DIR)
        .endsWith(FileLocations.PROTOBUF_BINARY_FILE_SUFFIX)
        .hasSize(
            FileLocations.VIEW_DEF_FILE_DIR.length()
                + "ns1".length()
                + "v1".length()
                + 36
                + 3
                + FileLocations.PROTOBUF_BINARY_FILE_SUFFIX.length());
  }

  @Test
  public void testDistTransactionDefFilePath() {
    assertThat(FileLocations.distTransactionDefFilePath("tid1")).isEqualTo("def/dtxn/tid1.binpb");
  }

  @Test
  public void testNewNodeFilePath() {
    assertThat(FileLocations.newNodeFilePath())
        .startsWith(FileLocations.NODE_DIR_NAME)
        .endsWith(FileLocations.ARROW_FILE_SUFFIX)
        .hasSize(
            FileLocations.NODE_DIR_NAME.length()
                + 36
                + 1
                + FileLocations.ARROW_FILE_SUFFIX.length());
  }
}

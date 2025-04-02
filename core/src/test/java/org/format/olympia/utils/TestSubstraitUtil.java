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
package org.format.olympia.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.ByteString;
import io.substrait.proto.NamedObjectWrite;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.WriteRel;
import org.format.olympia.exception.InvalidArgumentException;
import org.format.olympia.util.SubstraitUtil;
import org.junit.jupiter.api.Test;

public class TestSubstraitUtil {

  @Test
  public void testValidSubstraitReadRel() {
    ReadRel readRel =
        ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("person"))
            .setBaseSchema(NamedStruct.newBuilder().addNames("name").build())
            .build();
    ByteString validReadRelByteString = ByteString.copyFrom(readRel.toByteArray());

    assertThat(SubstraitUtil.loadSubstraitReadReal(validReadRelByteString)).isEqualTo(readRel);
  }

  @Test
  public void testNonSubstraitReadRel() {
    WriteRel writeRel =
        WriteRel.newBuilder()
            .setNamedTable(NamedObjectWrite.newBuilder().addNames("person").build())
            .setTableSchema(NamedStruct.newBuilder().addNames("name").build())
            .build();
    ByteString writeRelByteString = ByteString.copyFrom(writeRel.toByteArray());

    // Expect IllegalArgumentException when passing invalid data
    assertThatThrownBy(() -> SubstraitUtil.loadSubstraitReadReal(writeRelByteString))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Invalid Substrait read relation");
  }

  @Test
  public void testInvalidSubstraitPlan() {
    ByteString invalidPlan = ByteString.copyFromUtf8("non substrait plan");

    // Expect IllegalArgumentException when passing invalid data
    assertThatThrownBy(() -> SubstraitUtil.loadSubstraitReadReal(invalidPlan))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("Invalid Substrait read relation");
  }

  @Test
  public void testEmptySubstraitPlan() {
    ByteString emptyPlan = ByteString.EMPTY;
    ReadRel emptyReadRel = ReadRel.newBuilder().build();

    assertThat(SubstraitUtil.loadSubstraitReadReal(emptyPlan)).isEqualTo(emptyReadRel);
  }

  @Test
  public void testNullSubstraitPlan() {
    ByteString nullPlan = null;

    assertThatThrownBy(() -> SubstraitUtil.loadSubstraitReadReal(nullPlan))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessageContaining("substraitReadRelBytes cannot be null");
  }
}

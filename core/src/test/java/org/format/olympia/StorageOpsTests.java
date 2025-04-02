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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.format.olympia.relocated.com.google.common.io.CharStreams;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.StorageOps;
import org.junit.jupiter.api.Test;

public abstract class StorageOpsTests {

  protected abstract StorageOps storageOps();

  protected abstract LiteralURI createFileUri(String fileName);

  protected abstract LiteralURI createDirectoryUri(String directory);

  @Test
  public void testFileExists() throws IOException {
    StorageOps ops = storageOps();
    String fileName = UUID.randomUUID() + ".txt";
    LiteralURI fileUri = createFileUri(fileName);

    assertThat(ops.exists(fileUri)).isFalse();

    writeDataToFile(fileUri, "test data");
    assertThat(ops.exists(fileUri)).isTrue();
  }

  @Test
  public void testDeleteFiles() {
    StorageOps ops = storageOps();

    List<LiteralURI> files =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  try {
                    String fileName = UUID.randomUUID() + ".txt";
                    LiteralURI fileUri = createFileUri(fileName);
                    writeDataToFile(fileUri, "test data");
                    return fileUri;
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    files.stream().forEach(f -> assertThat(ops.exists(f)).isTrue());
    ops.delete(files);
    files.stream().forEach(f -> assertThat(ops.exists(f)).isFalse());
  }

  @Test
  public void testDeleteNonExistentFile() {
    StorageOps ops = storageOps();

    String fileName = "nonexistent.txt";
    LiteralURI fileUri = createFileUri(fileName);

    assertThat(ops.exists(fileUri)).isFalse();

    ops.delete(List.of(fileUri));

    assertThat(ops.exists(fileUri)).isFalse();
  }

  @Test
  public void testReadFile() throws IOException {
    StorageOps ops = storageOps();

    String fileName = UUID.randomUUID() + ".txt";
    LiteralURI fileUri = createFileUri(fileName);
    writeDataToFile(fileUri, "test data");
    InputStream stream = ops.startRead(fileUri);

    assertThat(CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8)))
        .isEqualTo("test data");
  }

  @Test
  public void testWriteFileWithOverwrite() throws IOException {
    StorageOps ops = storageOps();

    String fileName = UUID.randomUUID() + ".txt";
    LiteralURI fileUri = createFileUri(fileName);
    String testData1 = "test data 1";
    String testData2 = "test data 2";

    try (OutputStream stream = ops.startOverwrite(fileUri)) {
      stream.write(testData1.getBytes(StandardCharsets.UTF_8));
    }

    try (InputStream stream = ops.startRead(fileUri)) {
      String readData = CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8));
      assertThat(readData).isEqualTo(testData1);
    }

    try (OutputStream stream = ops.startOverwrite(fileUri)) {
      stream.write(testData2.getBytes(StandardCharsets.UTF_8));
    }

    try (InputStream stream = ops.startRead(fileUri)) {
      String readData = CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8));
      assertThat(readData).isEqualTo(testData2);
    }
  }

  @Test
  public void testReadFileLocal() throws IOException {
    StorageOps ops = storageOps();

    String fileName = UUID.randomUUID() + ".txt";
    LiteralURI fileUri = createFileUri(fileName);
    writeDataToFile(fileUri, "test data");

    ops.prepareToReadLocal(fileUri);
    InputStream stream = ops.startReadLocal(fileUri);
    assertThat(CharStreams.toString(new InputStreamReader(stream, StandardCharsets.UTF_8)))
        .isEqualTo("test data");
  }

  @Test
  public void testWriteFile() throws IOException {
    StorageOps ops = storageOps();

    String fileName = UUID.randomUUID() + ".txt";
    LiteralURI fileUri = createFileUri(fileName);
    try (OutputStream stream = ops.startCommit(fileUri)) {
      stream.write("data".getBytes(StandardCharsets.UTF_8));
    }

    assertThat(ops.exists(fileUri)).isTrue();
    try (InputStream stream = ops.startRead(fileUri)) {
      String data = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      assertThat(data).isEqualTo("data");
    }
  }

  @Test
  public void testListing() throws IOException {
    StorageOps ops = storageOps();

    LiteralURI directory = createDirectoryUri("warehouse");
    System.out.println(directory);
    for (int i = 0; i < 10; i++) {
      String fileName = UUID.randomUUID() + ".txt";
      LiteralURI fileUri = directory.extendPath(fileName);
      writeDataToFile(fileUri, "test data");
    }

    List<LiteralURI> results = ops.list(directory);
    assertThat(results.size()).isEqualTo(10);
  }

  protected void writeDataToFile(LiteralURI fileUri, String data) throws IOException {
    StorageOps ops = storageOps();
    try (OutputStream stream = ops.startCommit(fileUri)) {
      stream.write(data.getBytes(StandardCharsets.UTF_8));
    }
  }
}

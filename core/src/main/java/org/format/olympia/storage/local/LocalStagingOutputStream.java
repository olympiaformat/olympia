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
package org.format.olympia.storage.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.format.olympia.exception.StoragePathNotFoundException;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LocalStagingOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(LocalStagingOutputStream.class);

  private final Path targetFilePath;
  private final Path stagingFilePath;
  private final FileOutputStream stream;

  protected LocalStagingOutputStream(
      Path targetFilePath,
      CommonStorageOpsProperties commonProperties,
      LocalStorageOpsProperties localProperties) {
    this.targetFilePath = targetFilePath;
    File stagingFile = FileUtil.createTempFile("local-", commonProperties.writeStagingDirectory());
    this.stagingFilePath = stagingFile.toPath();
    LOG.debug("Created temporary file for staging write: {}", stagingFile);
    try {
      this.stream = new FileOutputStream(stagingFile);
    } catch (FileNotFoundException e) {
      throw new StoragePathNotFoundException(e);
    }
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    stream.write(bytes);
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] bytes, int off, int len) throws IOException {
    stream.write(bytes, off, len);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  public FileChannel getChannel() {
    return stream.getChannel();
  }

  protected Path getTargetFilePath() {
    return targetFilePath;
  }

  protected Path getStagingFilePath() {
    return stagingFilePath;
  }

  protected void createParentDirectories() throws IOException {
    Files.createDirectories(targetFilePath.getParent());
  }

  protected abstract void commit() throws IOException;

  @Override
  public void close() throws IOException {
    try {
      commit();
    } finally {
      stream.close();
    }
  }
}

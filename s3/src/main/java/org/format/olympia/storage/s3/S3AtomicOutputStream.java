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
package org.format.olympia.storage.s3;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import org.format.olympia.exception.CommitFailureException;
import org.format.olympia.storage.AtomicOutputStream;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3AtomicOutputStream extends AtomicOutputStream {
  private final S3StagingOutputStream delegate;

  S3AtomicOutputStream(
      S3AsyncClient s3,
      LiteralURI localUri,
      LiteralURI s3Uri,
      CommonStorageOpsProperties commonProperties,
      S3StorageOpsProperties s3Properties) {
    this.delegate =
        new S3StagingOutputStream(s3, localUri, s3Uri, commonProperties, s3Properties) {
          @Override
          protected void commit() throws IOException {
            createParentDirectories();
            Files.move(getStagingFilePath(), getTargetFilePath());
            LiteralURI targetURI = getTargetPath();
            PutObjectRequest.Builder requestBuilder =
                PutObjectRequest.builder()
                    .bucket(targetURI.authority())
                    .key(targetURI.path())
                    .ifNoneMatch("*");
            uploadToS3(requestBuilder);
          }
        };
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void write(int b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(byte[] bytes) throws IOException {
    delegate.write(bytes);
  }

  @Override
  public void atomicallySeal() throws CommitFailureException, IOException {
    delegate.close();
  }

  @Override
  public FileChannel channel() {
    return delegate.getChannel();
  }
}

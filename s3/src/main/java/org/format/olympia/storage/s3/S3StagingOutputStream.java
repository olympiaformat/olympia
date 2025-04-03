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
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import org.format.olympia.exception.StorageWriteFailureException;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.local.LocalStagingOutputStream;
import org.format.olympia.storage.local.LocalStorageOpsProperties;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public abstract class S3StagingOutputStream extends LocalStagingOutputStream {
  private final S3AsyncClient s3;
  private final LiteralURI s3Uri;

  public S3StagingOutputStream(
      S3AsyncClient s3,
      LiteralURI localUri,
      LiteralURI s3Uri,
      CommonStorageOpsProperties commonProperties,
      S3StorageOpsProperties s3Properties) {
    super(Path.of(localUri.path()), commonProperties, LocalStorageOpsProperties.instance());
    this.s3 = s3;
    this.s3Uri = s3Uri;
    // TODO: given the file size is limited to 1MB by default, is it still worth buffering to
    // staging file?
    // pending benchmarking to confirm.
  }

  private S3AsyncClient getS3Client() {
    return s3;
  }

  protected LiteralURI getTargetPath() {
    return s3Uri;
  }

  protected void uploadToS3(PutObjectRequest.Builder requestBuilder) {
    try {
      getS3Client()
          .putObject(requestBuilder.build(), AsyncRequestBody.fromFile(getTargetFilePath()))
          .get();
    } catch (ExecutionException | InterruptedException e) {
      throw new StorageWriteFailureException(
          e, "Fail to upload to %s from staging file: %s", getTargetPath(), getStagingFilePath());
    }
  }

  @Override
  public void close() throws IOException {
    commit();
  }
}

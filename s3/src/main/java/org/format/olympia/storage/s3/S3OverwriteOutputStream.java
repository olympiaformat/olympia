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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3OverwriteOutputStream extends S3StagingOutputStream {
  public S3OverwriteOutputStream(
      S3AsyncClient s3,
      LiteralURI localUri,
      LiteralURI s3Uri,
      CommonStorageOpsProperties commonProperties,
      S3StorageOpsProperties s3Properties) {
    super(s3, localUri, s3Uri, commonProperties, s3Properties);
  }

  @Override
  protected void commit() throws IOException {
    createParentDirectories();
    Files.move(getStagingFilePath(), getTargetFilePath(), StandardCopyOption.REPLACE_EXISTING);

    LiteralURI targetUri = getTargetPath();
    PutObjectRequest.Builder requestBuilder =
        PutObjectRequest.builder().bucket(targetUri.authority()).key(targetUri.path());

    uploadToS3(requestBuilder);
  }
}

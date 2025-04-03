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

import static org.format.olympia.storage.s3.S3StorageOpsProperties.S3_ACCESS_KEY_ID;
import static org.format.olympia.storage.s3.S3StorageOpsProperties.S3_ENDPOINT;
import static org.format.olympia.storage.s3.S3StorageOpsProperties.S3_PATH_STYLE_ACCESS;
import static org.format.olympia.storage.s3.S3StorageOpsProperties.S3_REGION;
import static org.format.olympia.storage.s3.S3StorageOpsProperties.S3_SECRET_ACCESS_KEY;

import java.net.URI;
import java.util.Map;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class MinioTestUtil {

  private MinioTestUtil() {}

  public static MinIOContainer createContainer() {
    return new MinIOContainer(DockerImageName.parse("minio/minio:latest"))
        .withEnv("MINIO_DOMAIN", "localhost");
  }

  public static S3AsyncClient createS3Client(MinIOContainer container) {
    return S3AsyncClient.builder()
        .endpointOverride(URI.create(container.getS3URL()))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(container.getUserName(), container.getPassword())))
        .region(Region.US_EAST_1)
        .forcePathStyle(true)
        .build();
  }

  public static Map<String, String> createS3PropertiesMap(MinIOContainer container) {
    String endpoint = container.getS3URL();
    String accessKey = container.getUserName();
    String secretAccessKey = container.getPassword();
    return ImmutableMap.of(
        S3_ENDPOINT,
        endpoint,
        S3_ACCESS_KEY_ID,
        accessKey,
        S3_SECRET_ACCESS_KEY,
        secretAccessKey,
        S3_REGION,
        String.valueOf(Region.US_EAST_1),
        S3_PATH_STYLE_ACCESS,
        "true");
  }
}

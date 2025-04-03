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

import java.io.File;
import java.net.URI;
import java.util.UUID;
import org.format.olympia.Olympia;
import org.format.olympia.OlympiaTests;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.storage.BasicCatalogStorage;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.StorageOps;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
public class TestS3StorageOlympiaTests extends OlympiaTests {
  @Container private static final MinIOContainer MINIO = MinioTestUtil.createContainer();

  @TempDir private File tempDir;
  private static S3AsyncClient s3Client;
  private CatalogStorage storage;

  @BeforeAll
  public static void setupClient() {
    s3Client =
        S3AsyncClient.builder()
            .endpointOverride(URI.create(MINIO.getS3URL()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build();
  }

  @BeforeEach
  public void beforeEach() {
    String tempBucket = String.valueOf(UUID.randomUUID());
    s3Client.createBucket(CreateBucketRequest.builder().bucket(tempBucket).build()).join();
    S3StorageOpsProperties s3StorageOpsProperties =
        new S3StorageOpsProperties(MinioTestUtil.createS3PropertiesMap(MINIO));

    CommonStorageOpsProperties props =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir + "/tmp-write",
                CommonStorageOpsProperties.PREPARE_READ_STAGING_DIRECTORY, tempDir + "/tmp-read"));

    StorageOps storageOps = new S3StorageOps(props, s3StorageOpsProperties);

    this.storage = new BasicCatalogStorage(new LiteralURI("s3://" + tempBucket), storageOps);

    Olympia.createCatalog(storage, CATALOG_DEF);
  }

  @Override
  protected CatalogStorage storage() {
    return storage;
  }
}

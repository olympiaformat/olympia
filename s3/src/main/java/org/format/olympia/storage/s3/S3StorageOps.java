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

import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.format.olympia.exception.StorageDeleteFailureException;
import org.format.olympia.exception.StorageReadFailureException;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.relocated.com.google.common.collect.Maps;
import org.format.olympia.relocated.com.google.common.collect.Multimaps;
import org.format.olympia.relocated.com.google.common.collect.SetMultimap;
import org.format.olympia.relocated.com.google.common.collect.Sets;
import org.format.olympia.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.format.olympia.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.format.olympia.storage.AtomicOutputStream;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import org.format.olympia.storage.SeekableInputStream;
import org.format.olympia.storage.StorageOps;
import org.format.olympia.storage.StorageOpsProperties;
import org.format.olympia.storage.local.LocalInputStream;
import org.format.olympia.storage.local.LocalStorageOps;
import org.format.olympia.storage.local.LocalStorageOpsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;

public class S3StorageOps implements StorageOps {

  private static final Logger LOG = LoggerFactory.getLogger(S3StorageOps.class);

  private static volatile ExecutorService executorService;

  private CommonStorageOpsProperties commonProperties;
  private S3StorageOpsProperties s3Properties;

  private transient volatile S3AsyncClient s3;
  private transient volatile S3TransferManager transferManager;
  private final LocalStorageOps localStorageOps;

  public S3StorageOps() {
    this(CommonStorageOpsProperties.instance(), S3StorageOpsProperties.instance());
  }

  public S3StorageOps(
      CommonStorageOpsProperties commonProperties, S3StorageOpsProperties s3Properties) {
    this.commonProperties = commonProperties;
    this.s3Properties = s3Properties;
    this.localStorageOps =
        new LocalStorageOps(commonProperties, LocalStorageOpsProperties.instance());
    initializeS3AsyncClient();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.commonProperties = new CommonStorageOpsProperties(properties);
    this.s3Properties = new S3StorageOpsProperties(properties);
    initializeS3AsyncClient();
  }

  @Override
  public CommonStorageOpsProperties commonProperties() {
    return commonProperties;
  }

  @Override
  public StorageOpsProperties systemSpecificProperties() {
    return s3Properties;
  }

  @Override
  public void prepareToReadLocal(LiteralURI uri) {
    LiteralURI localUri = s3ToLocalUri(uri);
    if (!localStorageOps.exists(localUri)) {
      downloadFromS3(uri, localUri);
    }
  }

  @Override
  public LocalInputStream startReadLocal(LiteralURI uri) {
    LiteralURI localUri = s3ToLocalUri(uri);
    if (!localStorageOps.exists(localUri)) {
      downloadFromS3(uri, localUri);
    }
    return localStorageOps.startReadLocal(localUri);
  }

  @Override
  public SeekableInputStream startRead(LiteralURI uri) {
    LiteralURI localUri = s3ToLocalUri(uri);
    if (localStorageOps.exists(localUri)) {
      localStorageOps.startRead(localUri);
    }
    LOG.info("Start read without preparation, directly open S3 object: {}", uri);
    return new S3InputStream(s3(), uri);
  }

  @Override
  public boolean exists(LiteralURI uri) {
    try {
      s3().headObject(HeadObjectRequest.builder().bucket(uri.authority()).key(uri.path()).build())
          .get();
      return true;
    } catch (ExecutionException e) {
      return false;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public AtomicOutputStream startCommit(LiteralURI uri) {
    LiteralURI localUri = s3ToLocalUri(uri);
    return new S3AtomicOutputStream(s3(), localUri, uri, commonProperties, s3Properties);
  }

  @Override
  public OutputStream startOverwrite(LiteralURI uri) {
    LiteralURI localUri = s3ToLocalUri(uri);
    return new S3OverwriteOutputStream(s3(), localUri, uri, commonProperties, s3Properties);
  }

  @Override
  public void delete(List<LiteralURI> uris) {
    SetMultimap<String, String> bucketToObjects =
        Multimaps.newSetMultimap(Maps.newHashMap(), Sets::newHashSet);
    List<Future<List<String>>> deletionTasks = Lists.newArrayList();

    for (LiteralURI uri : uris) {
      String bucket = uri.authority();
      String objectKey = uri.path();
      bucketToObjects.get(bucket).add(objectKey);
      if (bucketToObjects.get(bucket).size() == commonProperties().deleteBatchSize()) {
        Set<String> keys = Sets.newHashSet(bucketToObjects.get(bucket));
        Future<List<String>> deletionTask =
            executorService().submit(() -> deleteBatch(bucket, keys));
        deletionTasks.add(deletionTask);
        bucketToObjects.removeAll(bucket);
      }
    }

    // Delete the remainder
    for (Map.Entry<String, Collection<String>> bucketToObjectsEntry :
        bucketToObjects.asMap().entrySet()) {
      String bucket = bucketToObjectsEntry.getKey();
      Collection<String> keys = bucketToObjectsEntry.getValue();
      Future<List<String>> deletionTask = executorService().submit(() -> deleteBatch(bucket, keys));
      deletionTasks.add(deletionTask);
    }

    int totalFailedDeletions = 0;
    for (Future<List<String>> deletionTask : deletionTasks) {
      try {
        List<String> failedDeletions = deletionTask.get();
        failedDeletions.forEach(path -> LOG.warn("Failed to delete object at path {}", path));
        totalFailedDeletions += failedDeletions.size();
      } catch (ExecutionException e) {
        LOG.warn("Caught unexpected exception during batch deletion: ", e.getCause());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        deletionTasks.stream().filter(task -> !task.isDone()).forEach(task -> task.cancel(true));
        throw new RuntimeException("Interrupted when waiting for deletions to complete", e);
      }
    }

    if (totalFailedDeletions > 0) {
      throw new StorageDeleteFailureException(
          "Failed to delete all files, remaining: %d", totalFailedDeletions);
    }
  }

  @Override
  public List<LiteralURI> list(LiteralURI prefix) {
    ListObjectsV2Request request =
        ListObjectsV2Request.builder().bucket(prefix.authority()).prefix(prefix.path()).build();

    List<LiteralURI> result = Lists.newArrayList();
    s3().listObjectsV2Paginator(request)
        .flatMapIterable(ListObjectsV2Response::contents)
        .map(obj -> new LiteralURI(prefix.scheme(), prefix.authority(), obj.key()))
        .subscribe(result::add)
        .join();
    return result;
  }

  private List<String> deleteBatch(String bucket, Collection<String> keysToDelete) {
    List<ObjectIdentifier> objectIds =
        keysToDelete.stream()
            .map(key -> ObjectIdentifier.builder().key(key).build())
            .collect(Collectors.toList());
    DeleteObjectsRequest request =
        DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(Delete.builder().objects(objectIds).build())
            .build();
    List<String> failures = Lists.newArrayList();
    try {
      DeleteObjectsResponse response = s3().deleteObjects(request).get();
      if (response.hasErrors()) {
        failures.addAll(
            response.errors().stream()
                .map(error -> String.format("s3://%s/%s", request.bucket(), error.key()))
                .collect(Collectors.toList()));
      }
    } catch (Exception e) {
      LOG.warn("Encountered failure when deleting batch", e);
      failures.addAll(
          request.delete().objects().stream()
              .map(obj -> String.format("s3://%s/%s", request.bucket(), obj.key()))
              .collect(Collectors.toList()));
    }
    return failures;
  }

  private LiteralURI s3ToLocalUri(LiteralURI s3Uri) {
    Path cacheDirPath = Paths.get(s3Properties.s3CacheDirectory());
    String bucket = s3Uri.authority();
    Path localPath = cacheDirPath;
    localPath = localPath.resolve(bucket);
    String objectKey = s3Uri.path().substring(1);
    localPath = localPath.resolve(objectKey).toAbsolutePath();
    LOG.debug("Mapped S3 URI {} to local path {}", s3Uri, localPath);
    return new LiteralURI("file", "", localPath.toString());
  }

  private S3AsyncClient s3() {
    if (s3 == null) {
      synchronized (this) {
        if (s3 == null) {
          initializeS3AsyncClient();
        }
      }
    }
    return s3;
  }

  private S3TransferManager transferManager() {
    if (s3 == null) {
      synchronized (this) {
        if (s3 == null) {
          initializeS3AsyncClient();
        }
      }
    }
    return transferManager;
  }

  private void initializeS3AsyncClient() {
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    if (s3Properties.region() != null) {
      builder.region(Region.of(s3Properties.region()));
    }

    if (s3Properties.endpoint() != null) {
      builder.endpointOverride(URI.create(s3Properties.endpoint()));
    }

    if (s3Properties.pathStyleAccess() != null) {
      builder.forcePathStyle(Boolean.valueOf(s3Properties.pathStyleAccess()));
    }

    if (s3Properties.accessKeyId() != null) {
      if (s3Properties.sessionToken() != null) {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsSessionCredentials.create(
                    s3Properties.accessKeyId(),
                    s3Properties.secretAccessKey(),
                    s3Properties.sessionToken())));
      } else {
        builder.credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    s3Properties.accessKeyId(), s3Properties.secretAccessKey())));
      }
    }
    this.s3 = builder.build();
    this.transferManager = S3TransferManager.builder().s3Client(s3).build();
  }

  private void downloadFromS3(LiteralURI s3Uri, LiteralURI localUri) {
    try {
      Path localPath = Paths.get(localUri.toString());
      Files.createDirectories(localPath.getParent());
      DownloadFileRequest downloadFileRequest =
          DownloadFileRequest.builder()
              .getObjectRequest(b -> b.bucket(s3Uri.authority()).key(s3Uri.path()))
              .destination(localPath)
              .build();
      transferManager().downloadFile(downloadFileRequest).completionFuture().join();
    } catch (Exception e) {
      throw new StorageReadFailureException(e, "Failed to download %s from S3", s3Uri);
    }
  }

  private ExecutorService executorService() {
    if (executorService == null) {
      synchronized (S3StorageOps.class) {
        if (executorService == null) {
          executorService =
              MoreExecutors.getExitingExecutorService(
                  (ThreadPoolExecutor)
                      Executors.newFixedThreadPool(
                          Runtime.getRuntime().availableProcessors(),
                          new ThreadFactoryBuilder()
                              .setDaemon(true)
                              .setNameFormat("trinitylake-s3-%d")
                              .build()));
        }
      }
    }

    return executorService;
  }

  @Override
  public void close() {
    if (s3 != null) {
      try {
        s3.close();
      } catch (Exception e) {
        LOG.warn("Failed to close s3 client", e);
      }
    }
  }
}

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
package org.format.olympia.storage;

import java.util.Map;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.util.InitializationUtil;
import org.format.olympia.util.PropertyUtil;
import org.format.olympia.util.ValidationUtil;

public class CatalogStorages {

  public static final String STORAGE_TYPE = "storage.type";
  public static final String STORAGE_TYPE_LOCAL = "local";
  public static final String STORAGE_TYPE_S3 = "s3";
  public static final String STORAGE_TYPE_DEFAULT = STORAGE_TYPE_LOCAL;

  public static final String STORAGE_ROOT = "storage.root";

  private static final Map<String, String> STORAGE_TYPE_TO_IMPL =
      ImmutableMap.<String, String>builder()
          .put(STORAGE_TYPE_LOCAL, "org.format.olympia.storage.local.LocalStorageOps")
          .put(STORAGE_TYPE_S3, "org.format.olympia.storage.s3.AmazonS3StorageOps")
          .build();

  private static final Map<String, String> STORAGE_SCHEME_TO_TYPE =
      ImmutableMap.<String, String>builder()
          .put("file", STORAGE_TYPE_LOCAL)
          .put("s3", STORAGE_TYPE_S3)
          .build();

  private CatalogStorages() {}

  public static CatalogStorage initialize(Map<String, String> properties) {
    LiteralURI storageRoot =
        new LiteralURI(PropertyUtil.propertyAsString(properties, STORAGE_ROOT));
    String storageType = PropertyUtil.propertyAsNullableString(properties, STORAGE_TYPE);

    if (storageType == null) {
      ValidationUtil.checkArgument(
          STORAGE_SCHEME_TO_TYPE.containsKey(storageRoot.scheme()),
          "Cannot infer storage type from root: %s",
          storageRoot);

      storageType = STORAGE_SCHEME_TO_TYPE.get(storageRoot.scheme());
    }

    // if not found in default mapping, just treat type as the impl
    String storageImpl = STORAGE_TYPE_TO_IMPL.getOrDefault(storageType, storageType);

    StorageOps storageOps =
        InitializationUtil.loadInitializable(storageImpl, properties, StorageOps.class);
    return new BasicCatalogStorage(storageRoot, storageOps);
  }
}

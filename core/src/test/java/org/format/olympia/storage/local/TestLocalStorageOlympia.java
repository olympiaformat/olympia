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
import org.format.olympia.Olympia;
import org.format.olympia.OlympiaTests;
import org.format.olympia.relocated.com.google.common.collect.ImmutableMap;
import org.format.olympia.storage.BasicCatalogStorage;
import org.format.olympia.storage.CatalogStorage;
import org.format.olympia.storage.CommonStorageOpsProperties;
import org.format.olympia.storage.LiteralURI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class TestLocalStorageOlympia extends OlympiaTests {

  @TempDir private File tempDir;

  private CatalogStorage storage;

  @BeforeEach
  public void beforeEach() {
    CommonStorageOpsProperties props =
        new CommonStorageOpsProperties(
            ImmutableMap.of(
                CommonStorageOpsProperties.WRITE_STAGING_DIRECTORY, tempDir + "/tmp-write",
                CommonStorageOpsProperties.PREPARE_READ_STAGING_DIRECTORY, tempDir + "/tmp-read"));

    this.storage =
        new BasicCatalogStorage(
            new LiteralURI("file://" + tempDir),
            new LocalStorageOps(props, LocalStorageOpsProperties.instance()));

    Olympia.createCatalog(storage, CATALOG_DEF);
  }

  @Override
  protected CatalogStorage storage() {
    return storage;
  }
}

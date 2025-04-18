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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.format.olympia.storage.CommonStorageOpsProperties;

public class LocalOverwriteOutputStream extends LocalStagingOutputStream {

  public LocalOverwriteOutputStream(
      Path file,
      CommonStorageOpsProperties commonProperties,
      LocalStorageOpsProperties localProperties) {
    super(file, commonProperties, localProperties);
  }

  @Override
  public void commit() throws IOException {
    createParentDirectories();
    Files.move(getStagingFilePath(), getTargetFilePath(), StandardCopyOption.REPLACE_EXISTING);
  }
}

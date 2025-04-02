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

import java.io.IOException;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class BasicCatalogStorage implements CatalogStorage {

  private final LiteralURI root;
  private final StorageOps ops;
  private final BufferAllocator bufferAllocator;

  public BasicCatalogStorage(LiteralURI root, StorageOps ops) {
    this.ops = ops;
    this.root = root;
    this.bufferAllocator = new RootAllocator();
  }

  @Override
  public LiteralURI root() {
    return root;
  }

  @Override
  public StorageOps ops() {
    return ops;
  }

  @Override
  public BufferAllocator getArrowAllocator() {
    // TODO: figure out best allocation size
    return bufferAllocator.newChildAllocator(String.valueOf(UUID.randomUUID()), 0, 1024 * 1024);
  }

  @Override
  public void close() throws IOException {
    ops().close();
    bufferAllocator.close();
  }
}

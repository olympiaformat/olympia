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
package org.format.olympia.tree;

import java.util.Optional;

/**
 * Representation of a tree root.
 *
 * <p>It is expected that this object continue to be updated during operations. This object is not
 * thread safe.
 */
public interface TreeRoot extends TreeNode {

  Optional<String> previousRootNodeFilePath();

  void setPreviousRootNodeFilePath(String previousRootNodeFilePath);

  void clearPreviousRootNodeFilePath();

  Optional<String> rollbackFromRootNodeFilePath();

  void setRollbackFromRootNodeFilePath(String rollbackFromRootNodeFilePath);

  void clearRollbackFromRootNodeFilePath();

  String catalogDefFilePath();

  void setCatalogDefFilePath(String catalogDefFilePath);

  int order();

  void setOrder(int order);
}

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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.format.olympia.action.Action;

/**
 * Representation of a tree node.
 *
 * <p>It is expected that this object continue to be updated during operations. This object is not
 * thread safe.
 */
public interface TreeNode extends Serializable {

  /**
   * Path for a tree node, if the node is persisted in storage
   *
   * @return path
   */
  Optional<String> path();

  void setPath(String path);

  void clearPath();

  /**
   * Creation time of the node file, if the node is persisted in storage
   *
   * @return creation epoch time in millis
   */
  Optional<Long> createdAtMillis();

  void setCreatedAtMillis(long createdAtMillis);

  void clearCreatedAtMillis();

  /**
   * THe number of keys in the pivot table
   *
   * @return number of keys
   */
  int numKeys();

  /** The number of actions */
  int numActions();

  NodeSearchResult search(String key);

  void set(String key, String value);

  void remove(String key);

  void setLeftmostChild(NodeKeyTableRow child);

  Optional<NodeKeyTableRow> getLeftmostChild();

  Map<String, NodeKeyTableRow> pendingChanges();

  List<VectorSlice> getVectorSlices();

  boolean isDirty();

  void addVectorSlice(String sourcePath, int startIndex, int endIndex);

  void addInMemoryChange(String key, String value, TreeNode child);

  void clear();

  Iterable<Action> actions();

  void setActions(Collection<Action> actions);

  void clearActions();
}

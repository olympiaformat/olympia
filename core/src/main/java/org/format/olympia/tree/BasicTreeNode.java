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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.format.olympia.relocated.com.google.common.collect.Lists;
import org.format.olympia.relocated.com.google.common.collect.Maps;

public class BasicTreeNode implements TreeNode {

  private final TreeMap<String, NodeKeyTableRow> pendingChanges;
  private final List<VectorSlice> vectorSlices;
  private NodeKeyTableRow leftmostChild;
  private String path;
  private Long createdAtMillis;

  public BasicTreeNode() {
    this.pendingChanges = Maps.newTreeMap();
    this.vectorSlices = Lists.newArrayList();
    this.leftmostChild = null;
  }

  @Override
  public Optional<String> path() {
    return Optional.ofNullable(path);
  }

  @Override
  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public void clearPath() {
    this.path = null;
  }

  @Override
  public Optional<Long> createdAtMillis() {
    return Optional.ofNullable(createdAtMillis);
  }

  @Override
  public void setCreatedAtMillis(long createdAtMillis) {
    this.createdAtMillis = createdAtMillis;
  }

  @Override
  public void clearCreatedAtMillis() {
    this.createdAtMillis = null;
  }

  @Override
  public int numKeys() {
    int count = pendingChanges.size();
    for (VectorSlice slice : vectorSlices) {
      count += (slice.endIndex() - slice.startIndex() + 1);
    }
    return count;
  }

  @Override
  public NodeSearchResult search(String key) {
    // If key is null, return leftmost child if exists
    if (key == null && leftmostChild != null) {
      return ImmutableNodeSearchResult.builder().nodePointer(leftmostChild.child()).build();
    }

    // exact match in pending changes
    if (key != null && pendingChanges.containsKey(key)) {
      NodeKeyTableRow row = pendingChanges.get(key);
      return ImmutableNodeSearchResult.builder()
          .key(Optional.of(key))
          .value(row.value())
          .nodePointer(row.child())
          .build();
    }

    // For traversal, find floor key in pending changes
    if (key != null) {
      String floorKey = pendingChanges.floorKey(key);
      if (floorKey != null) {
        NodeKeyTableRow row = pendingChanges.get(floorKey);
        return ImmutableNodeSearchResult.builder()
            .key(Optional.of(floorKey))
            .value(row.value())
            .nodePointer(row.child())
            .build();
      }
    }

    // fall back to left child
    if (leftmostChild != null) {
      return ImmutableNodeSearchResult.builder().nodePointer(leftmostChild.child()).build();
    }

    return ImmutableNodeSearchResult.builder().build();
  }

  @Override
  public void set(String key, String value) {
    NodeKeyTableRow existingRow = pendingChanges.get(key);
    Optional<TreeNode> child = existingRow != null ? existingRow.child() : Optional.empty();
    pendingChanges.put(
        key,
        ImmutableNodeKeyTableRow.builder()
            .key(Optional.of(key))
            .value(Optional.ofNullable(value))
            .child(child)
            .build());
  }

  @Override
  public void remove(String key) {
    pendingChanges.put(
        key,
        ImmutableNodeKeyTableRow.builder()
            .key(Optional.of(key))
            .value(Optional.empty())
            .child(Optional.empty())
            .build());
  }

  @Override
  public void setLeftmostChild(NodeKeyTableRow child) {
    this.leftmostChild = child;
  }

  @Override
  public Optional<NodeKeyTableRow> getLeftmostChild() {
    return Optional.ofNullable(leftmostChild);
  }

  @Override
  public Map<String, NodeKeyTableRow> pendingChanges() {
    return pendingChanges;
  }

  @Override
  public List<VectorSlice> getVectorSlices() {
    return vectorSlices;
  }

  @Override
  public void clear() {
    pendingChanges.clear();
    vectorSlices.clear();
    leftmostChild = null;
  }
}

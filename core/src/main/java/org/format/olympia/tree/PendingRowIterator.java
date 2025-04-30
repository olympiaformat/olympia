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

import java.util.Iterator;
import java.util.Map;

public class PendingRowIterator implements RowIterator, Comparable<RowIterator> {
  private final Iterator<NodeKeyTableRow> iterator;
  private NodeKeyTableRow current;

  public PendingRowIterator(
      Map<String, NodeKeyTableRow> pendingChanges, NodeKeyTableRow leftChild) {
    this.iterator = pendingChanges.values().iterator();
    if (leftChild != null && leftChild.child().isPresent()) {
      current = leftChild;
    } else if (iterator.hasNext()) {
      current = iterator.next();
    } else {
      current = null;
    }
  }

  @Override
  public String key() {
    return current != null ? current.key().orElse(null) : null;
  }

  @Override
  public String value() {
    return current != null ? current.value().orElse(null) : null;
  }

  @Override
  public TreeNode child() {
    return current != null ? current.child().orElse(null) : null;
  }

  @Override
  public void next() {
    if (iterator.hasNext()) {
      current = iterator.next();
    } else {
      current = null;
    }
  }

  @Override
  public boolean hasNext() {
    return current != null;
  }

  @Override
  public void close() throws Exception {
    // no-op
  }

  @Override
  public int compareTo(RowIterator o) {
    // prioritize pending rows
    if (key() != null && o.key() != null && key().equals(o.key())
        || key() == null && o.key() == null) {
      return -1;
    } else if (key() == null) {
      return -1;
    }
    if (o.key() == null) {
      return 1;
    }
    return key().compareTo(o.key());
  }
}

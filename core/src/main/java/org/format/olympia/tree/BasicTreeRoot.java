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
import org.format.olympia.ObjectDefinitions;
import org.format.olympia.util.ValidationUtil;

public class BasicTreeRoot extends BasicTreeNode implements TreeRoot {

  private String previousRootNodeFilePath;
  private String rollbackFromRootNodeFilePath;
  private String catalogDefFilePath;
  private int order = ObjectDefinitions.CATALOG_ORDER_DEFAULT;

  public BasicTreeRoot() {}

  @Override
  public Optional<String> previousRootNodeFilePath() {
    return Optional.ofNullable(previousRootNodeFilePath);
  }

  @Override
  public void setPreviousRootNodeFilePath(String previousRootNodeFilePath) {
    this.previousRootNodeFilePath = previousRootNodeFilePath;
  }

  @Override
  public void clearPreviousRootNodeFilePath() {
    this.previousRootNodeFilePath = null;
  }

  @Override
  public Optional<String> rollbackFromRootNodeFilePath() {
    return Optional.ofNullable(rollbackFromRootNodeFilePath);
  }

  @Override
  public void setRollbackFromRootNodeFilePath(String rollbackFromRootNodeFilePath) {
    this.rollbackFromRootNodeFilePath = rollbackFromRootNodeFilePath;
  }

  @Override
  public void clearRollbackFromRootNodeFilePath() {
    this.rollbackFromRootNodeFilePath = null;
  }

  @Override
  public String catalogDefFilePath() {
    ValidationUtil.checkState(
        catalogDefFilePath != null, "Catalog definition file path should be set for a tree root");
    return catalogDefFilePath;
  }

  @Override
  public void setCatalogDefFilePath(String catalogDefFilePath) {
    this.catalogDefFilePath = catalogDefFilePath;
  }

  @Override
  public int getOrder() {
    return order;
  }

  @Override
  public void setOrder(int order) {
    this.order = order;
  }
}

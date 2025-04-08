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
package org.format.olympia.action;

import org.format.olympia.proto.objects.IsolationLevel;

public class AnalyzeActionConflicts {

  private AnalyzeActionConflicts() {}

  public static ConflictAnalysisResult analyze(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (committed.type()) {
      case CATALOG_SNOW_NAMESPACES:
        return analyzeCatalogShowNamespaces(pending, committed, isolationLevel);
      case NAMESPACE_DESCRIBE:
        return analyzeNamespaceDescribe(pending, committed, isolationLevel);
      case NAMESPACE_EXISTS:
        return analyzeNamespaceExists(pending, committed, isolationLevel);
      case NAMESPACE_SHOW_TABLES:
        return analyzeNamespaceShowTables(pending, committed, isolationLevel);
      case NAMESPACE_SHOW_VIEWS:
        return analyzeNamespaceShowViews(pending, committed, isolationLevel);
      case NAMESPACE_CREATE:
        return analyzeNamespaceCreate(pending, committed, isolationLevel);
      case NAMESPACE_ALTER:
        return analyzeNamespaceAlter(pending, committed, isolationLevel);
      case NAMESPACE_ALTER_SET_PROPERTIES:
        return analyzeNamespaceAlterSetProperties(pending, committed, isolationLevel);
      case NAMESPACE_ALTER_UNSET_PROPERTIES:
        return analyzeNamespaceAlterUnsetProperties(pending, committed, isolationLevel);
      case NAMESPACE_DROP:
        return analyzeNamespaceDrop(pending, committed, isolationLevel);
      case TABLE_DESCRIBE:
        return analyzeTableDescribe(pending, committed, isolationLevel);
      case TABLE_EXISTS:
        return analyzeTableExists(pending, committed, isolationLevel);
      case TABLE_SELECT:
        return analyzeTableSelect(pending, committed, isolationLevel);
      case TABLE_CREATE:
        return analyzeTableCreate(pending, committed, isolationLevel);
      case TABLE_ALTER:
        return analyzeTableAlter(pending, committed, isolationLevel);
      case TABLE_ALTER_ADD_COLUMNS:
        return analyzeTableAlterAddColumns(pending, committed, isolationLevel);
      case TABLE_ALTER_REMOVE_COLUMNS:
        return analyzeTableAlterRemoveColumns(pending, committed, isolationLevel);
      case TABLE_INSERT:
        return analyzeTableInsert(pending, committed, isolationLevel);
      case TABLE_UPDATE:
        return analyzeTableUpdate(pending, committed, isolationLevel);
      case TABLE_DELETE:
        return analyzeTableDelete(pending, committed, isolationLevel);
      case TABLE_DROP:
        return analyzeTableDrop(pending, committed, isolationLevel);
      case VIEW_DESCRIBE:
        return analyzeViewDescribe(pending, committed, isolationLevel);
      case VIEW_EXISTS:
        return analyzeViewExists(pending, committed, isolationLevel);
      case VIEW_CREATE:
        return analyzeViewCreate(pending, committed, isolationLevel);
      case VIEW_REPLACE:
        return analyzeViewReplace(pending, committed, isolationLevel);
      case VIEW_DROP:
        return analyzeViewDrop(pending, committed, isolationLevel);
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeViewDrop(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case VIEW_REPLACE:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeViewReplace(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeViewCreate(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeViewExists(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeViewDescribe(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeTableDrop(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_ALTER:
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
      case TABLE_INSERT:
      case TABLE_UPDATE:
      case TABLE_DELETE:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableDelete(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_INSERT:
      case TABLE_ALTER:
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return canResolveConflict();
      case TABLE_UPDATE:
      case TABLE_DELETE:

      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableUpdate(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_INSERT:
      case TABLE_UPDATE:
      case TABLE_DELETE:
      // TODO: finish the resolution logic
      case TABLE_ALTER:
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableInsert(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_INSERT:
      case TABLE_UPDATE:
      case TABLE_DELETE:
      case TABLE_ALTER:
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return canResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableAlterRemoveColumns(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_ALTER:
      // TODO: allow resolving conflicts for these
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableAlterAddColumns(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_ALTER:
      // TODO: allow resolving conflicts for these
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableAlter(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_ALTER:
      // TODO: allow resolving conflicts for these
      case TABLE_ALTER_ADD_COLUMNS:
      case TABLE_ALTER_REMOVE_COLUMNS:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();

      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableCreate(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case TABLE_CREATE:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeTableSelect(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeTableExists(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeTableDescribe(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeNamespaceDrop(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeNamespaceAlterUnsetProperties(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case NAMESPACE_ALTER:
      // TODO: allow resolving conflicts for these
      case NAMESPACE_ALTER_SET_PROPERTIES:
      case NAMESPACE_ALTER_UNSET_PROPERTIES:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeNamespaceAlterSetProperties(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case NAMESPACE_ALTER:
      // TODO: allow resolving conflicts for these
      case NAMESPACE_ALTER_SET_PROPERTIES:
      case NAMESPACE_ALTER_UNSET_PROPERTIES:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeNamespaceAlter(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case NAMESPACE_ALTER:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeNamespaceCreate(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    switch (pending.type()) {
      case NAMESPACE_CREATE:
        if (!pending.objectKey().equals(committed.objectKey())) {
          return noConflict();
        }
        return cannotResolveConflict();
      default:
        return noConflict();
    }
  }

  private static ConflictAnalysisResult analyzeNamespaceShowViews(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeNamespaceShowTables(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeNamespaceExists(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeNamespaceDescribe(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult analyzeCatalogShowNamespaces(
      Action pending, Action committed, IsolationLevel isolationLevel) {
    return noConflict();
  }

  private static ConflictAnalysisResult noConflict() {
    return ImmutableConflictAnalysisResult.builder().hasConflict(false).build();
  }

  private static ConflictAnalysisResult canResolveConflict() {
    return ImmutableConflictAnalysisResult.builder()
        .hasConflict(true)
        .canResolveConflict(true)
        .build();
  }

  private static ConflictAnalysisResult cannotResolveConflict() {
    return ImmutableConflictAnalysisResult.builder()
        .hasConflict(true)
        .canResolveConflict(false)
        .build();
  }
}

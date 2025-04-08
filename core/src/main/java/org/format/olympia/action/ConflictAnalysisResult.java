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

import org.immutables.value.Value;

/**
 * Result of analyzing conflict when applying one pending action on top of the outcome of a
 * committed action
 */
@Value.Immutable
public interface ConflictAnalysisResult {

  /**
   * If the pending action is in conflict with the committed action. Two actions are in conflict if
   * reapplying the pending action on top of the outcome of the committed action is not possible or
   * requires recreating the value of the object key.
   */
  boolean hasConflict();

  /**
   * If having conflict, this value indicates if the conflict can be resolved by reapplying the
   * pending action on top of the outcome of the committed action
   */
  boolean canResolveConflict();
}

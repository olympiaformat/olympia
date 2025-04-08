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
package org.apache.iceberg;

import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.io.CloseableIterable;
import org.format.olympia.action.Action;
import org.format.olympia.action.ImmutableAction;
import org.format.olympia.iceberg.OlympiaIcebergTableOperations;
import org.format.olympia.proto.actions.ActionDef.ActionType;
import org.format.olympia.proto.actions.TableSelectDef;
import org.format.olympia.proto.objects.ObjectDef.ObjectType;

class OlympiaIcebergTableScan extends DataTableScan {

  OlympiaIcebergTableScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    CloseableIterable<FileScanTask> result = super.planFiles();
    Action action =
        ImmutableAction.builder()
            .objectType(ObjectType.TABLE)
            .type(ActionType.TABLE_SELECT)
            .def(
                TableSelectDef.newBuilder()
                    .addAllColumnNames(scanColumns())
                    .setIcebergExpressionJson(ExpressionParser.toJson(filter()))
                    .build())
            .build();
    ((OlympiaIcebergTableOperations) ((BaseTable) table()).operations())
        .transaction()
        .addAction(action);
    return result;
  }
}

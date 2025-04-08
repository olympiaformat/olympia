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

import java.util.List;
import org.format.olympia.Transaction;
import org.format.olympia.action.Action;
import org.format.olympia.action.ImmutableAction;
import org.format.olympia.iceberg.OlympiaIcebergTableOperations;
import org.format.olympia.proto.actions.ActionDef.ActionType;
import org.format.olympia.proto.actions.TableInsertDef;
import org.format.olympia.proto.actions.TableSelectContext;
import org.format.olympia.proto.actions.TableSelectDef;
import org.format.olympia.proto.objects.ObjectDef.ObjectType;
import org.format.olympia.relocated.com.google.common.collect.Lists;

class OlympiaIcebergMergeAppend extends MergeAppend {

  private final List<DataFile> dataFiles = Lists.newArrayList();

  OlympiaIcebergMergeAppend(String tableName, TableOperations ops) {
    super(tableName, ops);
  }

  @Override
  public MergeAppend appendFile(DataFile file) {
    MergeAppend result = super.appendFile(file);
    dataFiles.add(file);
    return result;
  }

  @Override
  public void commit() {
    super.commit();

    ImmutableAction.Builder actionBuilder =
        ImmutableAction.builder().objectType(ObjectType.TABLE).type(ActionType.TABLE_INSERT);

    TableInsertDef.Builder tableInsertDefBuilder = TableInsertDef.newBuilder();
    for (DataFile file : dataFiles) {
      tableInsertDefBuilder.addIcebergDataFilesJson(ContentFileParser.toJson(file, dataSpec()));
    }

    Transaction transaction = ((OlympiaIcebergTableOperations) ops()).transaction();
    for (Action existingAction : transaction.actions()) {
      if (existingAction.type() == ActionType.TABLE_SELECT) {
        tableInsertDefBuilder.addSelects(
            TableSelectContext.newBuilder()
                .setNamespaceName(existingAction.namespaceName().get())
                .setTableName(existingAction.namespaceObjectName().get())
                .setSelectContext((TableSelectDef) existingAction.def().get())
                .build());
      }
    }

    transaction.addAction(actionBuilder.def(tableInsertDefBuilder.build()).build());
  }
}

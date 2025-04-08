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

import org.apache.iceberg.metrics.MetricsReporter;

public class OlympiaIcebergTable extends BaseTable {

  public OlympiaIcebergTable(TableOperations ops, String name, MetricsReporter reporter) {
    super(ops, name, reporter);
  }

  @Override
  public AppendFiles newAppend() {
    return new OlympiaIcebergMergeAppend(name(), operations());
  }

  @Override
  public OverwriteFiles newOverwrite() {
    return new OlympiaIcebergOverwriteFiles(name(), operations());
  }

  @Override
  public TableScan newScan() {
    return new OlympiaIcebergTableScan(
        this,
        this.schema(),
        ImmutableTableScanContext.builder().metricsReporter(reporter()).build());
  }
}

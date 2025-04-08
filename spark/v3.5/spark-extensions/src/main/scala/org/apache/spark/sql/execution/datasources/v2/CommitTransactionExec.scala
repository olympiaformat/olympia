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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.format.olympia.iceberg.OlympiaIcebergCatalog

case class CommitTransactionExec() extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    session.sessionState.catalogManager.currentCatalog match {
      case catalog: SparkCatalog =>
        catalog.icebergCatalog() match {
          case icebergCatalog: OlympiaIcebergCatalog => icebergCatalog.commitCatalogTransaction()
          case _ =>
            throw new UnsupportedOperationException(
              "Cannot begin transaction in non-Olympia Iceberg catalog")
        }
      case _ =>
        throw new UnsupportedOperationException(
          "Cannot begin transaction in non-Iceberg Spark catalog")
    }
    Seq.empty
  }

  override def simpleString(maxFields: Int): String = {
    "CommitTransactionExec"
  }
}

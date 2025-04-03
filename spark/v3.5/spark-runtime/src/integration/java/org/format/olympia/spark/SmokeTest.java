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
package org.format.olympia.spark;

import org.apache.spark.sql.SparkSession;
import org.format.olympia.spark.extensions.OlympiaSparkExtensions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SmokeTest {

  private static SparkSession spark;

  @BeforeAll
  public static void beforeAll() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("SmokeTest")
            .config("spark.sql.extensions", OlympiaSparkExtensions.class.getName())
            .getOrCreate();
  }

  @Test
  public void test() {}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import io.openlineage.spark.agent.OpenLineageSparkListener;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;

public class OpenLineageIntegrationTest {
  @Test
  void testOpenLineageIntegrationForDataset() {
    SparkSession spark =
        SparkSession.builder()
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .appName("TestSparkWithOpenLineage")
            .config("spark.openlineage.transport.type", "file")
            .config(
                "spark.openlineage.transport.location",
                "/Users/pawelkocinski/Desktop/projects/iceberg/spark/v3.5/spark/src/test/java/org/apache/iceberg/lineage")
//            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
//            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
//            .config("spark.sql.catalog.local.type", "hadoop")
//            .config("spark.sql.catalog.local.warehouse", "warehouse")
            .master("local")
            .getOrCreate();



//      spark.sql("DROP TABLE IF EXISTS local.tt");
//      spark.sql("CREATE TABLE local.tt (id bigint, data string) USING iceberg");
//      spark.sql("insert into local.tt values(1, 'a'), (2, 'b'), (3, 'c')");
//
//      spark.sql("select * from local.test_table").show();
    Dataset<Row> df = spark.read().format("iceberg").load("/Users/pawelkocinski/Desktop/projects/iceberg/spark/v3.5/spark-runtime/warehouse/test_table");

//    catalog.addListener(new CatalogListener());

//    val wrapped = new ExternalCatalogWithListener(externalCatalog)

//    spark.sparkContext.addSparkListener(new SparkListener {
//      override def onOtherEvent(event: SparkListenerEvent): Unit = {
//              event match {
//      case externalCatalogEvent: ExternalCatalogEvent => catalogEvents.append(externalCatalogEvent)
//      case _ => {}
//        }
//      }
//    })
//
//    df.explain(true);
//
    df.createOrReplaceTempView("testview");
//
    Dataset<Row> dfTransformed = spark.sql("select id from testview");
    dfTransformed.write().mode("overwrite").format("parquet").save("/Users/pawelkocinski/Desktop/projects/iceberg/spark/v3.5/spark-runtime/warehouse/test_table_parquet");

    dfTransformed.explain(true);


//
//      spark.read().format("iceberg").load("/Users/pawelkocinski/Desktop/projects/iceberg/spark/v3.5/spark-runtime/warehouse/test_table").
//              write().
//              mode("overwrite").
//              saveAsTable("local.test_table_1");
//
//    spark.sql("ALTER TABLE local.test_table CREATE TAG `EOW-04`");

  }
}

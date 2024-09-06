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

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.openlineage.spark.agent.OpenLineageSparkListener;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OpenLineageIntegrationTest {
  private File lineageFile;

  @Test
  void testOpenLineageIntegrationForDataset() throws Exception {
    lineageFile = File.createTempFile("openlineage_test_" + System.nanoTime(), ".log");
    lineageFile.deleteOnExit();

    SparkSession spark =
        SparkSession.builder()
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .appName("TestSparkWithOpenLineage")
            .config("spark.openlineage.transport.type", "file")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "warehouse")
            .config(
                "spark.openlineage.transport.location", lineageFile.getAbsolutePath())
            .master("local")
            .getOrCreate();

    spark.sql("DROP TABLE IF EXISTS local.default.test_table");
    spark.sql("DROP TABLE IF EXISTS local.default.test_table_rewritten");

    // create initial dataframe
    StructType structType = new StructType();
    structType = structType.add("id", DataTypes.StringType, false);
    structType = structType.add("value", DataTypes.IntegerType, false);

    List<Row> nums = new ArrayList<Row>();
    nums.add(RowFactory.create("name-1", 1));
    nums.add(RowFactory.create("name-2", 2));
    nums.add(RowFactory.create("name-3", 3));

    Dataset<Row> df = spark.createDataFrame(nums, structType);

    // write multiple versions
    for (int i = 0; i < 10; i++) {
      df.writeTo("local.default.test_table").createOrReplace();
    }

    spark.read().table("local.default.test_table").createOrReplaceTempView("temp_table");

    spark.sql("select * from temp_table").writeTo("local.default.test_table_rewritten").createOrReplace();


    List<JsonObject> inputEvents = readInputEvents(lineageFile);

    for (JsonObject event : inputEvents) {
      JsonArray inputs = event.getAsJsonArray("inputs");

      inputs.forEach(
          input -> {
            JsonObject inputObj = input.getAsJsonObject();
            String namespace = inputObj.get("namespace").getAsString();
            assertThat(namespace).isEqualTo("iceberg");

            String name = inputObj.get("name").getAsString();
            assertThat(name).isEqualTo("warehouse/default/test_table");

            JsonObject facets = inputObj.getAsJsonObject("facets");

            String version = facets.get("version").getAsJsonObject().get("datasetVersion").getAsString();
            assertThat(version).isEqualTo("10");
      });
    }

    List<JsonObject> outputEvents = readOutputEvents(lineageFile);

    int versionNr = 0;
    for (JsonObject event : outputEvents) {
      JsonArray outputs = event.getAsJsonArray("outputs");

      for (int i = 0; i< outputs.size(); i++) {
        JsonObject output = outputs.get(i).getAsJsonObject();
        versionNr+=1;

        JsonObject outputObj = output.getAsJsonObject();
        String namespace = outputObj.get("namespace").getAsString();
        if (Objects.equals(namespace, "file")) {
          return;
        }

        assertThat(namespace).isEqualTo("iceberg");

        String name = outputObj.get("name").getAsString();
        assertThat(
            List.of("warehouse/default/test_table", "warehouse/default/test_table_rewritten")
        ).contains(name);

        String version = outputObj
                .getAsJsonObject("facets")
                .get("version")
                .getAsJsonObject()
                .get("datasetVersion")
                .getAsString();

        if (name.equals("warehouse/default/test_table")) {
          assertThat(version).isEqualTo(String.valueOf(versionNr));
        } else {
          assertThat(version).isEqualTo("1");
        }
      }
    }
  }

  private List<JsonObject> readInputEvents(File file) throws Exception {
    List<JsonObject> eventList;
    try (Scanner scanner = new Scanner(file, "UTF-8")) {
      eventList = new ArrayList<>();
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        JsonObject event = JsonParser.parseString(line).getAsJsonObject();
        if (!event.getAsJsonArray("inputs").isEmpty()) {
          eventList.add(event);
        }
      }
    }
    return eventList;
  }

  private List<JsonObject> readOutputEvents(File file) throws Exception {
    List<JsonObject> eventList;
    try (Scanner scanner = new Scanner(file, "UTF-8")) {
      eventList = new ArrayList<>();
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        JsonObject event = JsonParser.parseString(line).getAsJsonObject();
        if (!event.getAsJsonArray("outputs").isEmpty()) {
          eventList.add(event);
        }
      }
    }
    return eventList;
  }
}

/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.snowflake.source.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.snowflake.Constants;
import io.cdap.plugin.snowflake.common.BaseSnowflakeTest;
import io.cdap.plugin.snowflake.sink.batch.SnowflakeBatchSink;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests to verify configuration of {@link SnowflakeBatchSink}
 * <p>
 * By default all tests will be skipped, since Snowflake credentials are needed.
 * <p>
 * Instructions to enable the tests:
 * 1. Create/use existing Snowflake account.
 * 2. Create database for testing.
 * 3. Run the tests using the command below:
 * <p>
 * mvn clean test
 * -Dsnowflake.test.account.name=
 * -Dsnowflake.test.database=
 * -Dsnowflake.test.schema=
 * -Dsnowflake.test.username=
 * -Dsnowflake.test.password=
 */
public class SnowflakeBatchSinkTest extends BaseSnowflakeTest {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactSummary APP_ARTIFACT_PIPELINE =
    new ArtifactSummary("data-pipeline", "3.2.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifactPipeline =
      NamespaceId.DEFAULT.artifact(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion());

    setupBatchArtifacts(parentArtifactPipeline, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("snowflake-plugins", "1.0.0"),
                      parentArtifactPipeline,
                      SnowflakeBatchSink.class);
  }

  @Test
  public void testSinkTypes() throws Exception {
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("NumberOfEmployees", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("ShippingLatitude", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("ShippingLongitude", Schema.of(Schema.Type.DOUBLE))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", "testInsertAccount1")
        .set("NumberOfEmployees", 6)
        .set("ShippingLatitude", 50.4501)
        .set("ShippingLongitude", 30.5234)
        .build(),
      StructuredRecord.builder(schema)
        .set("Name", "testInsertAccount2")
        .set("NumberOfEmployees", 1)
        .set("ShippingLatitude", 37.4220)
        .set("ShippingLongitude", 122.0841)
        .build()
    );

    ApplicationManager appManager = deployPipeline(schema);
    runPipeline(appManager, inputRecords);
  }

  @Test
  public void testNullFields() throws Exception {
    Schema schema = Schema.recordOf("output",
                                    Schema.Field.of("Name",
                                                    Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                                    Schema.Field.of("NumberOfEmployees",
                                                    Schema.nullableOf(Schema.of(Schema.Type.INT))),
                                    Schema.Field.of("ShippingLatitude",
                                                    Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                                    Schema.Field.of("ShippingLongitude",
                                                    Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))
    );

    List<StructuredRecord> inputRecords = ImmutableList.of(
      StructuredRecord.builder(schema)
        .set("Name", null)
        .set("NumberOfEmployees", null)
        .set("ShippingLatitude", null)
        .set("ShippingLongitude", null)
        .build()
    );

    ApplicationManager appManager = deployPipeline(schema);
    runPipeline(appManager, inputRecords);
  }

  private ImmutableMap.Builder<String, String> getBasicConfig() {
    return ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("accountName", ACCOUNT_NAME)
      .put("database", DATABASE)
      .put("schemaName", SCHEMA)
      .put("tableName", Constants.TEST_TABLE)
      .put("username", USERNAME)
      .put("password", PASSWORD)
      .put("maxFileSize", "0");
  }

  protected ApplicationManager deployPipeline(Schema schema) throws Exception {
    return deployPipeline(schema, Collections.emptyMap());
  }

  protected ApplicationManager deployPipeline(Schema schema, Map<String, String> pluginProperties)
    throws Exception {

    Map<String, String> sinkProperties = new HashMap<>(getBasicConfig().build());

    sinkProperties.putAll(pluginProperties);

    String inputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName, schema));

    ETLStage sink = new ETLStage(SnowflakeBatchSink.PLUGIN_NAME,
                                 new ETLPlugin(SnowflakeBatchSink.PLUGIN_NAME,
                                               BatchSink.PLUGIN_TYPE, sinkProperties, null));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app(
      SnowflakeBatchSink.PLUGIN_NAME + "_" + testName.getMethodName());
    return deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT_PIPELINE, etlConfig));
  }

  protected void runPipeline(ApplicationManager appManager, List<StructuredRecord> input) throws Exception {
    String inputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED,  5, TimeUnit.MINUTES);
  }
}

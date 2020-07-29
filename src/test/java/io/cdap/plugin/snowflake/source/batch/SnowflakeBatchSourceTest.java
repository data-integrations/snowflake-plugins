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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
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
import io.cdap.plugin.snowflake.ValidationAssertions;
import io.cdap.plugin.snowflake.common.BaseSnowflakeTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests to verify configuration of {@link SnowflakeBatchSource}
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
public class SnowflakeBatchSourceTest extends BaseSnowflakeTest {

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
                      SnowflakeBatchSource.class);
  }

  @Test
  public void testBatchSource() throws Exception {
    ImmutableMap<String, String> properties = getBasicConfig()
      .build();

    List<StructuredRecord> actual = getPipelineResults(
      properties, SnowflakeBatchSource.NAME, "SnowflakeBatch");

    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.size());
    ValidationAssertions.assertTestTableResults(Constants.TEST_TABLE_EXPECTED, actual.get(0));
  }

  private ImmutableMap.Builder<String, String> getBasicConfig() {
    return ImmutableMap.<String, String>builder()
      .put("referenceName", "ref")
      .put("accountName", ACCOUNT_NAME)
      .put("database", DATABASE)
      .put("schemaName", SCHEMA)
      .put("importQuery", Constants.IMPORT_QUERY)
      .put("username", USERNAME)
      .put("password", PASSWORD)
      .put("maxSplitSize", "0");
  }

  private List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties,
                                                    String pluginName,
                                                    String applicationPrefix) throws Exception {
    ETLStage source = new ETLStage("SnowflakeReader", new ETLPlugin(
      pluginName, BatchSource.PLUGIN_TYPE, sourceProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + testName.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app(applicationPrefix + "_" + testName.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT_PIPELINE, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }
}

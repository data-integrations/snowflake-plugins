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
package io.cdap.plugin.snowflake.sink.batch;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Plugin inserts records into Snowflake tables.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SnowflakeBatchSink.PLUGIN_NAME)
@Description("Writes records to Snowflake")
public class SnowflakeBatchSink extends BatchSink<StructuredRecord, NullWritable, CSVRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeBatchSink.class);

  public static final String PLUGIN_NAME = "Snowflake";

  private final SnowflakeSinkConfig config;
  private StructuredRecordToCSVRecordTransformer transformer;

  public SnowflakeBatchSink(SnowflakeSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getInputSchema(), stageConfigurer.getFailureCollector());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Schema inputSchema = context.getInputSchema();
    FailureCollector collector = context.getFailureCollector();
    config.validate(inputSchema, collector);
    collector.getOrThrowException();

    context.addOutput(Output.of(config.getReferenceName(), new SnowflakeOutputFormatProvider(config)));

    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    if (inputSchema.getFields() != null && !inputSchema.getFields().isEmpty()) {
      String operationDescription = String.format("Wrote to Snowflake table '%s'", config.getTableName());
      lineageRecorder.recordWrite("Write", operationDescription,
                                  inputSchema.getFields().stream()
                                    .map(Schema.Field::getName)
                                    .collect(Collectors.toList()));
    }
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new StructuredRecordToCSVRecordTransformer();
  }

  @Override
  public void transform(StructuredRecord record, Emitter<KeyValue<NullWritable, CSVRecord>> emitter)
    throws IOException {
    CSVRecord csvRecord = transformer.transform(record);
    emitter.emit(new KeyValue<>(null, csvRecord));
  }
}

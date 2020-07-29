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

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Writes csv records into batches and submits them to Snowflake.
 * Accepts <code>null</code> as a key, and CSVRecord as a value.
 */
public class SnowflakeRecordWriter extends RecordWriter<NullWritable, CSVRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeRecordWriter.class);
  private static final Gson GSON = new Gson();

  private final CSVBuffer csvBuffer;
  private final CSVBuffer csvBufferSizeCheck;
  private final SnowflakeSinkConfig config;
  private final SnowflakeSinkAccessor snowflakeAccessor;
  private final String destinationStagePath;

  public SnowflakeRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    destinationStagePath = conf.get(SnowflakeOutputFormat.DESTINATION_STAGE_PATH_PROPERTY);
    String configJson = conf.get(
      SnowflakeOutputFormatProvider.PROPERTY_CONFIG_JSON);
    config = GSON.fromJson(
      configJson, SnowflakeSinkConfig.class);

    csvBuffer = new CSVBuffer(true);
    csvBufferSizeCheck = new CSVBuffer(false);
    snowflakeAccessor = new SnowflakeSinkAccessor(config);
  }

  @Override
  public void write(NullWritable key, CSVRecord csvRecord) throws IOException {
    csvBufferSizeCheck.reset();
    csvBufferSizeCheck.write(csvRecord);

    if (config.getMaxFileSize() > 0 && csvBuffer.size() + csvBufferSizeCheck.size() > config.getMaxFileSize()) {
      submitCurrentBatch();
    }

    csvBuffer.write(csvRecord);
  }

  private void submitCurrentBatch() throws IOException {
    if (csvBuffer.getRecordsCount() != 0) {
      try (InputStream csvInputStream = new ByteArrayInputStream(csvBuffer.getByteArray())) {
        snowflakeAccessor.uploadStream(csvInputStream, destinationStagePath);
      }

      csvBuffer.reset();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    submitCurrentBatch();
  }
}

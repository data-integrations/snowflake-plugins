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

import au.com.bytecode.opencsv.CSVReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RecordReader implementation, which reads object from Snowflake.
 */
public class SnowflakeRecordReader extends RecordReader<NullWritable, Map<String, String>> {

  private final String stageSplit;
  private final SnowflakeSourceAccessor snowflakeAccessor;
  private CSVReader csvReader;
  private String[] headers;
  private String[] nextLine;

  public SnowflakeRecordReader(String stageSplit, SnowflakeSourceAccessor snowflakeAccessor) {
    this.stageSplit = stageSplit;
    this.snowflakeAccessor = snowflakeAccessor;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
    this.csvReader = snowflakeAccessor.buildCsvReader(stageSplit);
    this.headers = csvReader.readNext();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    nextLine = csvReader.readNext();
    return nextLine != null;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public Map<String, String> getCurrentValue() {
    Map<String, String> result = new HashMap<>();
    for (int i = 0; i < headers.length; i++) {
      result.put(headers[i], nextLine[i]);
    }
    return result;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (csvReader != null) {
      csvReader.close();
    }
    snowflakeAccessor.removeStageFile(stageSplit);
  }
}

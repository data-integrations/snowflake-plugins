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


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Input format class which generates input splits for each given object
 * and initializes appropriate record reader.
 */
public class SnowflakeInputFormat extends InputFormat {

  private static final Gson GSON = new Gson();

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    SnowflakeSourceAccessor snowflakeAccessor = getSnowflakeAccessor(jobContext.getConfiguration());
    List<String> stageSplits = snowflakeAccessor.prepareStageSplits();
    return stageSplits.stream()
      .map(SnowflakeSplit::new)
      .collect(Collectors.toList());
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit,
                                         TaskAttemptContext context) {
    SnowflakeSplit snowflakeSplit = (SnowflakeSplit) inputSplit;
    SnowflakeSourceAccessor snowflakeAccessor = getSnowflakeAccessor(context.getConfiguration());
    return new SnowflakeRecordReader(snowflakeSplit.getStageSplit(), snowflakeAccessor);
  }

  private SnowflakeSourceAccessor getSnowflakeAccessor(Configuration configuration) {
    String configJson = configuration.get(
      SnowflakeInputFormatProvider.PROPERTY_CONFIG_JSON);
    SnowflakeBatchSourceConfig config = GSON.fromJson(
      configJson, SnowflakeBatchSourceConfig.class);
    return new SnowflakeSourceAccessor(config);
  }
}

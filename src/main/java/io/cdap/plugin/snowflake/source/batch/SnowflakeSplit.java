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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A split used for mapreduce.
 */
public class SnowflakeSplit extends InputSplit implements Writable {

  private String stageSplit;

  public SnowflakeSplit() {
    // For serialization
  }

  public SnowflakeSplit(String stageSplit) {
    this.stageSplit = stageSplit;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(stageSplit);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    stageSplit = in.readUTF();
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public String getStageSplit() {
    return stageSplit;
  }
}

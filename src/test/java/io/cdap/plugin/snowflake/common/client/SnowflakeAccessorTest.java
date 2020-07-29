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

package io.cdap.plugin.snowflake.common.client;

import io.cdap.plugin.snowflake.Constants;
import io.cdap.plugin.snowflake.common.BaseSnowflakeTest;
import io.cdap.plugin.snowflake.source.batch.SnowflakeSourceAccessor;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Tests for {@link SnowflakeAccessor}
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
public class SnowflakeAccessorTest extends BaseSnowflakeTest {

  private SnowflakeSourceAccessor snowflakeAccessor = new SnowflakeSourceAccessor(CONFIG);

  @Test
  public void testDescribeQuery() throws Exception {
    List<SnowflakeFieldDescriptor> expected = Arrays.asList(
      new SnowflakeFieldDescriptor("COLUMN_NUMBER", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_DECIMAL", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_NUMERIC", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_INT", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_INTEGER", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_BIGINT", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_SMALLINT", -5, true),
      new SnowflakeFieldDescriptor("COLUMN_FLOAT", 8, true),
      new SnowflakeFieldDescriptor("COLUMN_FLOAT4", 8, true),
      new SnowflakeFieldDescriptor("COLUMN_FLOAT8", 8, true),
      new SnowflakeFieldDescriptor("COLUMN_DOUBLE", 8, true),
      new SnowflakeFieldDescriptor("COLUMN_DOUBLE_PRECISION", 8, true),
      new SnowflakeFieldDescriptor("COLUMN_REAL", 8, true),
      new SnowflakeFieldDescriptor("COLUMN_VARCHAR", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_CHAR", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_CHARACTER", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_STRING", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_TEXT", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_BINARY", -2, true),
      new SnowflakeFieldDescriptor("COLUMN_VARBINARY", -2, true),
      new SnowflakeFieldDescriptor("COLUMN_BOOLEAN", 16, true),
      new SnowflakeFieldDescriptor("COLUMN_DATE", 91, true),
      new SnowflakeFieldDescriptor("COLUMN_DATETIME", 93, true),
      new SnowflakeFieldDescriptor("COLUMN_TIME", 92, true),
      new SnowflakeFieldDescriptor("COLUMN_TIMESTAMP", 93, true),
      new SnowflakeFieldDescriptor("COLUMN_TIMESTAMP_LTZ", 93, true),
      new SnowflakeFieldDescriptor("COLUMN_TIMESTAMP_NTZ", 93, true),
      new SnowflakeFieldDescriptor("COLUMN_TIMESTAMP_TZ", 93, true),
      new SnowflakeFieldDescriptor("COLUMN_VARIANT", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_OBJECT", 12, true),
      new SnowflakeFieldDescriptor("COLUMN_ARRAY", 12, true));

    List<SnowflakeFieldDescriptor> actual = snowflakeAccessor.describeQuery(Constants.IMPORT_QUERY);

    Assert.assertNotNull(actual);
    Assert.assertFalse(actual.isEmpty());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPrepareStageSplits() throws Exception {
    Pattern expected = Pattern.compile("cdap_stage/result.*data__0_0_0\\.csv\\.gz");

    List<String> actual = snowflakeAccessor.prepareStageSplits();

    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.size());
    Assert.assertTrue(expected.matcher(actual.get(0)).matches());
  }
}

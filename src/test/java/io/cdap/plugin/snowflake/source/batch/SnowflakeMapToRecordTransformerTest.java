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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.snowflake.Constants;
import io.cdap.plugin.snowflake.ValidationAssertions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link SnowflakeMapToRecordTransformer}
 */
public class SnowflakeMapToRecordTransformerTest {

  @Test
  public void transform() {
    Map<String, String> row = new HashMap<>();
    row.put("COLUMN_NUMBER", "101");
    row.put("COLUMN_DECIMAL", "102");
    row.put("COLUMN_NUMERIC", "103");
    row.put("COLUMN_INT", "104");
    row.put("COLUMN_INTEGER", "105");
    row.put("COLUMN_BIGINT", "106");
    row.put("COLUMN_SMALLINT", "107");
    row.put("COLUMN_FLOAT", "108.1");
    row.put("COLUMN_FLOAT4", "109.1");
    row.put("COLUMN_FLOAT8", "110.1");
    row.put("COLUMN_DOUBLE", "111.1");
    row.put("COLUMN_DOUBLE_PRECISION", "112.1");
    row.put("COLUMN_REAL", "113.1");
    row.put("COLUMN_VARCHAR", "text_114");
    row.put("COLUMN_CHAR", "1");
    row.put("COLUMN_CHARACTER", "2");
    row.put("COLUMN_STRING", "text_115");
    row.put("COLUMN_TEXT", "text_116");
    row.put("COLUMN_BINARY", "text_117");
    row.put("COLUMN_VARBINARY", "text_118");
    row.put("COLUMN_BOOLEAN", "true");
    row.put("COLUMN_DATE", "2019-01-01");
    row.put("COLUMN_DATETIME", "2019-01-01T01:01:01+00:00");
    row.put("COLUMN_TIME", "01:01:01");
    row.put("COLUMN_TIMESTAMP", "2019-01-01T01:01:01+00:00");
    row.put("COLUMN_TIMESTAMP_LTZ", "2019-01-01T01:01:01+00:00");
    row.put("COLUMN_TIMESTAMP_NTZ", "2019-01-01T01:01:01+00:00");
    row.put("COLUMN_TIMESTAMP_TZ", "2019-01-01T01:01:01+00:00");
    row.put("COLUMN_VARIANT", "{ \"key1\": \"value1\", \"key2\": \"value2\" }");
    row.put("COLUMN_OBJECT", "{ \"outer_key1\": { \"inner_key1A\": \"1a\", \"inner_key1B\": \"1b\" }, " +
      "\"outer_key2\": { \"inner_key2\": 2 } }");
    row.put("COLUMN_ARRAY", "[1, 2, 3]");

    SnowflakeMapToRecordTransformer transformer =
      new SnowflakeMapToRecordTransformer(Constants.TEST_TABLE_SCHEMA);

    StructuredRecord actual = transformer.transform(row);

    ValidationAssertions.assertTestTableResults(Constants.TEST_TABLE_EXPECTED, actual);
  }
}

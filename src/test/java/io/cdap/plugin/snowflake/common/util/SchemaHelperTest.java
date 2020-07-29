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

package io.cdap.plugin.snowflake.common.util;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.snowflake.ValidationAssertions;
import io.cdap.plugin.snowflake.common.client.SnowflakeFieldDescriptor;
import io.cdap.plugin.snowflake.source.batch.SnowflakeBatchSourceConfig;
import io.cdap.plugin.snowflake.source.batch.SnowflakeSourceAccessor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link SchemaHelper}
 */
public class SchemaHelperTest {

  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void testGetSchema() {
    Schema expected = Schema.recordOf(
      "test",
      Schema.Field.of("test_field", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    Schema actual = SchemaHelper.getSchema(null, expected.toString(), collector, null);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetSchemaInvalidJson() {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    SchemaHelper.getSchema(null, "{}", collector, null);

    ValidationAssertions.assertValidationFailed(
      collector, Collections.singletonList(SnowflakeBatchSourceConfig.PROPERTY_SCHEMA));
  }

  @Test
  public void testGetSchemaFromSnowflakeUnknownType() throws IOException {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    SnowflakeSourceAccessor snowflakeAccessor = Mockito.mock(SnowflakeSourceAccessor.class);

    List<SnowflakeFieldDescriptor> sample = new ArrayList<>();
    sample.add(new SnowflakeFieldDescriptor("field1", -1000, false));

    Mockito.when(snowflakeAccessor.describeQuery(null)).thenReturn(sample);

    SchemaHelper.getSchema(snowflakeAccessor, null, collector, null);

    ValidationAssertions.assertValidationFailed(
      collector, Collections.singletonList(SnowflakeBatchSourceConfig.PROPERTY_SCHEMA));
  }

  @Test
  public void testGetSchemaFromSnowflake() throws IOException {
    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    SnowflakeSourceAccessor snowflakeAccessor = Mockito.mock(SnowflakeSourceAccessor.class);

    List<SnowflakeFieldDescriptor> sample = Arrays.asList(
      new SnowflakeFieldDescriptor("field011", Types.VARCHAR, true),
      new SnowflakeFieldDescriptor("field012", Types.VARCHAR, false),
      new SnowflakeFieldDescriptor("field021", Types.CHAR, true),
      new SnowflakeFieldDescriptor("field022", Types.CHAR, false),
      new SnowflakeFieldDescriptor("field031", Types.BINARY, true),
      new SnowflakeFieldDescriptor("field032", Types.BINARY, false),
      new SnowflakeFieldDescriptor("field041", Types.INTEGER, true),
      new SnowflakeFieldDescriptor("field042", Types.INTEGER, false),
      new SnowflakeFieldDescriptor("field051", Types.DECIMAL, true),
      new SnowflakeFieldDescriptor("field052", Types.DECIMAL, false),
      new SnowflakeFieldDescriptor("field061", Types.DOUBLE, true),
      new SnowflakeFieldDescriptor("field062", Types.DOUBLE, false),
      new SnowflakeFieldDescriptor("field071", Types.TIMESTAMP, true),
      new SnowflakeFieldDescriptor("field072", Types.TIMESTAMP, false),
      new SnowflakeFieldDescriptor("field081", Types.DATE, true),
      new SnowflakeFieldDescriptor("field082", Types.DATE, false),
      new SnowflakeFieldDescriptor("field091", Types.TIME, true),
      new SnowflakeFieldDescriptor("field092", Types.TIME, false),
      new SnowflakeFieldDescriptor("field111", Types.BOOLEAN, true),
      new SnowflakeFieldDescriptor("field112", Types.BOOLEAN, false),
      new SnowflakeFieldDescriptor("field121", Types.BIGINT, true),
      new SnowflakeFieldDescriptor("field122", Types.BIGINT, false),
      new SnowflakeFieldDescriptor("field131", Types.SMALLINT, true),
      new SnowflakeFieldDescriptor("field132", Types.SMALLINT, false));

    Schema expected = Schema.recordOf(
      "data",
      Schema.Field.of("field011", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("field012", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("field021", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("field022", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("field031", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
      Schema.Field.of("field032", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("field041", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("field042", Schema.of(Schema.Type.INT)),
      Schema.Field.of("field051", Schema.nullableOf(Schema.decimalOf(38, 9))),
      Schema.Field.of("field052", Schema.decimalOf(38, 9)),
      Schema.Field.of("field061", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("field062", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("field071", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("field072", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("field081", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("field082", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("field091", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
      Schema.Field.of("field092", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("field111", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("field112", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("field121", Schema.nullableOf(Schema.decimalOf(38))),
      Schema.Field.of("field122", Schema.decimalOf(38)),
      Schema.Field.of("field131", Schema.nullableOf(Schema.decimalOf(38))),
      Schema.Field.of("field132", Schema.decimalOf(38))
    );

    Mockito.when(snowflakeAccessor.describeQuery(null)).thenReturn(sample);

    Schema actual = SchemaHelper.getSchema(snowflakeAccessor, null, collector, null);

    Assert.assertTrue(collector.getValidationFailures().isEmpty());
    Assert.assertEquals(expected, actual);
  }
}

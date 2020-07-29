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

package io.cdap.plugin.snowflake;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.math.BigDecimal;

/**
 * Helper class with test constants.
 */
public class Constants {

  public static final String TEST_TABLE = "TEST_TABLE";

  public static final String DROP_TEST_TABLE = String.format("DROP TABLE IF EXISTS %s", TEST_TABLE);

  public static final String CREATE_TEST_TABLE = String.format(
    "CREATE TABLE IF NOT EXISTS %s (COLUMN_NUMBER NUMBER, " +
      "COLUMN_DECIMAL DECIMAL, " +
      "COLUMN_NUMERIC NUMERIC, " +
      "COLUMN_INT INT, " +
      "COLUMN_INTEGER INTEGER, " +
      "COLUMN_BIGINT BIGINT, " +
      "COLUMN_SMALLINT SMALLINT, " +
      "COLUMN_FLOAT FLOAT, " +
      "COLUMN_FLOAT4 FLOAT4, " +
      "COLUMN_FLOAT8 FLOAT8, " +
      "COLUMN_DOUBLE DOUBLE, " +
      "COLUMN_DOUBLE_PRECISION DOUBLE PRECISION, " +
      "COLUMN_REAL REAL, " +
      "COLUMN_VARCHAR VARCHAR, " +
      "COLUMN_CHAR CHAR, " +
      "COLUMN_CHARACTER CHARACTER, " +
      "COLUMN_STRING STRING, " +
      "COLUMN_TEXT TEXT, " +
      "COLUMN_BINARY BINARY, " +
      "COLUMN_VARBINARY VARBINARY, " +
      "COLUMN_BOOLEAN BOOLEAN, " +
      "COLUMN_DATE DATE, " +
      "COLUMN_DATETIME DATETIME, " +
      "COLUMN_TIME TIME, " +
      "COLUMN_TIMESTAMP TIMESTAMP, " +
      "COLUMN_TIMESTAMP_LTZ TIMESTAMP_LTZ, " +
      "COLUMN_TIMESTAMP_NTZ TIMESTAMP_NTZ, " +
      "COLUMN_TIMESTAMP_TZ TIMESTAMP_TZ, " +
      "COLUMN_VARIANT VARIANT, " +
      "COLUMN_OBJECT OBJECT, " +
      "COLUMN_ARRAY ARRAY);", TEST_TABLE);

  public static final String INSERT_INTO_TEST_TABLE = String.format(
    "INSERT INTO %s " +
      "SELECT " +
      "101, " +
      "102, " +
      "103, " +
      "104, " +
      "105, " +
      "106, " +
      "107, " +
      "108.1, " +
      "109.1, " +
      "110.1, " +
      "111.1, " +
      "112.1, " +
      "113.1, " +
      "'text_114', " +
      "'1', " +
      "'2', " +
      "'text_115', " +
      "'text_116', " +
      "hex_encode('text_117'), " +
      "hex_encode('text_118'), " +
      "true, " +
      "'2019-01-01', " +
      "'2019-01-01 01:01:01 +00:00', " +
      "'01:01:01', " +
      "'2019-01-01 01:01:01 +00:00', " +
      "'2019-01-01 01:01:01 +00:00', " +
      "'2019-01-01 01:01:01 +00:00', " +
      "'2019-01-01 01:01:01 +00:00', " +
      "parse_json(' { \"key1\": \"value1\", \"key2\": \"value2\" } '), " +
      "parse_json(' { \"outer_key1\": { \"inner_key1A\": \"1a\", \"inner_key1B\": \"1b\" }, " +
      "\"outer_key2\": { \"inner_key2\": 2 } } '), " +
      "array_construct(1, 2, 3);", TEST_TABLE);

  public static final String IMPORT_QUERY = String.format("SELECT * FROM %s;", TEST_TABLE);

  public static final Schema TEST_TABLE_SCHEMA = Schema.recordOf(
    "data",
    Schema.Field.of("COLUMN_NUMBER", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_DECIMAL", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_NUMERIC", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_INT", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_INTEGER", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_BIGINT", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_SMALLINT", Schema.nullableOf(Schema.decimalOf(38))),
    Schema.Field.of("COLUMN_FLOAT", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("COLUMN_FLOAT4", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("COLUMN_FLOAT8", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("COLUMN_DOUBLE", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("COLUMN_DOUBLE_PRECISION", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("COLUMN_REAL", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    Schema.Field.of("COLUMN_VARCHAR", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_CHAR", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_CHARACTER", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_STRING", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_TEXT", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_BINARY", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
    Schema.Field.of("COLUMN_VARBINARY", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
    Schema.Field.of("COLUMN_BOOLEAN", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
    Schema.Field.of("COLUMN_DATE", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
    Schema.Field.of("COLUMN_DATETIME", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
    Schema.Field.of("COLUMN_TIME", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
    Schema.Field.of("COLUMN_TIMESTAMP", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
    Schema.Field.of("COLUMN_TIMESTAMP_LTZ", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
    Schema.Field.of("COLUMN_TIMESTAMP_NTZ", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
    Schema.Field.of("COLUMN_TIMESTAMP_TZ", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
    Schema.Field.of("COLUMN_VARIANT", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_OBJECT", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
    Schema.Field.of("COLUMN_ARRAY", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
  );

  public static final StructuredRecord TEST_TABLE_EXPECTED = StructuredRecord.builder(TEST_TABLE_SCHEMA)
    .set("COLUMN_NUMBER", new BigDecimal("101").unscaledValue().toByteArray())
    .set("COLUMN_DECIMAL", new BigDecimal("102").unscaledValue().toByteArray())
    .set("COLUMN_NUMERIC", new BigDecimal("103").unscaledValue().toByteArray())
    .set("COLUMN_INT", new BigDecimal("104").unscaledValue().toByteArray())
    .set("COLUMN_INTEGER", new BigDecimal("105").unscaledValue().toByteArray())
    .set("COLUMN_BIGINT", new BigDecimal("106").unscaledValue().toByteArray())
    .set("COLUMN_SMALLINT", new BigDecimal("107").unscaledValue().toByteArray())
    .set("COLUMN_FLOAT", 108.1d)
    .set("COLUMN_FLOAT4", 109.1d)
    .set("COLUMN_FLOAT8", 110.1d)
    .set("COLUMN_DOUBLE", 111.1d)
    .set("COLUMN_DOUBLE_PRECISION", 112.1d)
    .set("COLUMN_REAL", 113.1d)
    .set("COLUMN_VARCHAR", "text_114")
    .set("COLUMN_CHAR", "1")
    .set("COLUMN_CHARACTER", "2")
    .set("COLUMN_STRING", "text_115")
    .set("COLUMN_TEXT", "text_116")
    .set("COLUMN_BINARY", Bytes.toBytes("text_117"))
    .set("COLUMN_VARBINARY", Bytes.toBytes("text_118"))
    .set("COLUMN_BOOLEAN", true)
    .set("COLUMN_DATE", 17897)
    .set("COLUMN_DATETIME", 1546304461000000L)
    .set("COLUMN_TIME", 3661000000L)
    .set("COLUMN_TIMESTAMP", 1546304461000000L)
    .set("COLUMN_TIMESTAMP_LTZ", 1546304461000000L)
    .set("COLUMN_TIMESTAMP_NTZ", 1546304461000000L)
    .set("COLUMN_TIMESTAMP_TZ", 1546304461000000L)
    .set("COLUMN_VARIANT", "{ \"key1\": \"value1\", \"key2\": \"value2\" }")
    .set("COLUMN_OBJECT", "{ \"outer_key1\": { \"inner_key1A\": \"1a\", \"inner_key1B\": \"1b\" }, " +
      "\"outer_key2\": { \"inner_key2\": 2 } }")
    .set("COLUMN_ARRAY", "[1, 2, 3]")
    .build();
}

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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import io.cdap.plugin.snowflake.common.client.SnowflakeFieldDescriptor;
import io.cdap.plugin.snowflake.common.exception.SchemaParseException;
import io.cdap.plugin.snowflake.source.batch.SnowflakeBatchSourceConfig;
import io.cdap.plugin.snowflake.source.batch.SnowflakeInputFormatProvider;
import io.cdap.plugin.snowflake.source.batch.SnowflakeSourceAccessor;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Resolves schema.
 */
public class SchemaHelper {

  private static final Map<Integer, Schema> SNOWFLAKE_TYPE_TO_CDAP_SCHEMA =
    new ImmutableMap.Builder<Integer, Schema>()
      .put(Types.VARCHAR, Schema.of(Schema.Type.STRING))
      .put(Types.CHAR, Schema.of(Schema.Type.STRING))
      .put(Types.BINARY, Schema.of(Schema.Type.BYTES))
      .put(Types.INTEGER, Schema.of(Schema.Type.INT))
      .put(Types.DECIMAL, Schema.decimalOf(38, 9))
      .put(Types.DOUBLE, Schema.of(Schema.Type.DOUBLE))
      .put(Types.TIMESTAMP, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
      .put(Types.DATE, Schema.of(Schema.LogicalType.DATE))
      .put(Types.TIME, Schema.of(Schema.LogicalType.TIME_MICROS))
      .put(Types.BOOLEAN, Schema.of(Schema.Type.BOOLEAN))
      .put(Types.BIGINT, Schema.decimalOf(38))
      .put(Types.SMALLINT, Schema.decimalOf(38))
      .put(Types.TIMESTAMP_WITH_TIMEZONE, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
      .build();

  private SchemaHelper() {
  }

  /**
   * Retrieves schema for the Snowflake batch source based on the given configuration.
   *
   * @param config    The configuration for Snowflake batch source
   * @param collector The failure collector to capture any schema retrieval errors.
   * @return The resolved schema for Snowflake source
   */
  public static Schema getSchema(SnowflakeBatchSourceConfig config, FailureCollector collector) {
    if (!config.canConnect()) {
      return getParsedSchema(config.getSchema());
    }

    SnowflakeSourceAccessor snowflakeSourceAccessor =
      new SnowflakeSourceAccessor(config, SnowflakeInputFormatProvider.PROPERTY_DEFAULT_ESCAPE_CHAR);
    return getSchema(
      snowflakeSourceAccessor,
      config.getSchema(),
      collector,
      config.getTableName(),
      config.getImportQuery()
    );
  }

  /**
   * Retrieves schema for a Snowflake source based on the provided parameters.
   *
   * @param snowflakeAccessor The {@link SnowflakeSourceAccessor} used to connect to Snowflake.
   * @param schema            A JSON-format schema string
   * @param collector         The {@link FailureCollector} to collect errors if schema retrieval fails.
   * @param tableName         The name of the table in Snowflake.
   * @param importQuery       The query to fetch data from Snowflake, used when `tableName` is not provided.
   * @return The parsed {@link Schema} if successful, or {@code null} if an error occurs.
   */
  public static Schema getSchema(SnowflakeSourceAccessor snowflakeAccessor, String schema,
                                 FailureCollector collector, String tableName, String importQuery) {
    try {
      if (!Strings.isNullOrEmpty(schema)) {
        return getParsedSchema(schema);
      }
      return getSchema(snowflakeAccessor, tableName, importQuery);
    } catch (SchemaParseException | IllegalArgumentException e) {
      collector.addFailure(String.format("Unable to retrieve output schema. Reason: '%s'", e.getMessage()),
                           null)
        .withStacktrace(e.getStackTrace())
        .withConfigProperty(SnowflakeBatchSourceConfig.PROPERTY_SCHEMA);
      return null;
    }
  }

  private static Schema getParsedSchema(String schema) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException | IllegalStateException e) {
      throw new SchemaParseException(e);
    }
  }

  private static Schema getSchema(SnowflakeAccessor snowflakeAccessor, @Nullable String tableName, @Nullable String importQuery) {
    try {
      List<SnowflakeFieldDescriptor> result;
      if (!Strings.isNullOrEmpty(tableName)) {
        result = snowflakeAccessor.describeTable(snowflakeAccessor.getSchema(), tableName);
      } else {
        result = snowflakeAccessor.describeQuery(importQuery);
      }
      List<Schema.Field> fields = result.stream()
        .map(fieldDescriptor -> Schema.Field.of(fieldDescriptor.getName(), getSchema(fieldDescriptor)))
        .collect(Collectors.toList());
      return Schema.recordOf("data", fields);
    } catch (SQLException e) {
      throw new SchemaParseException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Schema getSchema(SnowflakeAccessor snowflakeAccessor, String importQuery) {
    try {
      List<SnowflakeFieldDescriptor> result = snowflakeAccessor.describeQuery(importQuery);
      List<Schema.Field> fields = result.stream()
        .map(fieldDescriptor -> Schema.Field.of(fieldDescriptor.getName(), getSchema(fieldDescriptor)))
        .collect(Collectors.toList());
      return Schema.recordOf("data", fields);
    } catch (IOException e) {
      throw new SchemaParseException(e);
    }
  }

  private static Schema getSchema(SnowflakeFieldDescriptor fieldDescriptor) {
    Integer type = fieldDescriptor.getType();
    Schema schema = SNOWFLAKE_TYPE_TO_CDAP_SCHEMA.get(type);
    if (schema == null) {
      throw new SchemaParseException(String.format(
        "No corresponding Schema is found for java.sql.Type: %d", type));
    }
    return fieldDescriptor.getNullable()
      ? Schema.nullableOf(schema)
      : schema;
  }

  /**
   * Works like {@link SchemaHelper#checkCompatibility(Schema, Schema, boolean)}
   * except that checking nullable is always on.
   *
   * @see SchemaHelper#checkCompatibility(Schema, Schema, boolean)
   */
  public static void checkCompatibility(Schema actualSchema, Schema providedSchema) {
    checkCompatibility(actualSchema, providedSchema, true);
  }

  /**
   * Checks two schemas compatibility based on the following rules:
   * <ul>
   *   <li>Actual schema must have fields indicated in the provided schema.</li>
   *   <li>Fields types in both schema must match.</li>
   *   <li>If actual schema field is required, provided schema field can be required or nullable.</li>
   *   <li>If actual schema field is nullable, provided schema field must be nullable.</li>
   * </ul>
   *
   * @param actualSchema schema calculated based on Snowflake metadata information
   * @param providedSchema schema provided in the configuration
   * @param checkNullable if true, checks for nullability of fields in schema are triggered.
   */
  public static void checkCompatibility(Schema actualSchema, Schema providedSchema, boolean checkNullable) {
    for (Schema.Field providedField : Objects.requireNonNull(providedSchema.getFields())) {
      Schema.Field actualField = actualSchema.getField(providedField.getName(), true);
      if (actualField == null) {
        throw new IllegalArgumentException(
          String.format("Field '%s' does not exist in Snowflake", providedField.getName()));
      }

      Schema providedFieldSchema = providedField.getSchema();
      Schema actualFieldSchema = actualField.getSchema();

      boolean isActualFieldNullable = actualFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      actualFieldSchema = isActualFieldNullable ? actualFieldSchema.getNonNullable() : actualFieldSchema;
      providedFieldSchema = isProvidedFieldNullable ? providedFieldSchema.getNonNullable() : providedFieldSchema;

      boolean bothNumeric = isFieldNumeric(actualFieldSchema) && isFieldNumeric(providedFieldSchema);
      if (!bothNumeric && !Objects.equals(actualFieldSchema.getLogicalType(), providedFieldSchema.getLogicalType())) {
          throw new IllegalArgumentException(
            String.format("Expected field '%s' to be of '%s', but it is of '%s'",
                          providedField.getName(), providedFieldSchema, actualFieldSchema)
                          );
      }

      if (checkNullable && isActualFieldNullable && !isProvidedFieldNullable) {
        throw new IllegalArgumentException(String.format("Field '%s' should be nullable", providedField.getName()));
      }
    }
  }

  private static boolean isFieldNumeric(Schema field1) {
    return Objects.equals(field1.getType(), Schema.Type.INT) || Objects.equals(field1.getType(), Schema.Type.LONG)
      || Objects.equals(field1.getLogicalType(), Schema.LogicalType.DECIMAL);
  }
}

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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Transforms a {@link StructuredRecord} to a {@link CSVRecord}
 */
public class StructuredRecordToCSVRecordTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredRecordToCSVRecordTransformer.class);

  public CSVRecord transform(StructuredRecord record) throws IOException {
    List<String> fieldNames = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      String value = convertSchemaFieldToString(record.get(fieldName), field, record);

      fieldNames.add(fieldName);
      values.add(value);
    }

    return new CSVRecord(fieldNames, values);
  }

  /**
   * Convert a schema field to String which can be read by Snowflake.
   *
   * @param value field value
   * @param field schema field
   * @param record
   * @return string representing the value in format, which can be understood by Snowflake
   */
  @Nullable
  public static String convertSchemaFieldToString(Object value, Schema.Field field, StructuredRecord record)
    throws IOException {
    // don't convert null to avoid NPE
    if (value == null) {
      return null;
    }

    Schema fieldSchema = field.getSchema();

    if (fieldSchema.isNullable()) {
      fieldSchema = fieldSchema.getNonNullable();
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (fieldSchema.getLogicalType() != null) {
      Instant instant;
      switch (logicalType) {
        case DATE:
          // convert epoch day to yyyy-mm-dd format
          return LocalDate.ofEpochDay((Integer) value).toString();
        case TIMESTAMP_MICROS:
          // convert timestamp to ISO 8601 format
          instant = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis((Long) value));
          return instant.toString();
        case TIME_MICROS:
          // convert timestamp to HH:mm:ss,SSS
          instant = Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis((Long) value));
          return instant.atZone(ZoneOffset.UTC).toLocalTime().toString();
        case TIMESTAMP_MILLIS:
          // convert timestamp to ISO 8601 format
          instant = Instant.ofEpochMilli((Long) value);
          return instant.toString();
        case TIME_MILLIS:
          // convert timestamp to HH:mm:ss,SSS
          instant = Instant.ofEpochMilli((Long) value);
          return instant.atZone(ZoneOffset.UTC).toLocalTime().toString();
        case DECIMAL:
          return record.getDecimal(field.getName()).toString();
        default:
          throw new IllegalArgumentException(
            String.format("Field '%s' is of unsupported type '%s'", fieldSchema.getDisplayName(),
                          logicalType.getToken()));
      }
    }

    switch (fieldSchema.getType()) {
      // convert to json so it can be saved to Snowflake's variant
      case RECORD:
        return StructuredRecordStringConverter.toJsonString((StructuredRecord) value);
      // convert to json so it can be saved to Snowflake's variant
      case ARRAY:
        String stringRecord = StructuredRecordStringConverter.toJsonString(record);
        JsonElement jsonObject = new JsonParser().parse(stringRecord);
        return jsonObject.getAsJsonObject().get(field.getName()).toString();
      // convert to hex which can be understood by Snowflake and saved to BINARY type
      case BYTES:
        byte[] bytes;
        if (value instanceof ByteBuffer) {
          ByteBuffer byteBuffer = (ByteBuffer) value;
          bytes = new byte[byteBuffer.remaining()];
          byteBuffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }

        StringBuilder hexSb = new StringBuilder();
        for (byte b : bytes) {
          hexSb.append(String.format("%02X", b));
        }
        return hexSb.toString();
      default:
        return value.toString();
    }
  }
}

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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.snowflake.common.BaseSnowflakeConfig;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import io.cdap.plugin.snowflake.common.util.SchemaHelper;

import javax.annotation.Nullable;

/**
 * Provides the configurations for {@link SnowflakeBatchSink} plugin.
 */
public class SnowflakeSinkConfig extends BaseSnowflakeConfig {
  public static final String PROPERTY_REFERENCE_NAME = "referenceName";
  public static final String PROPERTY_TABLE_NAME = "tableName";
  public static final String PROPERTY_MAX_FILE_SIZE = "maxFileSize";
  public static final String PROPERTY_COPY_OPTIONS = "copyOptions";

  private static final String GET_FIELDS_QUERY = "SELECT * FROM %s"; // runs with a limit

  @Name(PROPERTY_REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(PROPERTY_TABLE_NAME)
  @Description("Name of the table to insert records into.")
  @Macro
  private String tableName;

  @Name(PROPERTY_MAX_FILE_SIZE)
  @Description("Maximum file size used to write data to Snowflake specified in bytes.")
  @Macro
  private Long maxFileSize;

  @Name(PROPERTY_COPY_OPTIONS)
  @Description("List of arbitrary copy options")
  @Macro
  @Nullable
  private String copyOptions;

  public SnowflakeSinkConfig(String referenceName, String accountName, String database,
                             String schemaName, String username, String password,
                             @Nullable Boolean keyPairEnabled, @Nullable String path,
                             @Nullable String passphrase, @Nullable Boolean oauth2Enabled,
                             @Nullable String clientId, @Nullable String clientSecret,
                             @Nullable String refreshToken, @Nullable String connectionArguments) {
    super(accountName, database, schemaName, username, password,
          keyPairEnabled, path, passphrase, oauth2Enabled, clientId, clientSecret, refreshToken, connectionArguments);
    this.referenceName = referenceName;
  }

  public String getTableName() {
    return tableName;
  }

  public Long getMaxFileSize() {
    return maxFileSize;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getCopyOptions() {
    String copyOptions = (this.copyOptions == null) ? "" : this.copyOptions;
    return copyOptions.replace(",", " ").replace(":", "=");
  }

  public void validate(Schema inputSchema, FailureCollector failureCollector) {
    super.validate(failureCollector);
    validateInputSchema(inputSchema, failureCollector);
  }

  /**
   * Checks that input schema is correct. Which means:
   * 1. All fields in it are present in table
   * 2. Field types are in accordance with the actual types in the table.
   *
   * @param schema input schema to check
   */
  private void validateInputSchema(Schema schema, FailureCollector failureCollector) {
    // schema can be null in case it is a macro
    if (schema == null) {
      return;
    }

    SnowflakeAccessor snowflakeAccessor = new SnowflakeAccessor(this);
    Schema expectedSchema = SchemaHelper.getSchema(snowflakeAccessor, String.format(GET_FIELDS_QUERY, tableName));

    try {
      SchemaHelper.checkCompatibility(expectedSchema, schema);
    } catch (IllegalArgumentException ex) {
      failureCollector.addFailure(String.format("Input schema does not correspond with schema of actual table. %s",
                                                ex.getMessage()) , null)
        .withStacktrace(ex.getStackTrace());
    }
  }
}

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

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.snowflake.common.BaseSnowflakeConfig;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * This class {@link SnowflakeBatchSourceConfig} provides all the configuration required for
 * configuring the Source plugin.
 */
public class SnowflakeBatchSourceConfig extends BaseSnowflakeConfig {
  public static final String PROPERTY_REFERENCE_NAME = "referenceName";
  public static final String PROPERTY_IMPORT_QUERY = "importQuery";
  public static final String PROPERTY_MAX_SPLIT_SIZE = "maxSplitSize";
  public static final String PROPERTY_SCHEMA = "schema";
  public static final String PROPERTY_TABLE_NAME = "tableName";
  public static final String PROPERTY_IMPORT_QUERY_TYPE = "importQueryType";

  @Name(PROPERTY_REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(PROPERTY_IMPORT_QUERY)
  @Description("Query for import data.")
  @Macro
  @Nullable
  private String importQuery;

  @Name(PROPERTY_MAX_SPLIT_SIZE)
  @Description("Maximum split size specified in bytes.")
  @Macro
  private Long maxSplitSize;

  @Name(PROPERTY_SCHEMA)
  @Nullable
  @Description("Output schema for the source.")
  @Macro
  private String schema;


  @Name(PROPERTY_TABLE_NAME)
  @Description("The name of the table used to retrieve the schema.")
  @Macro
  @Nullable
  private final String tableName;

  @Name(PROPERTY_IMPORT_QUERY_TYPE)
  @Description("Whether to select Table Name or Import Query to extract the data.")
  @Macro
  @Nullable
  private final String importQueryType;


  public SnowflakeBatchSourceConfig(String referenceName, String accountName, String database,
                                    String schemaName, @Nullable String importQuery, String username, String password,
                                    @Nullable Boolean keyPairEnabled, @Nullable String path,
                                    @Nullable String passphrase, @Nullable Boolean oauth2Enabled,
                                    @Nullable String clientId, @Nullable String clientSecret,
                                    @Nullable String refreshToken, Long maxSplitSize,
                                    @Nullable String connectionArguments,
                                    @Nullable String schema,
                                    @Nullable String tableName,
                                    @Nullable String importQueryType) {
    super(
      accountName, database, schemaName, username, password, keyPairEnabled, path, passphrase,
      oauth2Enabled, clientId, clientSecret, refreshToken, connectionArguments
    );
    this.referenceName = referenceName;
    this.importQuery = importQuery;
    this.maxSplitSize = maxSplitSize;
    this.schema = schema;
    this.tableName = tableName;
    this.importQueryType = getImportQueryType();
  }

  public String getImportQuery() {
    return importQuery;
  }

  public Long getMaxSplitSize() {
    return maxSplitSize;
  }

  public String getReferenceName() {
    return referenceName;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  @Nullable
  public String getTableName() {
    return tableName;
  }

  @Nullable
  public String getImportQueryType() {
    return importQueryType == null ? ImportQueryType.IMPORT_QUERY.name() : importQueryType;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (!containsMacro(PROPERTY_MAX_SPLIT_SIZE) && Objects.nonNull(maxSplitSize)
      && maxSplitSize < 0) {
      collector.addFailure("Maximum Slit Size cannot be a negative number.", null)
        .withConfigProperty(PROPERTY_MAX_SPLIT_SIZE);
    }

    if (!containsMacro(PROPERTY_IMPORT_QUERY_TYPE)) {
      boolean isImportQuerySelected = ImportQueryType.IMPORT_QUERY.getValue().equals(importQueryType);
      boolean isTableNameSelected = ImportQueryType.TABLE_NAME.getValue().equals(importQueryType);

      if (isImportQuerySelected && !containsMacro(PROPERTY_IMPORT_QUERY) && Strings.isNullOrEmpty(importQuery)) {
        collector.addFailure("Import Query cannot be empty", null)
          .withConfigProperty(PROPERTY_IMPORT_QUERY);

      } else if (isTableNameSelected && !containsMacro(PROPERTY_TABLE_NAME) && Strings.isNullOrEmpty(tableName)) {
        collector.addFailure("Table Name cannot be empty", null)
          .withConfigProperty(PROPERTY_TABLE_NAME);
      }
    } else {
      boolean isImportQueryMissing = !containsMacro(PROPERTY_IMPORT_QUERY) && Strings.isNullOrEmpty(importQuery);
      boolean isTableNameMissing = !containsMacro(PROPERTY_TABLE_NAME) && Strings.isNullOrEmpty(tableName);

      if (isImportQueryMissing && isTableNameMissing) {
        collector.addFailure("Either 'Import Query' or 'Table Name' must be provided.", null)
          .withConfigProperty(PROPERTY_IMPORT_QUERY)
          .withConfigProperty(PROPERTY_TABLE_NAME);
      }
    }
  }
}

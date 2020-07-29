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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.snowflake.common.BaseSnowflakeConfig;
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

  @Name(PROPERTY_REFERENCE_NAME)
  @Description("This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc.")
  private String referenceName;

  @Name(PROPERTY_IMPORT_QUERY)
  @Description("Query for import data.")
  @Macro
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

  public SnowflakeBatchSourceConfig(String referenceName, String accountName, String database,
                                    String schemaName, String importQuery, String username, String password,
                                    @Nullable Boolean keyPairEnabled, @Nullable String path,
                                    @Nullable String passphrase, @Nullable Boolean oauth2Enabled,
                                    @Nullable String clientId, @Nullable String clientSecret,
                                    @Nullable String refreshToken, Long maxSplitSize,
                                    @Nullable String connectionArguments, @Nullable String schema) {
    super(accountName, database, schemaName, username, password,
          keyPairEnabled, path, passphrase, oauth2Enabled, clientId, clientSecret, refreshToken, connectionArguments);
    this.referenceName = referenceName;
    this.importQuery = importQuery;
    this.maxSplitSize = maxSplitSize;
    this.schema = schema;
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
}

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

package io.cdap.plugin.snowflake.actions.loadunload.unload;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.plugin.snowflake.actions.loadunload.LoadUnloadConfig;
import javax.annotation.Nullable;

/**
 * Config for {@link UnloadAction}
 */
public class UnloadActionConfig extends LoadUnloadConfig {
  private static final String PROPERTY_SOURCE_TYPE = "sourceType";

  @Name(PROPERTY_SOURCE_TYPE)
  @Description("Source type. Can be a either a select query or table.")
  @Macro
  private String sourceType;

  @Nullable
  @Description("Source table of the data to be unloaded.")
  @Macro
  private String sourceTable;

  @Nullable
  @Description("Query result of which will be unloaded.")
  @Macro
  private String sourceQuery;

  @Description("Internal or external location where the files containing data are saved.")
  @Macro
  private String destinationPath;

  @Description("Specifies whether to include the table column headings in the output files.")
  @Macro
  private Boolean includeHeader;


  public UnloadActionConfig(String accountName, String database, String schemaName, String username, String password,
                            @Nullable Boolean keyPairEnabled, @Nullable String path, @Nullable String passphrase,
                            @Nullable Boolean oauth2Enabled, @Nullable String clientId, @Nullable String clientSecret,
                            @Nullable String refreshToken, @Nullable String connectionArguments) {
    super(accountName, database, schemaName, username, password, keyPairEnabled, path, passphrase, oauth2Enabled,
          clientId, clientSecret, refreshToken, connectionArguments);
  }

  public SourceType getSourceType() {
    return getEnumValueByString(SourceType.class, sourceType, PROPERTY_SOURCE_TYPE);
  }

  @Nullable
  public String getSourceTable() {
    return sourceTable;
  }

  @Nullable
  public String getSourceQuery() {
    return sourceQuery;
  }

  public String getDestinationPath() {
    return destinationPath;
  }

  public Boolean includeHeader() {
    return includeHeader;
  }
}

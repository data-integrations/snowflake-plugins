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

package io.cdap.plugin.snowflake.actions.loadunload.load;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.snowflake.actions.loadunload.LoadUnloadConfig;
import javax.annotation.Nullable;

/**
 * Config for {@LoadAction}
 */
public class LoadActionConfig extends LoadUnloadConfig {
  public static final String PROPERTY_SOURCE_TYPE = "sourceType";
  public static final String PROPERTY_SOURCE_PATH = "sourcePath";
  public static final String PROPERTY_SOURCE_QUERY = "sourceQuery";
  public static final String PROPERTY_DESTINATION_TABLE = "destinationTable";

  @Name(PROPERTY_SOURCE_TYPE)
  @Description("Source type. Can be a either a transformation select query or a stage location.")
  @Macro
  private String sourceType;

  @Nullable
  @Name(PROPERTY_SOURCE_PATH)
  @Description("Internal or external location where the files containing data to be loaded are staged.")
  @Macro
  private String sourcePath;

  @Nullable
  @Name(PROPERTY_SOURCE_QUERY)
  @Description("SELECT statement used for transformations. " +
    "Specifies an explicit set of fields/columns (separated by commas) " +
    "to load from the staged data files. The fields/columns are selected from the files using a standard SQL query. " +
    "The list must match the sequence of columns in the target table.")
  @Macro
  private String sourceQuery;

  @Name(PROPERTY_DESTINATION_TABLE)
  @Description("Name of the table into which data is unloaded.")
  @Macro
  private String destinationTable;

  @Description("List of one or more files names (separated by commas) to be loaded. The files must already have " +
    "been staged in either the Snowflake internal location or external location specified in the command.  " +
    "The maximum number of files names that can be specified is 1000.")
  @Macro
  @Nullable
  private String files;

  @Description("Regular expression pattern string, specifying the file names and/or " +
    "paths to match. For the best performance, try to avoid applying patterns that filter on a large number of files.")
  @Macro
  @Nullable
  private String pattern;

  public LoadActionConfig(String accountName, String database, String schemaName, String username, String password,
                          @Nullable Boolean keyPairEnabled, @Nullable String path, @Nullable String passphrase,
                          @Nullable Boolean oauth2Enabled, @Nullable String clientId, @Nullable String clientSecret,
                          @Nullable String refreshToken, @Nullable String connectionArguments) {
    super(accountName, database, schemaName, username, password, keyPairEnabled, path, passphrase, oauth2Enabled,
          clientId, clientSecret, refreshToken, connectionArguments);
  }

  public SourceType getSourceType() {
    return getEnumValueByString(SourceType.class, sourceType,
                                PROPERTY_SOURCE_TYPE);
  }

  public String getSourcePath() {
    return sourcePath;
  }

  @Nullable
  public String getSourceQuery() {
    return sourceQuery;
  }

  public String getDestinationTable() {
    return destinationTable;
  }

  @Nullable
  public String getFiles() {
    return files;
  }

  @Nullable
  public String getPattern() {
    return pattern;
  }

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);
    if (Strings.isNullOrEmpty(sourcePath) && Strings.isNullOrEmpty(sourceQuery)) {
      collector.addFailure("'Source Path' and 'Source Query' properties are not set.",
                           "Please set at least one of given properties.")
        .withConfigProperty(PROPERTY_SOURCE_PATH)
        .withConfigProperty(PROPERTY_SOURCE_QUERY);
    }
  }
}

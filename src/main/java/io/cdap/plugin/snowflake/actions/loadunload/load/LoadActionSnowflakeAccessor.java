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
import io.cdap.plugin.snowflake.actions.loadunload.LoadUnloadSnowflakeAccessor;
import io.cdap.plugin.snowflake.common.util.QueryUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Accesses Snowflake API to copy table into stage.
 */
public class LoadActionSnowflakeAccessor extends LoadUnloadSnowflakeAccessor {
  private static final String COPY_TABLE_INTO_STAGE = "COPY INTO %s FROM";

  private final LoadActionConfig config;

  public LoadActionSnowflakeAccessor(LoadActionConfig config) {
    super(config);
    this.config = config;
  }

  protected StringBuilder getQueryPrefix() {
    StringBuilder sb = new StringBuilder(String.format(COPY_TABLE_INTO_STAGE, config.getDestinationTable()));

    switch(config.getSourceType()) {
      case FROM_PATH:
        sb.append(String.format(" %s", LoadUnloadSnowflakeAccessor.quotePathIfNeeded(
          config.getSourcePath())));
        break;
      case FROM_QUERY:
        sb.append(String.format(" (%s)", QueryUtil.removeSemicolon(config.getSourceQuery())));
        break;
      default:
        throw new IllegalStateException(String.format("Unknown value for sourceType: '%s'",
                                                 config.getSourceType()));
    }

    if (!Strings.isNullOrEmpty(config.getPattern())) {
      sb.append(String.format(" PATTERN = '%s'", config.getPattern()));
    }

    if (!Strings.isNullOrEmpty(config.getFiles())) {
      sb.append(String.format(" FILES = ( %s )", quoteFilesString(config.getFiles())));
    }

    return sb;
  }

  private static String quoteFilesString(String files) {
    StringBuilder sb = new StringBuilder();
    for (String file : files.split(",")) {
      String fileNameClean = StringUtils.strip(file.trim(), "'");
      sb.append("'");
      sb.append(fileNameClean);
      sb.append("',");
    }
    sb.setLength(sb.length() - 1);

    return sb.toString();
  }
}

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

import io.cdap.plugin.snowflake.actions.loadunload.LoadUnloadSnowflakeAccessor;
import io.cdap.plugin.snowflake.common.util.QueryUtil;

/**
 * Accesses Snowflake API to copy table into stage.
 */
public class UnloadActionSnowflakeAccessor extends LoadUnloadSnowflakeAccessor {
  private static final String COPY_TABLE_INTO_STAGE = "COPY INTO %s FROM";

  private final UnloadActionConfig config;

  public UnloadActionSnowflakeAccessor(UnloadActionConfig config) {
    super(config);
    this.config = config;
  }

  protected StringBuilder getQueryPrefix() {
    StringBuilder sb = new StringBuilder(String.format(COPY_TABLE_INTO_STAGE,
                                                       LoadUnloadSnowflakeAccessor.quotePathIfNeeded(
                                                         config.getDestinationPath())));

    switch(config.getSourceType()) {
      case FROM_TABLE:
        sb.append(String.format(" %s", config.getSourceTable()));
        break;
      case FROM_QUERY:
        sb.append(String.format(" ( %s )", QueryUtil.removeSemicolon(config.getSourceQuery())));
        break;
      default:
        throw new IllegalStateException(String.format("Unknown value for sourceType: '%s'",
                                                 config.getSourceType()));
    }

    if (config.includeHeader()) {
      sb.append(" HEADER = TRUE");
    }

    return sb;
  }
}

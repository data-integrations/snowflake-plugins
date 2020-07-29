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

/**
 * Transforms import query.
 */
public class QueryUtil {

  // Matches "limit <number>". Also "limit $$$$" and "limit ''" which means unlimited in Snowflake.
  private static final String LIMIT_PATTERN = "(?i)LIMIT (''|\\$\\$\\$\\$|\\d+)";
  private static final String LIMIT_STRING = "limit %s";

  private QueryUtil() {
  }

  public static String removeSemicolon(String importQuery) {
    if (importQuery.endsWith(";")) {
      importQuery = importQuery.substring(0, importQuery.length() - 1);
    }
    return importQuery;
  }

  public static String limitQuery(String importQuery, int limit) {
    importQuery = removeSemicolon(importQuery);

    String limitString = String.format(LIMIT_STRING, limit);
    if (importQuery.toLowerCase().contains("limit")) {
      return importQuery.replaceAll(LIMIT_PATTERN, limitString);
    }
    return String.format("%s %s", importQuery, limitString);
  }
}

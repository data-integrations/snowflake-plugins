/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.snowflake.common;

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.plugin.snowflake.common.util.DocumentUrlUtil;
import net.snowflake.client.api.exception.ErrorCode;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Error Type provided based on the Snowflake error message code
 *
 **/
public class SnowflakeErrorType {

  private static final Map<Integer, ErrorType> ERROR_CODE_TO_ERROR_TYPE;
  private static final Map<Integer, ErrorCategory> ERROR_CODE_TO_ERROR_CATEGORY;

  //https://github.com/snowflakedb/snowflake-jdbc/blob/master/src/main/java/net/snowflake/client/jdbc/ErrorCode.java
  static {
    ERROR_CODE_TO_ERROR_TYPE = new HashMap<>();
    ERROR_CODE_TO_ERROR_TYPE.put(200004, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200006, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200007, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200008, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200009, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200010, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200011, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200012, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200014, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200017, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200018, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200019, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200021, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200023, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200024, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200025, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200026, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200028, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200029, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200030, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200031, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200032, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200033, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200034, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200035, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200036, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200037, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200038, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200045, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200046, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200047, ErrorType.USER);
    ERROR_CODE_TO_ERROR_TYPE.put(200056, ErrorType.USER);

    ERROR_CODE_TO_ERROR_TYPE.put(200001, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200002, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200003, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200013, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200015, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200016, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200020, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200022, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200039, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200040, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200044, ErrorType.SYSTEM);
    ERROR_CODE_TO_ERROR_TYPE.put(200061, ErrorType.SYSTEM);


    ErrorCategory.ErrorCategoryEnum plugin = ErrorCategory.ErrorCategoryEnum.PLUGIN;
    ERROR_CODE_TO_ERROR_CATEGORY = new HashMap<>();
    ERROR_CODE_TO_ERROR_CATEGORY.put(200004, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200006, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200007, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200008, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200009, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200010, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200011, new ErrorCategory(plugin, "INVALID_AUTHORIZATION_SPECIFICATION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200012, new ErrorCategory(plugin, "INVALID_AUTHORIZATION_SPECIFICATION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200014, new ErrorCategory(plugin, "PROGRAM_LIMIT_EXCEEDED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200017, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200018, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200019, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200021, new ErrorCategory(plugin, "SQL_STATEMENT_NOT_YET_COMPLETE"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200023, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200024, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200025, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200026, new ErrorCategory(plugin, "INVALID_AUTHORIZATION_SPECIFICATION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200028, new ErrorCategory(plugin, "INVALID_AUTHORIZATION_SPECIFICATION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200029, new ErrorCategory(plugin, "INVALID_AUTHORIZATION_SPECIFICATION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200030, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200031, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200032, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200033, new ErrorCategory(plugin, "INVALID_PARAMETER_VALUE"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200034, new ErrorCategory(plugin, "DATA_EXCEPTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200035, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200036, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200037, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200038, new ErrorCategory(plugin, "FEATURE_NOT_SUPPORTED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200045, new ErrorCategory(plugin, "SYNTAX_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200046, new ErrorCategory(plugin, "SYNTAX_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200047, new ErrorCategory(plugin, "INVALID_PARAMETER_VALUE"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200056, new ErrorCategory(plugin, "INVALID_PARAMETER_VALUE"));

    //system errors
    ERROR_CODE_TO_ERROR_CATEGORY.put(200001, new ErrorCategory(plugin, "INTERNAL_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200002,
      new ErrorCategory(plugin, "SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200003, new ErrorCategory(plugin, "QUERY_CANCELED"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200013, new ErrorCategory(plugin, "SYSTEM_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200015, new ErrorCategory(plugin, "IO_ERROR")); //network error
    ERROR_CODE_TO_ERROR_CATEGORY.put(200016, new ErrorCategory(plugin, "IO_ERROR")); //io error
    ERROR_CODE_TO_ERROR_CATEGORY.put(200020, new ErrorCategory(plugin, "SYSTEM_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200022, new ErrorCategory(plugin, "INTERNAL_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200039,
      new ErrorCategory(plugin, "SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200040,
      new ErrorCategory(plugin, "SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200044, new ErrorCategory(plugin, "SYSTEM_ERROR"));
    ERROR_CODE_TO_ERROR_CATEGORY.put(200061, new ErrorCategory(plugin, "SYSTEM_ERROR"));
  }

  /**
   * Method to get the error type based on the error code.
   *
   * @param errorCode the error code to classify
   * @return the corresponding ErrorType (USER, SYSTEM, UNKNOWN)
   */
  private static ErrorType getErrorTypeFromErrorCode(int errorCode) {
    if (ERROR_CODE_TO_ERROR_TYPE.containsKey(errorCode)) {
      return ERROR_CODE_TO_ERROR_TYPE.get(errorCode);
    }
    return ErrorType.UNKNOWN;
  }


  /**
   * Method to get the error type based on the error code.
   *
   * @param errorCode the error code to classify
   * @return the corresponding ErrorCategory
   */
  private static ErrorCategory getErrorCategoryFromSqlState(int errorCode) {
    if (ERROR_CODE_TO_ERROR_CATEGORY.containsKey(errorCode)) {
      return ERROR_CODE_TO_ERROR_CATEGORY.get(errorCode);
    }
    return new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN);
  }

  public static ProgramFailureException fetchProgramFailureException(SQLException e, String errorReason,
                                                                     String errorMessage) {
    Optional<ErrorCode> errorCodes = Arrays.stream(ErrorCode.values())
      .filter(errorCode -> errorCode.getSqlState().equals(e.getSQLState()))
      .findFirst();
    ErrorCategory errorCategory = errorCodes.isPresent() ?
      getErrorCategoryFromSqlState(errorCodes.get().getMessageCode()) :
      new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN);
    ErrorType errorType = errorCodes.isPresent() ? getErrorTypeFromErrorCode(errorCodes.get().getMessageCode()) :
      ErrorType.UNKNOWN;
    return ErrorUtils.getProgramFailureException(errorCategory, errorReason, errorMessage, errorType,
      true, ErrorCodeType.SQLSTATE, e.getSQLState(), DocumentUrlUtil.getSupportedDocumentUrl(), e);
  }
}

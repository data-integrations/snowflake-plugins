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

package io.cdap.plugin.snowflake.common.client;

import com.google.common.base.Strings;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.plugin.common.KeyValueListParser;
import io.cdap.plugin.snowflake.common.BaseSnowflakeConfig;
import io.cdap.plugin.snowflake.common.OAuthUtil;
import io.cdap.plugin.snowflake.common.SnowflakeErrorType;
import io.cdap.plugin.snowflake.common.exception.ConnectionTimeoutException;
import io.cdap.plugin.snowflake.common.util.DocumentUrlUtil;
import io.cdap.plugin.snowflake.common.util.QueryUtil;
import net.snowflake.client.internal.api.implementation.datasource.SnowflakeBasicDataSource;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A class which accesses Snowflake API.
 */
public class SnowflakeAccessor {
  private static final String APPLICATION_NAME = "CDAP";
  private static final int LIMIT_ROWS = 1;

  private final BaseSnowflakeConfig config;
  protected final SnowflakeBasicDataSource dataSource;

  public SnowflakeAccessor(BaseSnowflakeConfig config) {
    this.config = config;
    this.dataSource = new SnowflakeBasicDataSource();
    initDataSource(dataSource, config);
  }

  public void runSQL(String query) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement populateStmt = connection.prepareStatement(query);) {
      populateStmt.execute();
    } catch (SQLException e) {
      String errorMessage = String.format("Statement '%s' failed with SQL state %s and error code %s due to '%s'",
        query, e.getSQLState(), e.getErrorCode(), e.getMessage());
      String errorReason = String.format("Statement '%s' failed with SQL state %s and error code %s. For more " +
        "details see %s.", query, e.getSQLState(), e.getErrorCode(), DocumentUrlUtil.getSupportedDocumentUrl());
      throw SnowflakeErrorType.fetchProgramFailureException(e, errorReason, errorMessage);
    }
  }

  /**
   * Returns field descriptors for specified table name
   * @param schemaName The name of schema containing the table
   * @param tableName The name of table whose metadata needs to be retrieved
   * @return list of field descriptors
   */
  public List<SnowflakeFieldDescriptor> getFieldDescriptors(String schemaName, String tableName) {
    List<SnowflakeFieldDescriptor> fieldDescriptors = new ArrayList<>();
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData dbMetaData = connection.getMetaData();
      try (ResultSet columns = dbMetaData.getColumns(null, schemaName, tableName, null)) {
        while (columns.next()) {
          String columnName = columns.getString("COLUMN_NAME");
          int columnType = columns.getInt("DATA_TYPE");
          boolean nullable = columns.getInt("NULLABLE") == DatabaseMetaData.columnNullable;
          fieldDescriptors.add(new SnowflakeFieldDescriptor(columnName, columnType, nullable));
        }
      }
    } catch (SQLException e) {
      String errorMessage = String.format(
        "Failed to retrieve table metadata with SQL State %s and error code %s with message: %s.",
        e.getSQLState(), e.getErrorCode(), e.getMessage()
      );
      String errorReason = String.format("Failed to retrieve table metadata with SQL State %s and error " +
                                           "code %s. For more details %s", e.getSQLState(), e.getErrorCode(),
                                         DocumentUrlUtil.getSupportedDocumentUrl());
      throw SnowflakeErrorType.fetchProgramFailureException(e, errorReason, errorMessage);
    }
    return fieldDescriptors;
  }

  /**
   * Returns field descriptors for specified import query.
   *
   * @return List of field descriptors.
   * @throws IOException thrown if there are any issue with the I/O operations.
   */
  public List<SnowflakeFieldDescriptor> describeQuery(String query) throws IOException {
    String importQuery = QueryUtil.limitQuery(query, LIMIT_ROWS);
    List<SnowflakeFieldDescriptor> fieldDescriptors = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(importQuery);
         ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String name = metaData.getColumnName(i);
        int type = metaData.getColumnType(i);
        boolean nullable = metaData.isNullable(i) == ResultSetMetaData.columnNullable;
        fieldDescriptors.add(new SnowflakeFieldDescriptor(name, type, nullable));
      }
    } catch (SQLException e) {
      String errorMessage = String.format("Failed to execute query to fetch descriptors with SQL State %s and error " +
        "code %s with message: %s.", e.getSQLState(), e.getErrorCode(), e.getMessage());
      String errorReason = String.format("Failed to execute query to fetch descriptors with SQL State %s and error " +
        "code %s. For more details %s", e.getSQLState(), e.getErrorCode(), DocumentUrlUtil.getSupportedDocumentUrl());
      throw SnowflakeErrorType.fetchProgramFailureException(e, errorReason, errorMessage);
    }
    return fieldDescriptors;
  }

  private void initDataSource(SnowflakeBasicDataSource dataSource, BaseSnowflakeConfig config) {
    dataSource.setDatabaseName(config.getDatabase());
    dataSource.setSchema(config.getSchemaName());
    dataSource.setUrl(String.format("jdbc:snowflake://%s.snowflakecomputing.com", config.getAccountName()));

    String warehouse = config.getWarehouse();
    if (!Strings.isNullOrEmpty(warehouse)) {
      dataSource.setWarehouse(warehouse);
    }

    String role = config.getRole();
    if (!Strings.isNullOrEmpty(role)) {
      dataSource.setRole(role);
    }

    if (Boolean.TRUE.equals(config.getOauth2Enabled())) {
      String accessToken = OAuthUtil.getAccessTokenByRefreshToken(HttpClients.createDefault(), config);
      // In JDBC 4.x, setOauthToken() was removed. The recommended approach is to explicitly
      // set the authenticator to "oauth" and pass the access token as the password.
      // Migration guide: https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-migration
      dataSource.setAuthenticator("oauth");
      dataSource.setPassword(accessToken);
    } else if (Boolean.TRUE.equals(config.getKeyPairEnabled())) {
      dataSource.setUser(config.getUsername());

      String privateKeyPath = writeTextToTmpFile(config.getPrivateKey());
      try {
        dataSource.setPrivateKeyFile(privateKeyPath, config.getPassphrase());
      } finally {
        new File(privateKeyPath).deleteOnExit();
      }
    } else {
      dataSource.setUser(config.getUsername());
      dataSource.setPassword(config.getPassword());
    }
    String connectionArguments = config.getConnectionArguments();
    if (!Strings.isNullOrEmpty(connectionArguments)) {
      addConnectionArguments(dataSource, connectionArguments);
    }
  }

  /**
   * Checks connection to the service by testing API endpoint, in case
   * of exception would be generated {@link ConnectionTimeoutException}
   */
  public void checkConnection() {
    try (Connection connection = dataSource.getConnection()) {
      connection.getMetaData();
    } catch (SQLException e) {
      throw new ConnectionTimeoutException("Cannot create Snowflake connection.", e);
    } catch (NullPointerException e) {
      String errorMessage = String.format("Failed to create Snowflake connection due to missing Username or password " +
        "with message: %s.", e.getMessage());
      String errorReason = "Cannot create Snowflake connection. Username or password is missing.";
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorMessage, ErrorType.USER, true, e);
    }
  }
  // SnowflakeBasicDataSource doesn't provide access for additional properties.
  private void addConnectionArguments(SnowflakeBasicDataSource dataSource, String connectionArguments) {
    try {
      Class<? extends SnowflakeBasicDataSource> dataSourceClass = dataSource.getClass();
      Field propertiesField = dataSourceClass.getDeclaredField("properties");
      propertiesField.setAccessible(true);
      Properties properties = (Properties) propertiesField.get(dataSource);
      for (KeyValue<String, String> argument : KeyValueListParser.DEFAULT.parse(connectionArguments)) {
        properties.setProperty(argument.getKey(), argument.getValue());
      }
      properties.setProperty("application", APPLICATION_NAME);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalArgumentException(
        String.format("Cannot set connection arguments '%s'.", connectionArguments), e);
    }
  }

  private static String writeTextToTmpFile(String text) {
    try {
      File temp = File.createTempFile("cdap_key", ".tmp");
      temp.setReadable(true, true); // set readable only by owner

      try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
        bw.write(text);
      }
      return temp.getPath();
    } catch (IOException e) {
      throw new RuntimeException("Cannot write key to temporary file", e);
    }
  }

  /**
   * Retrieves schema name from the configuration
   *
   * @return The schema name
   */
  public String getSchema() {
    return config.getSchemaName();
  }
}

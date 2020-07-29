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

package io.cdap.plugin.snowflake.common;

import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.plugin.snowflake.Constants;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessorTest;
import io.cdap.plugin.snowflake.source.batch.SnowflakeBatchSourceConfig;
import io.cdap.plugin.snowflake.source.batch.SnowflakeBatchSourceConfigBuilder;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Tests to verify connection to Snowflake.
 * <p>
 * By default all tests will be skipped, since Snowflake credentials are needed.
 * <p>
 * Instructions to enable the tests:
 * 1. Create/use existing Snowflake account.
 * 2. Create database for testing.
 * 3. Run the tests using the command below:
 * <p>
 * mvn clean test
 * -Dsnowflake.test.account.name=
 * -Dsnowflake.test.database=
 * -Dsnowflake.test.schema=
 * -Dsnowflake.test.username=
 * -Dsnowflake.test.password=
 */
public abstract class BaseSnowflakeTest extends HydratorTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeAccessorTest.class);

  protected static final String ACCOUNT_NAME = System.getProperty("snowflake.test.account.name");
  protected static final String DATABASE = System.getProperty("snowflake.test.database");
  protected static final String SCHEMA = System.getProperty("snowflake.test.schema");
  protected static final String USERNAME = System.getProperty("snowflake.test.username");
  protected static final String PASSWORD = System.getProperty("snowflake.test.password");

  protected static final SnowflakeBatchSourceConfig CONFIG =
    new SnowflakeBatchSourceConfigBuilder(SnowflakeBatchSourceConfigBuilder.CONFIG)
      .setAccountName(ACCOUNT_NAME)
      .setDatabase(DATABASE)
      .setSchemaName(SCHEMA)
      .setUsername(USERNAME)
      .setPassword(PASSWORD)
      .setImportQuery(Constants.IMPORT_QUERY)
      .build();

  protected static SnowflakeBasicDataSource dataSource;

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setupBasic() throws Exception {
    assertProperties();
    buildDataSource();
    setupTable();
  }

  private static void assertProperties() {
    try {
      Assume.assumeNotNull(ACCOUNT_NAME, DATABASE, USERNAME, PASSWORD);
    } catch (AssumptionViolatedException e) {
      LOG.warn("Tests are skipped. Please find the instructions on enabling it at " +
                 "BaseSnowflakeTest javadoc.");
      throw e;
    }
  }

  private static void buildDataSource() {
    dataSource = new SnowflakeBasicDataSource();
    dataSource.setDatabaseName(DATABASE);
    dataSource.setSchema(SCHEMA);
    dataSource.setUrl(String.format("jdbc:snowflake://%s.snowflakecomputing.com", ACCOUNT_NAME));
    dataSource.setUser(USERNAME);
    dataSource.setPassword(PASSWORD);
  }

  private static void setupTable() throws SQLException {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement dropTableStmt = connection.prepareStatement(Constants.DROP_TEST_TABLE);
         PreparedStatement createTableStmt = connection.prepareStatement(Constants.CREATE_TEST_TABLE);
         PreparedStatement insertStmt = connection.prepareStatement(Constants.INSERT_INTO_TEST_TABLE)) {
      dropTableStmt.execute();
      createTableStmt.execute();
      insertStmt.execute();
    }
  }
}

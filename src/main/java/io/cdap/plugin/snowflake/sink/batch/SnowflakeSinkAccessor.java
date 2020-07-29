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

package io.cdap.plugin.snowflake.sink.batch;

import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/**
 * A class which accesses Snowflake API to do actions used by batch sink.
 */
public class SnowflakeSinkAccessor extends SnowflakeAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeSinkAccessor.class);
  private static final String POPULATE_TABLE_STAGE = "COPY INTO %s FROM %s " +
    "FILE_FORMAT=(" +
    "TYPE='CSV' " +
    "FIELD_OPTIONALLY_ENCLOSED_BY='\"' " +
    "SKIP_HEADER = 1) %s";

  private static final String DEST_FILE_NAME = "cdapRecords_%s.csv";
  private final SnowflakeSinkConfig config;

  public SnowflakeSinkAccessor(SnowflakeSinkConfig config) {
    super(config);
    this.config = config;
  }

  public void uploadStream(InputStream inputStream, String stageDir) throws IOException {
    // file name needs to be unique across all the nodes.
    String filename = String.format(DEST_FILE_NAME, UUID.randomUUID().toString());
    LOG.info("Uploading file '{}' to table stage", filename);

    try (Connection connection = dataSource.getConnection()) {
      connection.unwrap(SnowflakeConnection.class).uploadStream(stageDir,
                                                                null,
                                                                inputStream, filename, true);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  public void populateTable(String destinationStagePath) throws IOException {
    String populateStatement = String.format(POPULATE_TABLE_STAGE, config.getTableName(),
                                             destinationStagePath, config.getCopyOptions());
    runSQL(populateStatement);
  }

  public void removeDirectory(String path) throws IOException {
    runSQL(String.format("remove %s", path));
  }
}

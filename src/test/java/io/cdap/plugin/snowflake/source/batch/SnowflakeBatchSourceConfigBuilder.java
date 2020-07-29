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

/**
 * Helper class to simplify {@link SnowflakeBatchSourceConfig} class creation.
 */
public class SnowflakeBatchSourceConfigBuilder {

  public static final SnowflakeBatchSourceConfig CONFIG = new SnowflakeBatchSourceConfig(
    "ref",
    "accountName",
    "database",
    "schemaName",
    "importQuery",
    "username",
    "password",
    false,
    "",
    "",
    false,
    "",
    "",
    "",
    0L,
    "",
    "");

  private String referenceName;
  private String accountName;
  private String database;
  private String schemaName;
  private String importQuery;
  private String username;
  private String password;
  private Boolean keyPairEnabled;
  private String path;
  private String passphrase;
  private Boolean oauth2Enabled;
  private String clientId;
  private String clientSecret;
  private String refreshToken;
  private Long maxSplitSize;
  private String connectionArguments;
  private String schema;

  public SnowflakeBatchSourceConfigBuilder() {
  }

  public SnowflakeBatchSourceConfigBuilder(SnowflakeBatchSourceConfig config) {
    this.referenceName = config.getReferenceName();
    this.accountName = config.getAccountName();
    this.database = config.getDatabase();
    this.schemaName = config.getSchemaName();
    this.importQuery = config.getImportQuery();
    this.username = config.getUsername();
    this.password = config.getPassword();
    this.keyPairEnabled = config.getKeyPairEnabled();
    this.path = config.getPrivateKey();
    this.passphrase = config.getPassphrase();
    this.oauth2Enabled = config.getOauth2Enabled();
    this.clientId = config.getClientId();
    this.clientSecret = config.getClientSecret();
    this.refreshToken = config.getRefreshToken();
    this.maxSplitSize = config.getMaxSplitSize();
    this.connectionArguments = config.getConnectionArguments();
    this.schema = config.getSchema();
  }

  public SnowflakeBatchSourceConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setAccountName(String accountName) {
    this.accountName = accountName;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setDatabase(String database) {
    this.database = database;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setSchemaName(String schemaName) {
    this.schemaName = schemaName;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setImportQuery(String importQuery) {
    this.importQuery = importQuery;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setUsername(String username) {
    this.username = username;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setKeyPairEnabled(Boolean keyPairEnabled) {
    this.keyPairEnabled = keyPairEnabled;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setPath(String path) {
    this.path = path;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setPassphrase(String passphrase) {
    this.passphrase = passphrase;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setOauth2Enabled(Boolean oauth2Enabled) {
    this.oauth2Enabled = oauth2Enabled;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setClientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setRefreshToken(String refreshToken) {
    this.refreshToken = refreshToken;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setMaxSplitSize(Long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setConnectionArguments(String connectionArguments) {
    this.connectionArguments = connectionArguments;
    return this;
  }

  public SnowflakeBatchSourceConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public SnowflakeBatchSourceConfig build() {
    return new SnowflakeBatchSourceConfig(referenceName,
                                          accountName,
                                          database,
                                          schemaName,
                                          importQuery,
                                          username,
                                          password,
                                          keyPairEnabled,
                                          path,
                                          passphrase,
                                          oauth2Enabled,
                                          clientId,
                                          clientSecret,
                                          refreshToken,
                                          maxSplitSize,
                                          connectionArguments,
                                          schema);
  }
}

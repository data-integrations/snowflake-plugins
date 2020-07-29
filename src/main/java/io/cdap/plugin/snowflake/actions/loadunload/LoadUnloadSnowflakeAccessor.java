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

package io.cdap.plugin.snowflake.actions.loadunload;

import com.google.common.base.Strings;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * A class which accesses Snowflake API to do copy query for load and unload actions.
 */
public abstract class LoadUnloadSnowflakeAccessor extends SnowflakeAccessor {
  private static final Logger LOG = LoggerFactory.getLogger(LoadUnloadSnowflakeAccessor.class);

  private final LoadUnloadConfig config;

  public LoadUnloadSnowflakeAccessor(LoadUnloadConfig config) {
    super(config);
    this.config = config;
  }

  protected abstract StringBuilder getQueryPrefix();

  public void runCopy() throws IOException {
    StringBuilder sb = getQueryPrefix();

    switch(config.getFileFormatFilteringPolicy()) {
      case UNDEFINED:
        break;
      case BY_FILE_TYPE:
        sb.append(String.format(" FILE_FORMAT = ( TYPE = '%s' %s )",
                                config.getFormatType(), config.getFormatTypeOptions()));
        break;
      case BY_EXISTING_FORMAT:
        sb.append(String.format(" FILE_FORMAT = ( FORMAT_NAME = '%s' )", config.getFormatName()));
        break;
      default:
        throw new IllegalStateException(String.format("Unknown value for fileFormatFilteringPolicy: '%s'",
                                                 config.getFileFormatFilteringPolicy()));
    }

    if (!Strings.isNullOrEmpty(config.getCopyOptions())) {
      sb.append(String.format(" %s", config.getCopyOptions()));
    }

    if (config.getUseCloudProviderParameters()) {
      if (!Strings.isNullOrEmpty(config.getStorageIntegration())) {
        sb.append(String.format(" STORAGE_INTEGRATION = %s", config.getStorageIntegration()));
      }

      switch (config.getCloudProvider()) {
        case GCP:
          // no special parameters
          break;
        case AWS:
          StringBuilder credentialsOptionsSb = new StringBuilder("");

          if (!Strings.isNullOrEmpty(config.getAwsKeyId())) {
            credentialsOptionsSb.append(String.format(" AWS_KEY_ID = '%s'", config.getAwsKeyId()));
          }
          if (!Strings.isNullOrEmpty(config.getAwsSecretKey())) {
            credentialsOptionsSb.append(String.format(" AWS_SECRET_KEY = '%s'", config.getAwsSecretKey()));
          }
          if (!Strings.isNullOrEmpty(config.getAwsToken())) {
            credentialsOptionsSb.append(String.format(" AWS_TOKEN = '%s'", config.getAwsToken()));
          }
          String credentialsOptions = credentialsOptionsSb.toString();

          if (!Strings.isNullOrEmpty(credentialsOptions)) {
            sb.append(String.format(" CREDENTIALS = ( %s )", credentialsOptions));
          }

          break;
        case AZURE:
          if (!Strings.isNullOrEmpty(config.getAzureSasToken())) {
            sb.append(String.format(" CREDENTIALS = ( AZURE_SAS_TOKEN = '%s' )", config.getAzureSasToken()));
          }
          break;
        default:
          throw new IllegalStateException(String.format("Unknown value for cloudProvider: '%s'",
                                                   config.getCloudProvider()));
      }
      sb.append(getFilesEncryptionParams());
    }

    String query = sb.toString();
    LOG.info("Running query '{}'", query);

    runSQL(query);
  }

  private String getFilesEncryptionParams() {
    if (config.getFilesEncrypted()) {
      String encryptionParameter = "";
      switch (config.getEncryptionType()) {
        case AZURE_CSE:
        case AWS_CSE:
          if (!Strings.isNullOrEmpty(config.getMasterKey())) {
            encryptionParameter = String.format("MASTER_KEY = '%s'", config.getMasterKey());
          }
          break;
        case AWS_SSE_KMS:
        case GCS_SSE_KMS:
          if (!Strings.isNullOrEmpty(config.getKmsKeyId())) {
            encryptionParameter = String.format("KMS_KEY_ID = '%s'", config.getKmsKeyId());
          }
          break;
        case AWS_SSE_S3:
          break;
        default:
          throw new IllegalStateException(String.format("Unknown value for encryptionType: '%s'",
                                                   config.getEncryptionType()));
      }

      return String.format(" ENCRYPTION = ( TYPE = '%s' %s ) ", config.getEncryptionType().getValue(),
                           encryptionParameter);
    }
    return "";
  }

  // According to Snowflake COPY syntax external paths must be quoted, while internal must not.
  public static String quotePathIfNeeded(String path) {
    if (path.startsWith("gcs://") || path.startsWith("azure://") ||
      path.startsWith("s3://")) {
      return "'" + path + "'";
    }
    return path;
  }
}

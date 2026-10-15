/*
 * Copyright © 2026 Cask Data, Inc.
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

import net.snowflake.client.internal.jdbc.cloud.storage.SnowflakeStorageClient;
import net.snowflake.client.internal.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.internal.jdbc.cloud.storage.StorageClientFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Tests that the S3 client used by the driver's downloadStream accepts the stage endpoint returned by Snowflake.
 * Government accounts return an endpoint without a URI scheme, which JDBC 4.0.2 rejected (snowflake-jdbc#2572).
 */
public class SnowflakeS3StageEndpointTest {

  private static final String GOV_REGION = "us-gov-west-1";
  private static final String GOV_ENDPOINT = "s3-fips.us-gov-west-1.amazonaws.com";

  @Test
  public void testSchemelessStageEndpointIsNormalized() throws Exception {
    Assert.assertEquals(Optional.of(URI.create("https://" + GOV_ENDPOINT)), getEndpointOverride(GOV_ENDPOINT));
  }

  @Test
  public void testHttpsStageEndpointIsUnchanged() throws Exception {
    Assert.assertEquals(Optional.of(URI.create("https://" + GOV_ENDPOINT)),
                        getEndpointOverride("https://" + GOV_ENDPOINT));
  }

  @Test
  public void testNoStageEndpoint() throws Exception {
    Assert.assertEquals(Optional.empty(), getEndpointOverride(null));
  }

  /**
   * Creates the S3 client the same way the driver's downloadStream does and returns its endpoint override.
   */
  private static Object getEndpointOverride(String stageEndPoint) throws Exception {
    Map<String, String> credentials = new HashMap<>();
    credentials.put("AWS_KEY_ID", "test-key-id");
    credentials.put("AWS_SECRET_KEY", "test-secret");
    credentials.put("AWS_TOKEN", "test-token");
    StageInfo stageInfo = StageInfo.createStageInfo("S3", "bucket/stage/", credentials, GOV_REGION,
                                                    stageEndPoint, null, true);
    SnowflakeStorageClient client = StorageClientFactory.getFactory().createClient(stageInfo, 1, null, null);
    try {
      // The AWS SDK is shaded inside the driver, so the endpoint is read reflectively.
      Field amazonClientField = client.getClass().getDeclaredField("amazonClient");
      amazonClientField.setAccessible(true);
      Object amazonClient = amazonClientField.get(client);
      Object clientConfig = amazonClient.getClass().getMethod("serviceClientConfiguration").invoke(amazonClient);
      Method endpointOverride = clientConfig.getClass().getMethod("endpointOverride");
      endpointOverride.setAccessible(true);
      return endpointOverride.invoke(clientConfig);
    } finally {
      // Without a session the client owns an SDK-managed Netty event loop group, which shutdown() releases.
      client.shutdown();
    }
  }
}

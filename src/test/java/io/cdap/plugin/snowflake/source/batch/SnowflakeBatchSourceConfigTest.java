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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.snowflake.ValidationAssertions;
import org.junit.Assert;
import org.junit.Test;
import java.util.Collections;

/**
 * Tests for {@link SnowflakeBatchSourceConfig}
 */
public class SnowflakeBatchSourceConfigTest {

  private static final String MOCK_STAGE = "mockStage";

  @Test
  public void validate() {
    SnowflakeBatchSourceConfig config = SnowflakeBatchSourceConfigBuilder.CONFIG;

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    collector.getValidationFailures().remove(collector.getValidationFailures().size() - 1);
    Assert.assertTrue(collector.getValidationFailures().isEmpty());
  }

  @Test
  public void validateUsername() {
    SnowflakeBatchSourceConfig config =
      new SnowflakeBatchSourceConfigBuilder(SnowflakeBatchSourceConfigBuilder.CONFIG)
        .setUsername("")
        .build();

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    collector.getValidationFailures().remove(collector.getValidationFailures().size() - 1);

    ValidationAssertions.assertValidationFailed(
      collector, Collections.singletonList(SnowflakeBatchSourceConfig.PROPERTY_USERNAME));
  }

  @Test
  public void validatePassword() {
    SnowflakeBatchSourceConfig config =
      new SnowflakeBatchSourceConfigBuilder(SnowflakeBatchSourceConfigBuilder.CONFIG)
        .setPassword("")
        .build();

    MockFailureCollector collector = new MockFailureCollector(MOCK_STAGE);
    config.validate(collector);
    collector.getValidationFailures().remove(collector.getValidationFailures().size() - 1);

    ValidationAssertions.assertValidationFailed(
      collector, Collections.singletonList(SnowflakeBatchSourceConfig.PROPERTY_PASSWORD));
  }
}

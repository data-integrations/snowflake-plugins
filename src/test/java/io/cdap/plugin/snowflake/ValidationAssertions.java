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

package io.cdap.plugin.snowflake;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class to simplify config validation.
 */
public class ValidationAssertions {

  public static void assertValidationFailed(MockFailureCollector failureCollector, List<String> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    String attribute = CauseAttributes.STAGE_CONFIG;
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, attribute);
    Assert.assertEquals(paramNames.size(), causeList.size());
    IntStream.range(0, paramNames.size())
      .forEachOrdered(i -> Assert.assertEquals(
        paramNames.get(i), causeList.get(i).getAttribute(attribute)));
  }

  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String attribute) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(attribute) != null)
      .collect(Collectors.toList());
  }

  public static void assertTestTableResults(StructuredRecord expected, StructuredRecord actual) {
    Assert.assertNotNull(actual);
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_NUMBER"), actual.get("COLUMN_NUMBER"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_DECIMAL"), actual.get("COLUMN_DECIMAL"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_NUMERIC"), actual.get("COLUMN_NUMERIC"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_INT"), actual.get("COLUMN_INT"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_INTEGER"), actual.get("COLUMN_INTEGER"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_BIGINT"), actual.get("COLUMN_BIGINT"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_SMALLINT"), actual.get("COLUMN_SMALLINT"));
    Assert.assertEquals((double) expected.get("COLUMN_FLOAT"), actual.get("COLUMN_FLOAT"), 0.001d);
    Assert.assertEquals((double) expected.get("COLUMN_FLOAT4"), actual.get("COLUMN_FLOAT4"), 0.001d);
    Assert.assertEquals((double) expected.get("COLUMN_FLOAT8"), actual.get("COLUMN_FLOAT8"), 0.001d);
    Assert.assertEquals((double) expected.get("COLUMN_DOUBLE"), actual.get("COLUMN_DOUBLE"), 0.001d);
    Assert.assertEquals(
      (double) expected.get("COLUMN_DOUBLE_PRECISION"), actual.get("COLUMN_DOUBLE_PRECISION"), 0.001d);
    Assert.assertEquals((double) expected.get("COLUMN_REAL"), actual.get("COLUMN_REAL"), 0.001d);
    Assert.assertEquals((String) expected.get("COLUMN_VARCHAR"), actual.get("COLUMN_VARCHAR"));
    Assert.assertEquals((String) expected.get("COLUMN_CHAR"), actual.get("COLUMN_CHAR"));
    Assert.assertEquals((String) expected.get("COLUMN_CHARACTER"), actual.get("COLUMN_CHARACTER"));
    Assert.assertEquals((String) expected.get("COLUMN_STRING"), actual.get("COLUMN_STRING"));
    Assert.assertEquals((String) expected.get("COLUMN_TEXT"), actual.get("COLUMN_TEXT"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_BINARY"), actual.get("COLUMN_BINARY"));
    Assert.assertArrayEquals((byte[]) expected.get("COLUMN_VARBINARY"), actual.get("COLUMN_VARBINARY"));
    Assert.assertEquals((boolean) expected.get("COLUMN_BOOLEAN"), actual.get("COLUMN_BOOLEAN"));
    Assert.assertEquals((int) expected.get("COLUMN_DATE"), (int) actual.get("COLUMN_DATE"));
    Assert.assertEquals((long) expected.get("COLUMN_DATETIME"), (long) actual.get("COLUMN_DATETIME"));
    Assert.assertEquals((long) expected.get("COLUMN_TIME"), (long) actual.get("COLUMN_TIME"));
    Assert.assertEquals((long) expected.get("COLUMN_TIMESTAMP"), (long) actual.get("COLUMN_TIMESTAMP"));
    Assert.assertEquals((long) expected.get("COLUMN_TIMESTAMP_LTZ"), (long) actual.get("COLUMN_TIMESTAMP_LTZ"));
    Assert.assertEquals((long) expected.get("COLUMN_TIMESTAMP_NTZ"), (long) actual.get("COLUMN_TIMESTAMP_NTZ"));
    Assert.assertEquals((long) expected.get("COLUMN_TIMESTAMP_TZ"), (long) actual.get("COLUMN_TIMESTAMP_TZ"));
    Assert.assertEquals((String) expected.get("COLUMN_VARIANT"), actual.get("COLUMN_VARIANT"));
    Assert.assertEquals((String) expected.get("COLUMN_OBJECT"), actual.get("COLUMN_OBJECT"));
    Assert.assertEquals((String) expected.get("COLUMN_ARRAY"), actual.get("COLUMN_ARRAY"));
  }
}

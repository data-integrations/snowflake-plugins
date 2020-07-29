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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link QueryUtil}
 */
public class QueryUtilTest {

  @Test
  public void testRemoveSemicolon() {
    String expected = "select * from table";

    String actual = QueryUtil.removeSemicolon(expected);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testRemoveSemicolonSemicolon() {
    String expected = "select * from table";

    String query = "select * from table;";

    String actual = QueryUtil.removeSemicolon(query);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQuery() {
    String expected = "select * from table limit 5";

    String query = "select * from table";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQuerySemicolon() {
    String expected = "select * from table limit 5";

    String query = "select * from table;";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQueryWithLimit() {
    String expected = "select * from table limit 5";

    String query = "select * from table limit 10";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQueryWithLimitSemicolon() {
    String expected = "select * from table limit 5";

    String query = "select * from table limit 10;";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQueryRegexp() {
    String query = "select * from table LiMiT 10";
    String expected = "select * from table limit 5";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQueryRegexpQuote() {
    String expected = "select * from table limit 5";

    String query = "select * from table LiMiT ''";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQueryRegexpNull() {
    String expected = "select * from table limit 5";

    String query = "select * from table LiMiT null";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testLimitQueryRegexpDollarSign() {
    String expected = "select * from table limit 5";

    String query = "select * from table LiMiT $$$$";

    String actual = QueryUtil.limitQuery(query, 5);

    Assert.assertNotNull(actual);
    Assert.assertEquals(expected, actual);
  }
}

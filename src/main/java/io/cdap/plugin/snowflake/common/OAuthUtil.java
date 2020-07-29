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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import io.cdap.plugin.snowflake.common.exception.ConnectionTimeoutException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Base64;

/**
 * A class which contains utilities to make OAuth2 specific calls.
 */
public class OAuthUtil {

  private static final JsonParser parser = new JsonParser();

  public static String getAccessTokenByRefreshToken(CloseableHttpClient httpclient,
                                                    BaseSnowflakeConfig config) {
    try {
      String tokenUrl = String.format("https://%s.snowflakecomputing.com/oauth/token-request",
                                      config.getAccountName());
      URI uri = new URIBuilder(tokenUrl).build();

      HttpPost httppost = new HttpPost(uri);
      httppost.setHeader("Content-type", "application/x-www-form-urlencoded");

      // set grant type and refresh_token. It should be in body not url!
      StringEntity entity = new StringEntity(String.format("refresh_token=%s&grant_type=refresh_token",
                                                           URLEncoder.encode(config.getRefreshToken(), "UTF-8")));
      httppost.setEntity(entity);

      // set 'Authorization' header
      String stringToEncode = config.getClientId() + ":" + config.getClientSecret();
      String encondedAuthorization = new String(Base64.getEncoder().encode(stringToEncode.getBytes()));
      httppost.setHeader("Authorization", String.format("Basic %s", encondedAuthorization));


      CloseableHttpResponse response = httpclient.execute(httppost);
      String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

      JsonElement jsonElement = null;
      try {
        jsonElement = parser.parse(responseString).getAsJsonObject().get("access_token");
      } catch (JsonSyntaxException ex) {
        // this will be handled below
      }

      // if exception happened during parsing OR if json does not contain 'access_token' key.
      if (jsonElement == null) {
        throw new RuntimeException(String.format("Unexpected response '%s' from '%s'", responseString, uri.toString()));
      }

      return jsonElement.getAsString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build token URI for OAuth2.", e);
    } catch (IOException e) {
      throw new ConnectionTimeoutException("Failed to get refresh token for OAuth2.", e);
    }
  }
}

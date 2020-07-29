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

/**
 * Cloud provider type
 */
public enum EncryptionType implements EnumWithValue {
  NONE("NONE"),
  AWS_CSE("AWS_CSE"),
  AWS_SSE_S3("AWS_SSE_S3"),
  AWS_SSE_KMS("AWS_SSE_KMS"),
  AZURE_CSE("AZURE_CSE"),
  GCS_SSE_KMS("GCS_SSE_KMS");

  private final String value;

  EncryptionType(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return this.getValue();
  }
}

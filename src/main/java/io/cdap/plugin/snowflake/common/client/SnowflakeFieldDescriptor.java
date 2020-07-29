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

package io.cdap.plugin.snowflake.common.client;

import java.util.Objects;

/**
 * Contains information about field.
 * Can contain field name, type and nullable flag.
 */
public class SnowflakeFieldDescriptor {

  private String name;
  private Integer type;
  private Boolean nullable;

  public SnowflakeFieldDescriptor(String name, Integer type,
                                  Boolean nullable) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
  }

  public String getName() {
    return name;
  }

  public Integer getType() {
    return type;
  }

  public Boolean getNullable() {
    return nullable;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnowflakeFieldDescriptor that = (SnowflakeFieldDescriptor) o;
    return Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      Objects.equals(nullable, that.nullable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, nullable);
  }
}

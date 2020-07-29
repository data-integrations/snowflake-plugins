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

package io.cdap.plugin.snowflake.actions.sql;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.snowflake.common.client.SnowflakeAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an arbitrary SQL query on Snowflake.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("SnowflakeRunSQL")
@Description("Runs an arbitrary SQL query on Snowflake.")
public class RunSQLAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(RunSQLAction.class);

  private final RunSQLConfig config;

  public RunSQLAction(RunSQLConfig config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    SnowflakeAccessor snowflakeAccessor = new SnowflakeAccessor(config);
    snowflakeAccessor.runSQL(config.getQuery());
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getFailureCollector());
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A pipeline execution that run the defined pipeline via Flink's {@link
 * StreamExecutionEnvironment}.
 */
public class FlinkPipelineExecution implements PipelineExecution {

    private final StreamExecutionEnvironment env;
    private final String jobName;
    private final boolean isBlocking;

    public FlinkPipelineExecution(
            StreamExecutionEnvironment env, String jobName, boolean isBlocking) {
        this.env = env;
        this.jobName = jobName;
        this.isBlocking = isBlocking;
    }

    @Override
    public ExecutionInfo execute() throws Exception {
        //        JobClient jobClient = env.executeAsync(jobName);
        //        if (isBlocking) {
        //            jobClient.getJobExecutionResult().get();
        //        }
        //        return new ExecutionInfo(jobClient.getJobID().toString(), jobName);
        System.out.println("启动任务");
        JobExecutionResult jobExecutionResult = env.execute(jobName);
        return new ExecutionInfo(jobExecutionResult.getJobID().toString(), jobName);
    }
}

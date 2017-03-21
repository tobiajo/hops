/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.tensorflow;

import org.apache.commons.cli.Options;

public class ClientArguments extends CommonArguments {
  
  public static final String JAR = "jar";
  public static final String AM_MEMORY = "master_memory";
  public static final String AM_VCORES = "master_vcores";
  
  public static final String NAME = "appname";
  public static final String MAIN = "main";
  public static final String ARGS = "shell_args";
  public static final String PY_FILES = "py_files";
  public static final String FILES = "files";
  
  public static final String GPU = "gpu";
  public static final String RDMA = "rdma";
  public static final String TENSORBOARD = "tensorboard";
  
  public static Options createOptions() {
    Options opts = CommonArguments.createOptions();
    opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
    opts.addOption("timeout", true, "Application timeout in milliseconds");
    opts.addOption("shell_command", true, "Shell command to be executed by " +
        "the Application Master. Can only specify either --shell_command " +
        "or --shell_script");
    opts.addOption("shell_script", true, "Location of the shell script to be " +
        "executed. Can only specify either --shell_command or --shell_script");
    opts.addOption("shell_cmd_priority", true, "Priority for the shell command containers");
    opts.addOption("log_properties", true, "log4j.properties file");
    opts.addOption("keep_containers_across_application_attempts", false,
        "Flag to indicate whether to keep containers across application attempts." +
            " If the flag is true, running containers will not be killed when" +
            " application attempt fails and these containers will be retrieved by" +
            " the new application attempt ");
    opts.addOption("attempt_failures_validity_interval", true,
        "when attempt_failures_validity_interval in milliseconds is set to > 0," +
            "the failure number will not take failures which happen out of " +
            "the validityInterval into failure count. " +
            "If failure count reaches to maxAppAttempts, " +
            "the application will be failed.");
    opts.addOption("domain", true, "ID of the timeline domain where the "
        + "timeline entities will be put");
    opts.addOption("view_acls", true, "Users and groups that allowed to "
        + "view the timeline entities in the given domain");
    opts.addOption("modify_acls", true, "Users and groups that allowed to "
        + "modify the timeline entities in the given domain");
    opts.addOption("create", false, "Flag to indicate whether to create the "
        + "domain specified with -domain.");
    opts.addOption("node_label_expression", true,
        "Node label expression to determine the nodes"
            + " where all the containers of this application"
            + " will be allocated, \"\" means containers"
            + " can be allocated anywhere, if you don't specify the option,"
            + " default node_label_expression of queue will be used.");
  
    // YarnTF options
    opts.addOption(JAR, true, "Jar file containing the application master");
    opts.addOption(AM_MEMORY, true, "Amount of memory in MB to be requested to run the application master");
    opts.addOption(AM_VCORES, true, "Amount of virtual cores to be requested to run the application master");
    
    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
  
    opts.addOption(MAIN, true, "Your application's main Python file.");
    opts.addOption(ARGS, true, "Command line args for the application. Multiple args can be separated by empty space.");
    opts.addOption(NAME, true, "A name of your application.");
    opts.addOption(PY_FILES, true, "Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.");
    opts.addOption(FILES, true, "Comma-separated list of files to be placed in the working directory of each node.");
    // TODO: Make us of args below
    opts.addOption(GPU, false, "Enable GPU");
    opts.addOption(RDMA, false, "Enable RDMA");
    opts.addOption(TENSORBOARD, false, "Enable TensorBoard");
    return opts;
  }
}

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

public class CommonArguments {
  
  public static final String WORKERS = "workers";
  public static final String PSES = "pses";
  
  protected static Options createOptions() {
    Options opts = new Options();
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("debug", false, "Dump out debug information");
    opts.addOption("help", false, "Print usage");
    opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("container_vcores", true, "Amount of virtual cores to be requested to run the shell command");
    opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
    opts.addOption("shell_env", true, "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption(WORKERS, true, "Number of workers");
    opts.addOption(PSES, true, "Number of parameter servers");
    return opts;
  }
}

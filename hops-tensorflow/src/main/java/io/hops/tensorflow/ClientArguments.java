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

public class ClientArguments {
  
  public static final String JAR = "jar";
  public static final String AM_MEMORY = "master_memory";
  public static final String AM_VCORES = "master_vcores";
  
  public static final String NAME = "appname";
  public static final String MAIN = "main";
  public static final String ARG = "shell_args";
  public static final String PY_FILES = "py_files";
  public static final String FILES = "files";
  
  public static final String WORKER = "worker";
  public static final String PS = "ps";
  
  public static final String GPU = "gpu";
  public static final String RDMA = "rdma";
  public static final String TENSORBOARD = "tensorboard";
}

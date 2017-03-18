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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.hops.tensorflow.clusterspecgen.ClusterSpecGenGrpc;
import io.hops.tensorflow.clusterspecgen.Container;
import io.hops.tensorflow.clusterspecgen.GetClusterSpecReply;
import io.hops.tensorflow.clusterspecgen.GetClusterSpecRequest;
import io.hops.tensorflow.clusterspecgen.RegisterContainerReply;
import io.hops.tensorflow.clusterspecgen.RegisterContainerRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

public class ClusterSpecGenClient {
  
  private static final Log LOG = LogFactory.getLog(ClusterSpecGenClient.class);
  
  private final ManagedChannel channel;
  private final ClusterSpecGenGrpc.ClusterSpecGenBlockingStub blockingStub;
  
  public ClusterSpecGenClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
  }
  
  private ClusterSpecGenClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = ClusterSpecGenGrpc.newBlockingStub(channel);
  }
  
  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }
  
  public boolean registerContainer(String applicationId, String containerId,
      String ip, int port, String jobName, int taskIndex) {
    Container container = Container.newBuilder()
        .setApplicationId(applicationId)
        .setContainerId(containerId)
        .setIp(ip)
        .setPort(port)
        .setJobName(jobName)
        .setTaskIndex(taskIndex)
        .build();
    RegisterContainerRequest request = RegisterContainerRequest.newBuilder().setContainer(container).build();
    try {
      blockingStub.registerContainer(request);
    } catch (StatusRuntimeException e) {
      LOG.warn("RPC failed: " + e.getStatus());
      return false;
    }
    return true;
  }
  
  public ImmutableList<Container> getClusterSpec() {
    GetClusterSpecRequest request = GetClusterSpecRequest.newBuilder().build();
    GetClusterSpecReply reply;
    try {
      reply = blockingStub.getClusterSpec(request);
    } catch (StatusRuntimeException e) {
      LOG.warn("RPC failed: " + e.getStatus());
      return null;
    }
    return ImmutableList.copyOf(reply.getClusterSpecList());
  }
}

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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.hops.tensorflow.clusterspecgen.ClusterSpecGenGrpc;
import io.hops.tensorflow.clusterspecgen.Container;
import io.hops.tensorflow.clusterspecgen.GetClusterSpecReply;
import io.hops.tensorflow.clusterspecgen.GetClusterSpecRequest;
import io.hops.tensorflow.clusterspecgen.RegisterContainerReply;
import io.hops.tensorflow.clusterspecgen.RegisterContainerRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterSpecGenServer {
  
  private static final Log LOG = LogFactory.getLog(ClusterSpecGenServer.class);
  
  private int numContainers;
  private Server server;
  
  public ClusterSpecGenServer(int numContainers) {
    this.numContainers = numContainers;
  }
  
  public void start(int port) throws IOException {
    if (server != null) {
      throw new IllegalStateException("Already started");
    }
    server = ServerBuilder.forPort(port)
        .addService(new ClusterSpecGenImpl(numContainers))
        .build()
        .start();
    LOG.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        ClusterSpecGenServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }
  
  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }
  
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    ClusterSpecGenServer server = new ClusterSpecGenServer(3);
    server.start(50051);
    server.blockUntilShutdown();
  }
  
  private static class ClusterSpecGenImpl extends ClusterSpecGenGrpc.ClusterSpecGenImplBase {
    
    int numContainers;
    Map<String, Container> clusterSpec;
    
    ClusterSpecGenImpl(int numContainers) {
      this.numContainers = numContainers;
      clusterSpec = new ConcurrentHashMap<>();
    }
    
    @Override
    public void registerContainer(RegisterContainerRequest request,
        StreamObserver<RegisterContainerReply> responseObserver) {
      Container container = request.getContainer();
      LOG.debug("Received registerContainerRequest with containerId: " + container.getContainerId());
      clusterSpec.put(container.getContainerId(), container);
      if (clusterSpec.size() > numContainers) {
        throw new IllegalStateException("clusterSpec size: " + clusterSpec.size());
      }
      RegisterContainerReply reply = RegisterContainerReply.newBuilder().build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
    
    @Override
    public void getClusterSpec(GetClusterSpecRequest request, StreamObserver<GetClusterSpecReply> responseObserver) {
      LOG.debug("Received getClusterSpecRequest");
      GetClusterSpecReply reply;
      if (clusterSpec.size() == numContainers) {
        reply = GetClusterSpecReply.newBuilder().addAllClusterSpec(clusterSpec.values()).build();
      } else {
        reply = GetClusterSpecReply.newBuilder().build();
      }
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}

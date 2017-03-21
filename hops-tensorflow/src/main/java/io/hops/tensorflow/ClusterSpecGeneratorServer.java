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
import io.hops.tensorflow.clusterspecgenerator.ClusterSpecGeneratorGrpc;
import io.hops.tensorflow.clusterspecgenerator.Container;
import io.hops.tensorflow.clusterspecgenerator.GetClusterSpecReply;
import io.hops.tensorflow.clusterspecgenerator.GetClusterSpecRequest;
import io.hops.tensorflow.clusterspecgenerator.RegisterContainerReply;
import io.hops.tensorflow.clusterspecgenerator.RegisterContainerRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterSpecGeneratorServer {
  
  private static final Log LOG = LogFactory.getLog(ClusterSpecGeneratorServer.class);
  
  private int numContainers;
  private Server server;
  
  public ClusterSpecGeneratorServer(int numContainers) {
    this.numContainers = numContainers;
  }
  
  public void start(int port) throws IOException {
    if (server != null) {
      throw new IllegalStateException("Already started");
    }
    server = ServerBuilder.forPort(port)
        .addService(new ClusterSpecGeneratorImpl(numContainers))
        .build()
        .start();
    LOG.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        ClusterSpecGeneratorServer.this.stop();
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
    ClusterSpecGeneratorServer server = new ClusterSpecGeneratorServer(3);
    server.start(50051);
    server.blockUntilShutdown();
  }
  
  private static class ClusterSpecGeneratorImpl extends ClusterSpecGeneratorGrpc.ClusterSpecGeneratorImplBase {
    
    int numContainers;
    Map<String, Container> clusterSpec;
    
    ClusterSpecGeneratorImpl(int numContainers) {
      this.numContainers = numContainers;
      clusterSpec = new ConcurrentHashMap<>();
    }
    
    @Override
    public void registerContainer(RegisterContainerRequest request,
        StreamObserver<RegisterContainerReply> responseObserver) {
      // TODO: check applicationID ?
      Container container = request.getContainer();
      LOG.debug("Received registerContainerRequest from: " + container.getJobName() + container.getTaskIndex());
      clusterSpec.put(container.getJobName() + container.getTaskIndex(), container);
      if (clusterSpec.size() > numContainers) {
        throw new IllegalStateException("clusterSpec size: " + clusterSpec.size());
      }
      RegisterContainerReply reply = RegisterContainerReply.newBuilder().build();
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
    
    @Override
    public void getClusterSpec(GetClusterSpecRequest request, StreamObserver<GetClusterSpecReply> responseObserver) {
      // TODO: check applicationID ?
      LOG.debug("Received getClusterSpecRequest");
      GetClusterSpecReply reply;
      if (clusterSpec.size() == numContainers) {
        List<Container> clusterSpecList = new ArrayList<>(clusterSpec.values());
        Collections.sort(clusterSpecList, new Comparator<Container>() {
          @Override
          public int compare(Container c1, Container c2) {
            return (c1.getTaskIndex() < c2.getTaskIndex() ? -1 : (c1.getTaskIndex() == c2.getTaskIndex() ? 0 : 1));
          }
        });
        reply = GetClusterSpecReply.newBuilder().addAllClusterSpec(clusterSpecList).build();
      } else {
        reply = GetClusterSpecReply.newBuilder().build();
      }
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}

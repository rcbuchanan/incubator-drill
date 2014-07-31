/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server;

import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.ResponseSender;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionManager;
import org.apache.drill.exec.rpc.data.DataResponseHandler;
import org.apache.drill.exec.rpc.data.DataRpcConfig;
import org.apache.drill.exec.rpc.data.DataServer;
import org.apache.drill.exec.rpc.data.DataTunnel;
import org.apache.drill.exec.server.rest.ProfileWrapper;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.apache.drill.exec.work.fragment.FragmentManager;
import org.glassfish.jersey.server.mvc.Viewable;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;


public class TestBitRpc extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBitRpc.class);

  
  
  
  
  @Test
  public void testProtobufQuery(@Injectable WorkerBee bee) throws Exception {
    System.out.println("...");
  }
}

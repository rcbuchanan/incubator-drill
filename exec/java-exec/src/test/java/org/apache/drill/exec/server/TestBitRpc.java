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
    PStore<QueryProfile> profiles = bee.getContext().getPersistentStoreProvider().getPStore(QueryStatus.QUERY_PROFILE);
    QueryProfile profile = profiles.get("");
    if(profile == null) profile = QueryProfile.getDefaultInstance();
  
    ProfileWrapper wrapper = new ProfileWrapper(profile);
    
  }

  private static WritableBatch getRandomBatch(BufferAllocator allocator, int records) {
    List<ValueVector> vectors = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      Float8Vector v = (Float8Vector) TypeHelper.getNewVector(
          MaterializedField.create(new SchemaPath("a", ExpressionPosition.UNKNOWN), Types.required(MinorType.FLOAT8)),
          allocator);
      v.allocateNew(records);
      v.getMutator().generateTestData(records);
      vectors.add(v);
    }
    return WritableBatch.getBatchNoHV(records, vectors, false);
  }

  private class TimingOutcome implements RpcOutcomeListener<Ack> {
    private AtomicLong max;
    private Stopwatch watch = new Stopwatch().start();

    public TimingOutcome(AtomicLong max) {
      super();
      this.max = max;
    }

    @Override
    public void failed(RpcException ex) {
      ex.printStackTrace();
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      long micros = watch.elapsed(TimeUnit.MILLISECONDS);
      System.out.println(String.format("Total time to send: %d, start time %d", micros, System.currentTimeMillis() - micros));
      while (true) {
        long nowMax = max.get();
        if (nowMax < micros) {
          if (max.compareAndSet(nowMax, micros))
            break;
        } else {
          break;
        }
      }
    }

  }

  private class BitComTestHandler implements DataResponseHandler {

    int v = 0;

    @Override
    public void handle(RemoteConnection connection, FragmentManager manager, FragmentRecordBatch fragmentBatch, ByteBuf data, ResponseSender sender)
        throws RpcException {
      // System.out.println("Received.");
      try {
        v++;
        if (v % 10 == 0) {
          System.out.println("sleeping.");
          Thread.sleep(3000);
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      sender.send(DataRpcConfig.OK);
    }

    @Override
    public void informOutOfMemory() {
    }

  }
}

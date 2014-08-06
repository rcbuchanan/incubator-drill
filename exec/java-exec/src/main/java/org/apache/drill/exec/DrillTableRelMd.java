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

package org.apache.drill.exec;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.vector.ValueVector;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rex.RexNode;

public class DrillTableRelMd {
  public static final RelMetadataProvider SOURCE = 
  ReflectiveRelMetadataProvider.reflectiveSource(
      BuiltinMethod.DISTINCT_ROW_COUNT.method, new DrillTableRelMd());
  
  public static RelMetadataProvider getMetadataProvider(DrillScanRel rel) {
    // check for metadata
    // scan metadata table
    // wrap metadata
    return null;
  }
  
//  public Double getDistinctRowCount(
//      RelNode rel,
//      BitSet groupKey,
//      RexNode predicate) {
//    return 999999.99;
//  }
  
  public Double getDistinctRowCount(
      DrillScanRel rel,
      BitSet groupKey,
      RexNode predicate) {
    return 99999999.0;
  }
  
  private static class MetadataProvider {
  }
  
  
  private static class MetadataLoader implements UserResultsListener {
    MetadataLoader(RecordBatchLoader loader) {
    }

    @Override
    public void submissionFailed(RpcException ex) {
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
    }

    @Override
    public void queryIdArrived(UserBitShared.QueryId queryId) {
    }

    public List<Object> loadResults() throws Exception {
      return null;
    }
  }
}

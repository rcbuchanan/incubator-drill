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
package org.apache.drill.exec.physical.impl.pivotstats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.codahale.metrics.MetricRegistry;

public class TestSimplePivot extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSimplePivot.class);
  DrillConfig c = DrillConfig.create();


  @Test
  public void pivot(@Injectable final DrillbitContext bitContext, @Injectable UserClientConnection connection) throws Throwable{
    List<QueryResultBatch> results;
    //results = testPhysicalWithResults(Files.toString(FileUtils.getResourceAsFile("/statspivot/test1.json"), Charsets.UTF_8));
    //results = testSqlWithResults("select rfields(`employee_id`) from cp.`employee.json` limit 5");
    //results = testSqlWithResults("select hll_decode(hll(`employee_id`)) from cp.`employee.json`");
    results = testSqlWithResults("analyze table dfs.tmp.`test.json` compute statistics for all columns");
    //results = testSqlWithResults("select * from dfs.tmp.`test1.json` t1 inner join dfs.tmp.`test2.json` t2 on t1.color = t2.color");
    //results = testSqlWithResults("select * from dfs.tmp.`test1.json` t1 inner join dfs.tmp.`test2.json` t2 on t1.color = t2.color inner join dfs.tmp.`test3.json` t3 on t1.adj = t3.adj");
    //setColumnWidth(10000);
    printResult(results);
    //results = testSqlWithResults("select * from dfs.tmp.`test2.json` t2 inner join dfs.tmp.`test1.json` t1 on t2.color = t1.color");
    //printResult(results);
    //results = testSqlWithResults("select tfunc(tab.complex) from dfs.`/Users/rbuchanan/test2.json` as tab");
    System.out.println("Batch count: " + results.size());
    //RecordBatchLoader loader = new RecordBatchLoader(getAllocator());

  }
}
